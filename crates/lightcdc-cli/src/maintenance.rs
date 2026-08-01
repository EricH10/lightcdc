//! Provides offline integrity, backup, and restore operations for the segmented store.

use std::{
    collections::HashSet,
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use lightcdc_core::Config;
use lightcdc_storage::{
    IntegrityReport, LogOpenOptions, RedbEventStore, SegmentOptions, SourceIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::store::{segment_options, storage_options};

const BACKUP_FORMAT_VERSION: u64 = 1;
const MANIFEST_FILE: &str = "manifest.json";

#[derive(Debug, Serialize, Deserialize)]
struct BackupManifest {
    format_version: u64,
    lightcdc_version: String,
    created_at_unix_ms: u64,
    source_name: String,
    source_offset: Option<String>,
    source_identity: Option<SourceIdentity>,
    integrity: IntegrityReport,
    files: Vec<BackupFile>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BackupFile {
    path: PathBuf,
    bytes: u64,
    sha256: String,
}

/// Traverses the configured offline store and prints a compact success report.
pub(crate) fn check(config_path: PathBuf) -> anyhow::Result<()> {
    let config = load_config(&config_path)?;
    let options = storage_options(&config);
    require_existing_store(&options)?;
    let store = open_store(&options, segment_options(&config))?;
    let report = store
        .verify_integrity()
        .context("deep store integrity check failed")?;
    println!(
        "Integrity OK: {} events, {} replay markers, {} segments, high watermark {}",
        report.event_count,
        report.replay_id_count,
        report.segment_count,
        report
            .high_watermark
            .map_or_else(|| "empty".to_owned(), |sequence| sequence.to_string())
    );
    Ok(())
}

/// Creates an atomic offline backup with per-file SHA-256 checksums.
pub(crate) fn backup(config_path: PathBuf, output: PathBuf) -> anyhow::Result<()> {
    let config = load_config(&config_path)?;
    let options = storage_options(&config);
    create_backup(
        &options,
        segment_options(&config),
        &config.source.name,
        &output,
    )?;
    println!("Backup created at {}", output.display());
    Ok(())
}

/// Restores only after checksums and a deep temporary-store check succeed.
pub(crate) fn restore(config_path: PathBuf, input: PathBuf) -> anyhow::Result<()> {
    let config = load_config(&config_path)?;
    let options = storage_options(&config);
    restore_backup(
        &options,
        segment_options(&config),
        &config.source.name,
        &input,
    )?;
    println!("Backup restored into {}", options.data_dir.display());
    Ok(())
}

fn load_config(path: &Path) -> anyhow::Result<Config> {
    Config::from_path(path)
        .with_context(|| format!("could not load config from {}", path.display()))
}

fn create_backup(
    options: &LogOpenOptions,
    segment_options: SegmentOptions,
    source_name: &str,
    output: &Path,
) -> anyhow::Result<()> {
    require_existing_store(options)?;
    if output.exists() {
        anyhow::bail!("backup output already exists: {}", output.display());
    }
    if output.starts_with(&options.data_dir) {
        anyhow::bail!("backup output must be outside the LightCDC data directory");
    }
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create backup parent {}", parent.display()))?;
    let mut partial = PartialDirectory::create(parent, "lightcdc-backup")?;

    let store = open_store(options, segment_options)?;
    let integrity = store
        .verify_integrity()
        .context("source store integrity check failed")?;
    let source_offset = store.source_offset(source_name)?;
    let source_identity = store.source_identity(source_name)?;
    // redb may mark an open writable file as needing recovery. Close every
    // handle before copying so the offline backup contains clean database files.
    drop(store);
    let source_files = durable_store_files(options)?;
    let mut files = Vec::with_capacity(source_files.len());
    for source in source_files {
        let relative = source
            .strip_prefix(&options.data_dir)
            .with_context(|| format!("store file {} escaped data directory", source.display()))?;
        validate_relative_path(relative)?;
        let destination = partial.path().join(relative);
        files.push(copy_and_hash(
            &source,
            &destination,
            relative.to_path_buf(),
        )?);
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let manifest = BackupManifest {
        format_version: BACKUP_FORMAT_VERSION,
        lightcdc_version: env!("CARGO_PKG_VERSION").to_owned(),
        created_at_unix_ms: unix_timestamp_ms(),
        source_name: source_name.to_owned(),
        source_offset,
        source_identity,
        integrity,
        files,
    };
    write_json_sync(&partial.path().join(MANIFEST_FILE), &manifest)?;
    sync_directory(partial.path())?;
    partial.publish(output)?;
    sync_directory(parent)?;
    Ok(())
}

fn restore_backup(
    options: &LogOpenOptions,
    segment_options: SegmentOptions,
    source_name: &str,
    input: &Path,
) -> anyhow::Result<()> {
    let manifest_path = input.join(MANIFEST_FILE);
    let manifest: BackupManifest = serde_json::from_reader(
        File::open(&manifest_path)
            .with_context(|| format!("open backup manifest {}", manifest_path.display()))?,
    )
    .with_context(|| format!("parse backup manifest {}", manifest_path.display()))?;
    validate_manifest(&manifest, source_name)?;
    ensure_restore_target_empty(&options.data_dir)?;

    // Verify every source checksum before creating a partial restore.
    for file in &manifest.files {
        validate_relative_path(&file.path)?;
        verify_file(&input.join(&file.path), file)?;
    }

    let parent = options.data_dir.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create restore parent {}", parent.display()))?;
    let mut partial = PartialDirectory::create(parent, "lightcdc-restore")?;
    for file in &manifest.files {
        let source = input.join(&file.path);
        let destination = partial.path().join(&file.path);
        let copied = copy_and_hash(&source, &destination, file.path.clone())?;
        if copied.bytes != file.bytes || copied.sha256 != file.sha256 {
            anyhow::bail!(
                "backup file changed while restoring: {}",
                file.path.display()
            );
        }
    }
    sync_tree(partial.path())?;

    let restored_options = LogOpenOptions {
        data_dir: partial.path().to_path_buf(),
        database_file: options.database_file.clone(),
    };
    let restored = open_store(&restored_options, segment_options)
        .context("restored store could not be opened")?;
    let report = restored
        .verify_integrity()
        .context("restored store failed deep integrity verification")?;
    // Opening a redb file can reclaim allocator pages and change its physical
    // length. Exact backup bytes were already checksum-verified above; compare
    // the logical integrity fields after opening the restored store.
    if report.segment_count != manifest.integrity.segment_count
        || report.event_count != manifest.integrity.event_count
        || report.replay_id_count != manifest.integrity.replay_id_count
        || report.first_sequence != manifest.integrity.first_sequence
        || report.high_watermark != manifest.integrity.high_watermark
    {
        anyhow::bail!(
            "restored integrity report does not match the backup manifest: expected {:?}, got {:?}",
            manifest.integrity,
            report
        );
    }
    if restored.source_offset(source_name)? != manifest.source_offset
        || restored.source_identity(source_name)? != manifest.source_identity
    {
        anyhow::bail!("restored source metadata does not match the backup manifest");
    }
    drop(restored);

    if options.data_dir.exists() {
        fs::remove_dir(&options.data_dir).with_context(|| {
            format!("remove empty restore target {}", options.data_dir.display())
        })?;
    }
    partial.publish(&options.data_dir)?;
    sync_directory(parent)?;
    Ok(())
}

fn require_existing_store(options: &LogOpenOptions) -> anyhow::Result<()> {
    let control = options.data_dir.join(&options.database_file);
    if !control.is_file() {
        anyhow::bail!(
            "LightCDC control database does not exist: {}",
            control.display()
        );
    }
    Ok(())
}

fn open_store(
    options: &LogOpenOptions,
    segment_options: SegmentOptions,
) -> anyhow::Result<RedbEventStore> {
    RedbEventStore::open_with_segment_options(options, segment_options).with_context(|| {
        format!(
            "open offline store {}; stop capture, run, serve, and inspect first",
            options.data_dir.join(&options.database_file).display()
        )
    })
}

fn durable_store_files(options: &LogOpenOptions) -> anyhow::Result<Vec<PathBuf>> {
    let control = options.data_dir.join(&options.database_file);
    let marker = control.with_file_name(format!("{}.format", options.database_file));
    let segments = control.with_file_name(format!("{}.segments", options.database_file));
    let mut files = vec![control, marker];
    for entry in fs::read_dir(&segments)
        .with_context(|| format!("read segment directory {}", segments.display()))?
    {
        let path = entry?.path();
        let durable = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".redb") || name.ends_with(".redb.format"));
        if durable {
            files.push(path);
        }
    }
    for path in &files {
        if !fs::symlink_metadata(path)
            .with_context(|| format!("read store file metadata {}", path.display()))?
            .file_type()
            .is_file()
        {
            anyhow::bail!(
                "durable store path is not a regular file: {}",
                path.display()
            );
        }
    }
    Ok(files)
}

fn validate_manifest(manifest: &BackupManifest, source_name: &str) -> anyhow::Result<()> {
    if manifest.format_version != BACKUP_FORMAT_VERSION {
        anyhow::bail!(
            "backup format {} is unsupported; this binary supports {}",
            manifest.format_version,
            BACKUP_FORMAT_VERSION
        );
    }
    if manifest.source_name != source_name {
        anyhow::bail!(
            "backup source {:?} does not match configured source {:?}",
            manifest.source_name,
            source_name
        );
    }
    if manifest.files.is_empty() {
        anyhow::bail!("backup manifest contains no durable files");
    }
    let mut paths = HashSet::new();
    for file in &manifest.files {
        validate_relative_path(&file.path)?;
        if !paths.insert(&file.path) {
            anyhow::bail!("backup manifest repeats file {}", file.path.display());
        }
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> anyhow::Result<()> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        anyhow::bail!("backup manifest contains unsafe path {}", path.display());
    }
    Ok(())
}

fn ensure_restore_target_empty(path: &Path) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if !path.is_dir() {
        anyhow::bail!("restore target is not a directory: {}", path.display());
    }
    if fs::read_dir(path)?.next().is_some() {
        anyhow::bail!("restore target must be empty: {}", path.display());
    }
    Ok(())
}

fn copy_and_hash(
    source: &Path,
    destination: &Path,
    relative: PathBuf,
) -> anyhow::Result<BackupFile> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut input =
        File::open(source).with_context(|| format!("open backup source {}", source.display()))?;
    let mut output = File::create(destination)
        .with_context(|| format!("create backup file {}", destination.display()))?;
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
        digest.update(&buffer[..read]);
        bytes = bytes.saturating_add(read as u64);
    }
    output.sync_all()?;
    Ok(BackupFile {
        path: relative,
        bytes,
        sha256: format!("{:x}", digest.finalize()),
    })
}

fn verify_file(path: &Path, expected: &BackupFile) -> anyhow::Result<()> {
    let mut input =
        File::open(path).with_context(|| format!("open backup file {}", path.display()))?;
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        bytes = bytes.saturating_add(read as u64);
    }
    let actual = format!("{:x}", digest.finalize());
    if bytes != expected.bytes || actual != expected.sha256 {
        anyhow::bail!("backup checksum mismatch for {}", expected.path.display());
    }
    Ok(())
}

fn write_json_sync(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn sync_tree(path: &Path) -> anyhow::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree(&entry.path())?;
        }
    }
    sync_directory(path)
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

struct PartialDirectory {
    path: PathBuf,
    published: bool,
}

impl PartialDirectory {
    fn create(parent: &Path, prefix: &str) -> anyhow::Result<Self> {
        for attempt in 0..100u32 {
            let path = parent.join(format!(
                ".{prefix}-{}-{}-{attempt}.partial",
                std::process::id(),
                unix_timestamp_ms()
            ));
            match fs::create_dir(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        published: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error).context("create partial directory"),
            }
        }
        Err(anyhow!("could not allocate a unique partial directory"))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn publish(&mut self, destination: &Path) -> anyhow::Result<()> {
        fs::rename(&self.path, destination).with_context(|| {
            format!(
                "atomically publish {} as {}",
                self.path.display(),
                destination.display()
            )
        })?;
        self.published = true;
        Ok(())
    }
}

impl Drop for PartialDirectory {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use lightcdc_core::{ChangeEvent, Operation, SourceMetadata};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn backup_restore_round_trip_rejects_tampering_before_destination_changes() {
        let temp = TempDir::new().expect("temp dir");
        let source_dir = temp.path().join("source");
        let restored_dir = temp.path().join("restored");
        let backup_dir = temp.path().join("backup");
        let source_options = LogOpenOptions {
            data_dir: source_dir,
            database_file: "lightcdc.redb".to_owned(),
        };
        let store = RedbEventStore::open(&source_options).expect("source store");
        store.append_event(&event(1)).expect("append event");
        drop(store);

        create_backup(
            &source_options,
            SegmentOptions::default(),
            "default",
            &backup_dir,
        )
        .expect("create backup");
        let restored_options = LogOpenOptions {
            data_dir: restored_dir.clone(),
            database_file: "lightcdc.redb".to_owned(),
        };
        restore_backup(
            &restored_options,
            SegmentOptions::default(),
            "default",
            &backup_dir,
        )
        .expect("restore backup");
        let restored = RedbEventStore::open(&restored_options).expect("restored store");
        assert_eq!(restored.replay_from(1, 10).expect("replay"), [event(1)]);
        drop(restored);

        let tampered_target = temp.path().join("tampered-target");
        let manifest: BackupManifest =
            serde_json::from_reader(File::open(backup_dir.join(MANIFEST_FILE)).expect("manifest"))
                .expect("parse manifest");
        let segment = manifest
            .files
            .iter()
            .find(|file| {
                file.path
                    .extension()
                    .is_some_and(|extension| extension == "redb")
            })
            .expect("redb file");
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(backup_dir.join(&segment.path))
            .expect("tamper backup");
        file.write_all(b"tampered").expect("append tampering");
        let tampered_options = LogOpenOptions {
            data_dir: tampered_target.clone(),
            database_file: "lightcdc.redb".to_owned(),
        };

        assert!(
            restore_backup(
                &tampered_options,
                SegmentOptions::default(),
                "default",
                &backup_dir,
            )
            .is_err()
        );
        assert!(!tampered_target.exists());
    }

    fn event(sequence: u64) -> ChangeEvent {
        ChangeEvent {
            sequence,
            event_id: format!("event-{sequence}"),
            source: SourceMetadata {
                database: "lightcdc".to_owned(),
                slot: "slot".to_owned(),
                lsn: format!("0/{sequence:X}"),
            },
            transaction: None,
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            operation: Operation::Insert,
            key: Some(br#"{"id":"1"}"#.to_vec()),
            before: None,
            after: Some(br#"{"id":"1"}"#.to_vec()),
            commit_timestamp_ms: Some(1_784_862_080_000),
        }
    }
}
