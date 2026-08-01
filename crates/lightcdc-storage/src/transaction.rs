//! Buffers one source transaction in memory or a crash-cleaned staging file.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use lightcdc_core::ChangeEvent;
use thiserror::Error;

const STAGING_FILE_EXTENSION: &str = "lightcdc-stage";
const STAGING_MAGIC: &[u8; 8] = b"LCDCSTG\0";
const STAGING_FORMAT_VERSION: u32 = 1;
const STAGING_HEADER_BYTES: u64 = (STAGING_MAGIC.len() + size_of::<u32>()) as u64;
const RECORD_LENGTH_BYTES: u64 = size_of::<u64>() as u64;
static STAGING_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Configures transaction memory, disk staging, and hard safety limits.
#[derive(Debug, Clone)]
pub struct TransactionBufferOptions {
    /// Source-scoped directory for transactions that exceed memory threshold.
    pub staging_dir: Option<PathBuf>,
    /// Estimated decoded bytes retained before spilling the transaction.
    pub memory_threshold_bytes: u64,
    /// Hard limit applied to decoded and staged transaction size.
    pub max_transaction_bytes: u64,
    /// Hard event-count limit for one source transaction.
    pub max_transaction_events: usize,
    /// Filesystem bytes that staging writes must leave unused.
    pub min_free_disk_bytes: u64,
}

impl TransactionBufferOptions {
    /// Creates bounded options with a source-scoped directory below `staging_root`.
    pub fn bounded(
        staging_root: impl AsRef<Path>,
        source_name: &str,
        memory_threshold_bytes: u64,
        max_transaction_bytes: u64,
        max_transaction_events: usize,
    ) -> Self {
        Self {
            staging_dir: Some(
                staging_root
                    .as_ref()
                    .join(source_staging_directory(source_name)),
            ),
            memory_threshold_bytes,
            max_transaction_bytes,
            max_transaction_events,
            min_free_disk_bytes: 0,
        }
    }

    /// Preserves a filesystem reserve while spilling large transactions.
    pub fn with_min_free_disk_bytes(mut self, min_free_disk_bytes: u64) -> Self {
        self.min_free_disk_bytes = min_free_disk_bytes;
        self
    }

    /// Keeps all events in memory for direct connector users and focused tests.
    pub fn unbounded_in_memory() -> Self {
        Self {
            staging_dir: None,
            memory_threshold_bytes: u64::MAX,
            max_transaction_bytes: u64::MAX,
            max_transaction_events: usize::MAX,
            min_free_disk_bytes: 0,
        }
    }

    /// Rejects option combinations that cannot enforce their stated bounds.
    fn validate(&self) -> Result<(), TransactionBufferError> {
        if self.max_transaction_events == 0 {
            return Err(TransactionBufferError::InvalidOptions(
                "max_transaction_events must be greater than zero".to_owned(),
            ));
        }
        if self.max_transaction_bytes == 0 {
            return Err(TransactionBufferError::InvalidOptions(
                "max_transaction_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.memory_threshold_bytes > self.max_transaction_bytes {
            return Err(TransactionBufferError::InvalidOptions(format!(
                "memory threshold {} exceeds maximum transaction size {}",
                self.memory_threshold_bytes, self.max_transaction_bytes
            )));
        }
        if self.memory_threshold_bytes < u64::MAX && self.staging_dir.is_none() {
            return Err(TransactionBufferError::InvalidOptions(
                "a staging directory is required when memory spilling is enabled".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Reports the resources consumed by one decoded source transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionStats {
    /// Number of captured events in the source transaction.
    pub event_count: usize,
    /// Estimated bytes owned by decoded events.
    pub decoded_bytes: u64,
    /// Length-prefixed bytes the same events require when staged.
    pub staged_bytes: u64,
}

/// Buffers one transaction in memory and spills it to a file when configured.
pub struct TransactionBuffer {
    options: TransactionBufferOptions,
    transaction_id: Option<u64>,
    events: Vec<ChangeEvent>,
    staging: Option<StagingWriter>,
    stats: TransactionStats,
    cleaned_orphan_count: usize,
    orphan_cleanup_completed: bool,
}

impl TransactionBuffer {
    /// Creates an empty buffer and removes staging files left by an unclean exit.
    pub fn new(options: TransactionBufferOptions) -> Result<Self, TransactionBufferError> {
        let mut buffer = Self::new_with_deferred_orphan_cleanup(options)?;
        buffer.cleanup_orphaned_files()?;
        Ok(buffer)
    }

    /// Creates an empty buffer whose orphan cleanup is deferred until source ownership is proven.
    pub fn new_with_deferred_orphan_cleanup(
        options: TransactionBufferOptions,
    ) -> Result<Self, TransactionBufferError> {
        options.validate()?;

        Ok(Self {
            options,
            transaction_id: None,
            events: Vec::new(),
            staging: None,
            stats: TransactionStats::default(),
            cleaned_orphan_count: 0,
            orphan_cleanup_completed: false,
        })
    }

    /// Removes source-scoped staging leftovers once, returning the number removed.
    pub fn cleanup_orphaned_files(&mut self) -> Result<usize, TransactionBufferError> {
        if self.orphan_cleanup_completed {
            return Ok(0);
        }

        let removed = cleanup_source_staging_files(&self.options)?;
        self.cleaned_orphan_count = removed;
        self.orphan_cleanup_completed = true;
        Ok(removed)
    }

    /// Returns how many source-scoped orphan files startup removed.
    pub fn cleaned_orphan_count(&self) -> usize {
        self.cleaned_orphan_count
    }

    /// Starts accounting and buffering a new source transaction.
    pub fn begin(&mut self, transaction_id: u64) -> Result<(), TransactionBufferError> {
        if self.transaction_id.is_some() || !self.is_empty() {
            return Err(TransactionBufferError::TransactionAlreadyActive);
        }
        self.transaction_id = Some(transaction_id);
        Ok(())
    }

    /// Adds one event, spilling all buffered events when the threshold is crossed.
    pub fn push(&mut self, event: ChangeEvent) -> Result<(), TransactionBufferError> {
        let transaction_id = self
            .transaction_id
            .ok_or(TransactionBufferError::NoActiveTransaction)?;
        let decoded_bytes = estimated_decoded_bytes(&event);
        let encoded = serde_json::to_vec(&event)?;
        let record_bytes = RECORD_LENGTH_BYTES
            .checked_add(encoded.len() as u64)
            .ok_or(TransactionBufferError::AccountingOverflow)?;
        let staged_bytes = self
            .stats
            .staged_bytes
            .checked_add(if self.stats.event_count == 0 {
                STAGING_HEADER_BYTES
            } else {
                0
            })
            .and_then(|bytes| bytes.checked_add(record_bytes))
            .ok_or(TransactionBufferError::AccountingOverflow)?;
        let next_stats = TransactionStats {
            event_count: self
                .stats
                .event_count
                .checked_add(1)
                .ok_or(TransactionBufferError::AccountingOverflow)?,
            decoded_bytes: self
                .stats
                .decoded_bytes
                .checked_add(decoded_bytes)
                .ok_or(TransactionBufferError::AccountingOverflow)?,
            staged_bytes,
        };
        self.check_limits(next_stats)?;

        if self.staging.is_none() && next_stats.decoded_bytes > self.options.memory_threshold_bytes
        {
            self.spill_memory_events(transaction_id)?;
        }

        if let Some(staging) = &mut self.staging {
            staging.write_record(&encoded)?;
        } else {
            self.events.push(event);
        }
        self.stats = next_stats;
        Ok(())
    }

    /// Finishes the active transaction and resets the buffer for the next one.
    pub fn finish(&mut self) -> Result<TransactionEvents, TransactionBufferError> {
        if self.transaction_id.take().is_none() {
            return Err(TransactionBufferError::NoActiveTransaction);
        }

        let stats = std::mem::take(&mut self.stats);
        if let Some(staging) = self.staging.take() {
            debug_assert!(self.events.is_empty());
            Ok(TransactionEvents::Staged(staging.finish(stats)?))
        } else {
            Ok(TransactionEvents::InMemory {
                events: std::mem::take(&mut self.events),
                stats,
            })
        }
    }

    /// Returns true when no event is currently buffered.
    pub fn is_empty(&self) -> bool {
        self.stats.event_count == 0
    }

    /// Enforces hard event and byte limits before accepting another event.
    fn check_limits(&self, stats: TransactionStats) -> Result<(), TransactionBufferError> {
        if stats.event_count > self.options.max_transaction_events {
            return Err(TransactionBufferError::EventLimitExceeded {
                attempted: stats.event_count,
                maximum: self.options.max_transaction_events,
            });
        }
        if stats.decoded_bytes > self.options.max_transaction_bytes
            || stats.staged_bytes > self.options.max_transaction_bytes
        {
            return Err(TransactionBufferError::ByteLimitExceeded {
                attempted_decoded: stats.decoded_bytes,
                attempted_staged: stats.staged_bytes,
                maximum: self.options.max_transaction_bytes,
            });
        }
        Ok(())
    }

    /// Moves all in-memory events into a staging file without splitting ownership.
    fn spill_memory_events(&mut self, transaction_id: u64) -> Result<(), TransactionBufferError> {
        let staging_dir = self.options.staging_dir.as_ref().ok_or_else(|| {
            TransactionBufferError::InvalidOptions(
                "transaction exceeded memory threshold without a staging directory".to_owned(),
            )
        })?;
        let mut staging = StagingWriter::create(
            staging_dir,
            transaction_id,
            self.options.min_free_disk_bytes,
        )?;

        for event in self.events.drain(..) {
            staging.write_record(&serde_json::to_vec(&event)?)?;
        }
        self.staging = Some(staging);
        Ok(())
    }
}

/// Owns the committed events from one source transaction.
pub enum TransactionEvents {
    /// Events that remained below the configured memory threshold.
    InMemory {
        events: Vec<ChangeEvent>,
        stats: TransactionStats,
    },
    /// Events streamed from an owned staging file.
    Staged(StagedEvents),
}

impl TransactionEvents {
    /// Returns transaction resource accounting.
    pub fn stats(&self) -> TransactionStats {
        match self {
            Self::InMemory { stats, .. } => *stats,
            Self::Staged(staged) => staged.stats,
        }
    }

    /// Returns the number of events in the transaction.
    pub fn len(&self) -> usize {
        self.stats().event_count
    }

    /// Returns true when the source transaction contains no captured events.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true when the transaction crossed the in-memory threshold.
    pub fn is_staged(&self) -> bool {
        matches!(self, Self::Staged(_))
    }

    /// Opens an iterator that yields one owned event at a time.
    pub fn iter(&self) -> Result<TransactionEventIter<'_>, TransactionBufferError> {
        match self {
            Self::InMemory { events, .. } => Ok(TransactionEventIter::InMemory(events.iter())),
            Self::Staged(staged) => {
                let mut reader = BufReader::new(File::open(&staged.path)?);
                read_staging_header(&mut reader)?;
                Ok(TransactionEventIter::Staged(StagedEventReader {
                    reader,
                    remaining: staged.stats.event_count,
                }))
            }
        }
    }

    /// Loads all events for tests or other explicitly bounded callers.
    pub fn load(&self) -> Result<Vec<ChangeEvent>, TransactionBufferError> {
        self.iter()?.collect()
    }
}

/// Iterates over in-memory or file-backed transaction events.
pub enum TransactionEventIter<'a> {
    /// Clones events from the in-memory transaction slice.
    InMemory(std::slice::Iter<'a, ChangeEvent>),
    /// Deserializes events one at a time from disk.
    Staged(StagedEventReader),
}

impl Iterator for TransactionEventIter<'_> {
    type Item = Result<ChangeEvent, TransactionBufferError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::InMemory(events) => events.next().cloned().map(Ok),
            Self::Staged(events) => events.next(),
        }
    }
}

/// Owns a completed staging file and removes it when processing finishes.
pub struct StagedEvents {
    path: PathBuf,
    stats: TransactionStats,
}

impl StagedEvents {
    /// Returns the staging path for diagnostics and crash-boundary tests.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for StagedEvents {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Writes length-prefixed JSON records and removes incomplete files on drop.
struct StagingWriter {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    remove_on_drop: bool,
    min_free_disk_bytes: u64,
}

impl StagingWriter {
    /// Creates a collision-resistant file for one active source transaction.
    fn create(
        staging_dir: &Path,
        transaction_id: u64,
        min_free_disk_bytes: u64,
    ) -> Result<Self, TransactionBufferError> {
        fs::create_dir_all(staging_dir)?;
        let path = unique_staging_path(staging_dir, transaction_id);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;

        let mut staging = Self {
            path,
            writer: Some(BufWriter::new(file)),
            remove_on_drop: true,
            min_free_disk_bytes,
        };
        staging.write_header()?;
        Ok(staging)
    }

    fn write_header(&mut self) -> Result<(), TransactionBufferError> {
        self.ensure_available_space(STAGING_HEADER_BYTES)?;
        let writer = self
            .writer
            .as_mut()
            .ok_or(TransactionBufferError::ClosedStagingFile)?;
        writer.write_all(STAGING_MAGIC)?;
        writer.write_all(&STAGING_FORMAT_VERSION.to_be_bytes())?;
        Ok(())
    }

    /// Appends one length-prefixed serialized event.
    fn write_record(&mut self, payload: &[u8]) -> Result<(), TransactionBufferError> {
        let required_bytes = RECORD_LENGTH_BYTES.saturating_add(payload.len() as u64);
        self.ensure_available_space(required_bytes)?;
        let writer = self
            .writer
            .as_mut()
            .ok_or(TransactionBufferError::ClosedStagingFile)?;
        writer.write_all(&(payload.len() as u64).to_be_bytes())?;
        writer.write_all(payload)?;
        Ok(())
    }

    fn ensure_available_space(&self, required_bytes: u64) -> Result<(), TransactionBufferError> {
        let staging_dir = self.path.parent().ok_or_else(|| {
            TransactionBufferError::InvalidOptions(
                "staging file has no parent directory".to_owned(),
            )
        })?;
        let available_bytes = fs2::available_space(staging_dir)?;
        if available_bytes < self.min_free_disk_bytes.saturating_add(required_bytes) {
            return Err(TransactionBufferError::InsufficientStagingSpace {
                available: available_bytes,
                required: required_bytes,
                reserved: self.min_free_disk_bytes,
            });
        }
        Ok(())
    }

    /// Flushes the file and transfers cleanup ownership to `StagedEvents`.
    fn finish(mut self, stats: TransactionStats) -> Result<StagedEvents, TransactionBufferError> {
        let mut writer = self
            .writer
            .take()
            .ok_or(TransactionBufferError::ClosedStagingFile)?;
        writer.flush()?;
        drop(writer);
        self.remove_on_drop = false;

        Ok(StagedEvents {
            path: self.path.clone(),
            stats,
        })
    }
}

impl Drop for StagingWriter {
    fn drop(&mut self) {
        if self.remove_on_drop {
            let _ = self.writer.take();
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Streams length-prefixed events from one completed staging file.
pub struct StagedEventReader {
    reader: BufReader<File>,
    remaining: usize,
}

impl Iterator for StagedEventReader {
    type Item = Result<ChangeEvent, TransactionBufferError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let result = (|| {
            let mut length = [0_u8; size_of::<u64>()];
            self.reader.read_exact(&mut length)?;
            let length = usize::try_from(u64::from_be_bytes(length))
                .map_err(|_| TransactionBufferError::InvalidRecordLength)?;
            let mut payload = vec![0_u8; length];
            self.reader.read_exact(&mut payload)?;
            Ok(serde_json::from_slice(&payload)?)
        })();
        self.remaining -= 1;
        Some(result)
    }
}

fn read_staging_header(reader: &mut impl Read) -> Result<(), TransactionBufferError> {
    let mut magic = [0_u8; STAGING_MAGIC.len()];
    reader.read_exact(&mut magic)?;
    if &magic != STAGING_MAGIC {
        return Err(TransactionBufferError::InvalidStagingHeader);
    }
    let mut version = [0_u8; size_of::<u32>()];
    reader.read_exact(&mut version)?;
    let found = u32::from_be_bytes(version);
    if found != STAGING_FORMAT_VERSION {
        return Err(TransactionBufferError::UnsupportedStagingFormat {
            found,
            supported: STAGING_FORMAT_VERSION,
        });
    }
    Ok(())
}

/// Represents transaction accounting, staging, and staged-read failures.
#[derive(Debug, Error)]
pub enum TransactionBufferError {
    #[error("invalid transaction buffer options: {0}")]
    InvalidOptions(String),

    #[error("a source transaction is already active")]
    TransactionAlreadyActive,

    #[error("no source transaction is active")]
    NoActiveTransaction,

    #[error("transaction event count {attempted} exceeds configured maximum {maximum}")]
    EventLimitExceeded { attempted: usize, maximum: usize },

    #[error(
        "transaction size exceeds configured maximum {maximum} bytes \
         (decoded={attempted_decoded}, staged={attempted_staged})"
    )]
    ByteLimitExceeded {
        attempted_decoded: u64,
        attempted_staged: u64,
        maximum: u64,
    },

    #[error("transaction byte accounting overflowed")]
    AccountingOverflow,

    #[error(
        "staging filesystem has {available} bytes available; the next record needs {required} bytes while preserving {reserved} reserved bytes"
    )]
    InsufficientStagingSpace {
        available: u64,
        required: u64,
        reserved: u64,
    },

    #[error("staging file is already closed")]
    ClosedStagingFile,

    #[error("staging file contains an invalid record length")]
    InvalidRecordLength,

    #[error("staging file header is invalid")]
    InvalidStagingHeader,

    #[error("staging format {found} is unsupported; this binary supports format {supported}")]
    UnsupportedStagingFormat { found: u32, supported: u32 },

    #[error("staging I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("staged event serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

impl TransactionBufferError {
    /// Returns true when a configured hard transaction limit was exceeded.
    pub fn is_limit_exceeded(&self) -> bool {
        matches!(
            self,
            Self::EventLimitExceeded { .. }
                | Self::ByteLimitExceeded { .. }
                | Self::InsufficientStagingSpace { .. }
        )
    }
}

/// Removes only staging files inside the configured source-scoped directory.
fn cleanup_source_staging_files(
    options: &TransactionBufferOptions,
) -> Result<usize, TransactionBufferError> {
    let Some(staging_dir) = &options.staging_dir else {
        return Ok(0);
    };
    fs::create_dir_all(staging_dir)?;
    let mut removed = 0;

    for entry in fs::read_dir(staging_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && path.extension().and_then(|extension| extension.to_str())
                == Some(STAGING_FILE_EXTENSION)
        {
            fs::remove_file(path)?;
            removed += 1;
        }
    }

    Ok(removed)
}

/// Estimates owned heap and inline bytes used by a decoded event.
fn estimated_decoded_bytes(event: &ChangeEvent) -> u64 {
    let transaction_strings = event.transaction.as_ref().map_or(0, |transaction| {
        transaction
            .begin_lsn
            .as_ref()
            .map_or(0, String::len)
            .saturating_add(transaction.commit_lsn.as_ref().map_or(0, String::len))
    });
    let payload_bytes = event.key.as_ref().map_or(0, Vec::len)
        + event.before.as_ref().map_or(0, Vec::len)
        + event.after.as_ref().map_or(0, Vec::len);
    let string_bytes = event.event_id.len()
        + event.source.database.len()
        + event.source.slot.len()
        + event.source.lsn.len()
        + event.schema.len()
        + event.table.len()
        + transaction_strings;

    (size_of::<ChangeEvent>() + payload_bytes + string_bytes) as u64
}

/// Hex-encodes the source name so every source owns a filesystem-safe directory.
fn source_staging_directory(source_name: &str) -> String {
    let mut encoded = String::with_capacity(source_name.len() * 2 + 7);
    encoded.push_str("source-");
    for byte in source_name.bytes() {
        use std::fmt::Write;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Builds a process-, transaction-, time-, and counter-scoped staging filename.
fn unique_staging_path(staging_dir: &Path, transaction_id: u64) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = STAGING_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    staging_dir.join(format!(
        "txn-{}-{transaction_id}-{timestamp}-{counter}.{STAGING_FILE_EXTENSION}",
        std::process::id()
    ))
}

#[cfg(test)]
mod tests {
    use lightcdc_core::{Operation, SourceMetadata};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn spills_and_reads_events_without_retaining_them_in_memory() {
        let temp = TempDir::new().expect("temp dir");
        let options = TransactionBufferOptions::bounded(temp.path(), "source", 1, 1_000_000, 10);
        let mut buffer = TransactionBuffer::new(options).expect("buffer");
        buffer.begin(42).expect("begin");
        buffer.push(event(1)).expect("event 1");
        buffer.push(event(2)).expect("event 2");

        let events = buffer.finish().expect("finish");
        assert!(events.is_staged());
        assert_eq!(events.len(), 2);
        assert_eq!(
            events
                .iter()
                .expect("event iterator")
                .map(|event| event.expect("staged event").sequence)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[test]
    fn completed_stage_file_is_removed_when_events_are_dropped() {
        let temp = TempDir::new().expect("temp dir");
        let options = TransactionBufferOptions::bounded(temp.path(), "source", 1, 1_000_000, 10);
        let mut buffer = TransactionBuffer::new(options).expect("buffer");
        buffer.begin(42).expect("begin");
        buffer.push(event(1)).expect("event");
        let events = buffer.finish().expect("finish");
        let TransactionEvents::Staged(staged) = &events else {
            panic!("expected staged events");
        };
        let path = staged.path().to_path_buf();
        assert!(path.exists());

        drop(events);

        assert!(!path.exists());
    }

    #[test]
    fn newer_staging_format_is_rejected() {
        let temp = TempDir::new().expect("temp dir");
        let options = TransactionBufferOptions::bounded(temp.path(), "source", 1, 1_000_000, 10);
        let mut buffer = TransactionBuffer::new(options).expect("buffer");
        buffer.begin(42).expect("begin");
        buffer.push(event(1)).expect("event");
        let events = buffer.finish().expect("finish");
        let TransactionEvents::Staged(staged) = &events else {
            panic!("expected staged events");
        };
        let mut bytes = fs::read(staged.path()).expect("read staging file");
        bytes[STAGING_MAGIC.len()..STAGING_HEADER_BYTES as usize]
            .copy_from_slice(&(STAGING_FORMAT_VERSION + 1).to_be_bytes());
        fs::write(staged.path(), bytes).expect("write future staging format");

        let error = match events.iter() {
            Ok(_) => panic!("future staging format must be rejected"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            TransactionBufferError::UnsupportedStagingFormat {
                found,
                supported
            } if found == STAGING_FORMAT_VERSION + 1 && supported == STAGING_FORMAT_VERSION
        ));
    }

    #[test]
    fn startup_removes_only_source_scoped_orphan_files() {
        let temp = TempDir::new().expect("temp dir");
        let options = TransactionBufferOptions::bounded(temp.path(), "source", 1, 1_000_000, 10);
        let staging_dir = options.staging_dir.as_ref().expect("staging dir");
        fs::create_dir_all(staging_dir).expect("create staging dir");
        let orphan = staging_dir.join("orphan.lightcdc-stage");
        let unrelated = staging_dir.join("keep.txt");
        fs::write(&orphan, b"orphan").expect("write orphan");
        fs::write(&unrelated, b"keep").expect("write unrelated");

        let buffer = TransactionBuffer::new(options).expect("buffer");

        assert_eq!(buffer.cleaned_orphan_count(), 1);
        assert!(!orphan.exists());
        assert!(unrelated.exists());
    }

    #[test]
    fn deferred_cleanup_waits_for_explicit_source_ownership() {
        let temp = TempDir::new().expect("temp dir");
        let options = TransactionBufferOptions::bounded(temp.path(), "source", 1, 1_000_000, 10);
        let staging_dir = options.staging_dir.as_ref().expect("staging dir");
        fs::create_dir_all(staging_dir).expect("create staging dir");
        let orphan = staging_dir.join("orphan.lightcdc-stage");
        fs::write(&orphan, b"orphan").expect("write orphan");

        let mut buffer = TransactionBuffer::new_with_deferred_orphan_cleanup(options)
            .expect("deferred transaction buffer");

        assert!(orphan.exists());
        assert_eq!(buffer.cleanup_orphaned_files().expect("cleanup"), 1);
        assert!(!orphan.exists());
        assert_eq!(buffer.cleanup_orphaned_files().expect("second cleanup"), 0);
    }

    #[test]
    fn hard_event_limit_rejects_the_transaction() {
        let options = TransactionBufferOptions {
            staging_dir: None,
            memory_threshold_bytes: u64::MAX,
            max_transaction_bytes: u64::MAX,
            max_transaction_events: 1,
            min_free_disk_bytes: 0,
        };
        let mut buffer = TransactionBuffer::new(options).expect("buffer");
        buffer.begin(42).expect("begin");
        buffer.push(event(1)).expect("first event");

        let error = buffer.push(event(2)).expect_err("event limit");

        assert!(matches!(
            error,
            TransactionBufferError::EventLimitExceeded {
                attempted: 2,
                maximum: 1
            }
        ));
    }

    #[test]
    fn hard_byte_limit_rejects_the_transaction_before_staging() {
        let temp = TempDir::new().expect("temp dir");
        let options = TransactionBufferOptions::bounded(temp.path(), "source", 1, 1, 10);
        let mut buffer = TransactionBuffer::new(options).expect("buffer");
        buffer.begin(42).expect("begin");

        let error = buffer.push(event(1)).expect_err("byte limit");

        assert!(matches!(
            error,
            TransactionBufferError::ByteLimitExceeded { maximum: 1, .. }
        ));
    }

    #[test]
    fn staging_preserves_the_configured_free_space_reserve() {
        let temp = TempDir::new().expect("temp dir");
        let options = TransactionBufferOptions::bounded(temp.path(), "source", 1, 1_000_000, 10)
            .with_min_free_disk_bytes(u64::MAX);
        let mut buffer = TransactionBuffer::new(options).expect("buffer");
        buffer.begin(42).expect("begin");

        let error = buffer.push(event(1)).expect_err("reserved space");

        assert!(matches!(
            error,
            TransactionBufferError::InsufficientStagingSpace { .. }
        ));
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
            key: None,
            before: None,
            after: Some(br#"{"id":"1"}"#.to_vec()),
            commit_timestamp_ms: None,
        }
    }
}
