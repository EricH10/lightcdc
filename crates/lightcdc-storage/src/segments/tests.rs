//! Segment lifecycle and durable-format regression tests.

use lightcdc_core::{Operation, SourceMetadata};
use redb::ReadableDatabase;
use tempfile::TempDir;

use super::catalog::{
    deleting_segment_path, segment_format_marker_path, table_exists, write_segment_format_marker,
};
use super::*;
use crate::{SourceIdentity, TransactionEvents, TransactionStats};

#[test]
fn replay_crosses_segments_and_survives_reopen() {
    let temp = TempDir::new().expect("temp dir");
    let control_path = temp.path().join("events.redb");
    let options = test_segment_options(2);
    let store = SegmentStore::open(control_path.clone(), options).expect("open segmented store");

    for sequence in 1..=5 {
        store
            .append_event(&event(sequence))
            .expect("append segmented event");
    }

    assert_eq!(
        store
            .replay_from(2, 4, u64::MAX)
            .expect("replay across segments")
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [2, 3, 4, 5]
    );
    let stats = store.stats().expect("segmented stats");
    assert_eq!(stats.segment_count, 3);
    assert_eq!(stats.sealed_segment_count, 2);
    drop(store);

    let reopened = SegmentStore::open(control_path, options).expect("reopen segmented store");
    assert_eq!(
        reopened
            .replay_from(1, 10, u64::MAX)
            .expect("replay reopened segments")
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    assert_eq!(reopened.next_sequence().expect("next sequence"), 6);
}

#[test]
fn oversized_source_transaction_stays_in_one_segment() {
    let temp = TempDir::new().expect("temp dir");
    let store = SegmentStore::open(temp.path().join("events.redb"), test_segment_options(2))
        .expect("open segmented store");
    let events = (1..=3).map(event).collect::<Vec<_>>();
    let transaction = TransactionEvents::InMemory {
        events,
        stats: TransactionStats {
            event_count: 3,
            decoded_bytes: 300,
            staged_bytes: 300,
        },
    };

    store
        .persist_transaction_batch(
            &[SourceTransaction {
                events: &transaction,
                source_lsn: "0/10",
            }],
            "default",
            true,
            || Ok(()),
        )
        .expect("persist oversized transaction");

    let state = read_state(&store.state);
    assert_eq!(state.descriptors.len(), 2);
    assert!(state.descriptors[0].sealed);
    assert_eq!(state.descriptors[0].event_count, 3);
    assert_eq!(state.descriptors[0].first_sequence, Some(1));
    assert_eq!(state.descriptors[0].last_sequence, Some(3));
    assert_eq!(state.descriptors[1].event_count, 0);
    assert!(!state.descriptors[1].sealed);
}

#[test]
fn serialized_byte_limit_rotates_without_splitting_an_event() {
    let temp = TempDir::new().expect("temp dir");
    let mut options = test_segment_options(100);
    options.max_bytes = 1;
    let store =
        SegmentStore::open(temp.path().join("events.redb"), options).expect("open segmented store");

    store
        .append_event(&event(1))
        .expect("append oversized event");

    let state = read_state(&store.state);
    assert_eq!(state.descriptors.len(), 2);
    assert!(state.descriptors[0].sealed);
    assert_eq!(state.descriptors[0].event_count, 1);
    assert_eq!(state.descriptors[0].first_sequence, Some(1));
    assert_eq!(state.descriptors[0].last_sequence, Some(1));
    assert!(!state.descriptors[1].sealed);
    assert_eq!(state.descriptors[1].event_count, 0);
}

#[test]
fn elapsed_age_seals_a_nonempty_active_segment() {
    let temp = TempDir::new().expect("temp dir");
    let store = SegmentStore::open(temp.path().join("events.redb"), test_segment_options(100))
        .expect("open segmented store");
    store.append_event(&event(1)).expect("append event");
    let created_at_ms = read_state(&store.state).descriptors[0].created_at_ms;

    store
        .rotate_aged_active(created_at_ms + 60_000)
        .expect("rotate aged segment");

    let state = read_state(&store.state);
    assert_eq!(state.descriptors.len(), 2);
    assert!(state.descriptors[0].sealed);
    assert_eq!(state.descriptors[0].event_count, 1);
    assert!(!state.descriptors[1].sealed);
    assert_eq!(state.descriptors[1].event_count, 0);
}

#[test]
fn legacy_single_file_store_migrates_into_a_segment() {
    let temp = TempDir::new().expect("temp dir");
    let control_path = temp.path().join("events.redb");
    create_legacy_store(&control_path);

    let store = SegmentStore::open(control_path.clone(), test_segment_options(10))
        .expect("migrate legacy store");

    assert_eq!(
        store
            .replay_from(1, 10, u64::MAX)
            .expect("replay migrated events"),
        [event(1)]
    );
    assert_eq!(
        store.source_offset("default").expect("source offset"),
        Some("0/1".to_owned())
    );
    assert_eq!(
        store
            .consumer_offset("orders", "search")
            .expect("consumer offset"),
        Some(1)
    );
    assert_eq!(store.stats().expect("stats").segment_count, 1);
    assert!(!table_exists(&store.control, "events").expect("list control tables"));
    drop(store);

    let reopened =
        SegmentStore::open(control_path, test_segment_options(10)).expect("reopen migrated store");
    assert_eq!(
        reopened.replay_from(1, 10, u64::MAX).expect("replay"),
        [event(1)]
    );
}

#[test]
fn newer_control_format_is_rejected_without_mutation() {
    let temp = TempDir::new().expect("temp dir");
    let control_path = temp.path().join("events.redb");
    let database = Database::create(&control_path).expect("create future control");
    let write = database.begin_write().expect("begin future format");
    {
        let mut metadata = write.open_table(METADATA).expect("metadata");
        metadata
            .insert(CONTROL_FORMAT_KEY, CONTROL_FORMAT_VERSION + 1)
            .expect("future format");
    }
    write.commit().expect("commit future format");
    drop(database);
    write_control_format_marker(&control_path, CONTROL_FORMAT_VERSION + 1)
        .expect("write future format marker");
    let before = fs::read(&control_path).expect("read control before rejection");

    let error = match SegmentStore::open(control_path.clone(), test_segment_options(10)) {
        Ok(_) => panic!("future format must be rejected"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StorageError::UnsupportedControlFormat {
            found: 3,
            supported: 2
        }
    ));
    assert_eq!(
        fs::read(&control_path).expect("read control after rejection"),
        before
    );
    assert!(
        !segment_directory(&control_path)
            .expect("segment directory")
            .exists()
    );
}

#[test]
fn version_one_segment_migrates_bare_event_payloads() {
    let temp = TempDir::new().expect("temp dir");
    let control_path = temp.path().join("events.redb");
    let options = test_segment_options(10);
    let stored_event = event(1);
    let store = SegmentStore::open(control_path.clone(), options).expect("open store");
    store.append_event(&stored_event).expect("append event");
    drop(store);

    let path = segment_path(
        &segment_directory(&control_path).expect("segment directory"),
        1,
    );
    let legacy_payload = serde_json::to_vec(&stored_event).expect("legacy payload");
    let database = Database::open(&path).expect("open segment");
    let write = database.begin_write().expect("begin downgrade");
    {
        let mut events = write.open_table(EVENTS).expect("events");
        events
            .insert(stored_event.sequence, legacy_payload.as_slice())
            .expect("legacy event");
        let mut metadata = write.open_table(METADATA).expect("metadata");
        metadata.insert(SEGMENT_FORMAT_KEY, 1).expect("v1 format");
        metadata
            .remove(EVENT_PAYLOAD_FORMAT_KEY)
            .expect("remove payload format");
        metadata
            .insert(SEGMENT_STORED_BYTES_KEY, legacy_payload.len() as u64)
            .expect("legacy byte count");
    }
    write.commit().expect("commit downgrade");
    drop(database);
    write_segment_format_marker(&path, 1, 0).expect("v1 format marker");

    let migrated = SegmentStore::open(control_path, options).expect("migrate segment");
    assert_eq!(
        migrated.replay_from(1, 10, u64::MAX).expect("replay"),
        [stored_event]
    );
    drop(migrated);

    let database = Database::open(path).expect("open migrated segment");
    let read = database.begin_read().expect("read migrated segment");
    let metadata = read.open_table(METADATA).expect("metadata");
    assert_eq!(
        metadata
            .get(SEGMENT_FORMAT_KEY)
            .expect("segment format")
            .expect("segment format value")
            .value(),
        SEGMENT_FORMAT_VERSION
    );
    assert_eq!(
        metadata
            .get(EVENT_PAYLOAD_FORMAT_KEY)
            .expect("payload format")
            .expect("payload format value")
            .value(),
        EVENT_PAYLOAD_FORMAT_VERSION
    );
}

#[test]
fn newer_segment_format_is_rejected_without_mutation() {
    let temp = TempDir::new().expect("temp dir");
    let control_path = temp.path().join("events.redb");
    let options = test_segment_options(10);
    let store = SegmentStore::open(control_path.clone(), options).expect("open store");
    store.append_event(&event(1)).expect("append event");
    drop(store);

    let path = segment_path(
        &segment_directory(&control_path).expect("segment directory"),
        1,
    );
    let database = Database::open(&path).expect("open segment");
    let write = database.begin_write().expect("begin future format");
    {
        let mut metadata = write.open_table(METADATA).expect("metadata");
        metadata
            .insert(SEGMENT_FORMAT_KEY, SEGMENT_FORMAT_VERSION + 1)
            .expect("future format");
    }
    write.commit().expect("commit future format");
    drop(database);
    write_segment_format_marker(
        &path,
        SEGMENT_FORMAT_VERSION + 1,
        EVENT_PAYLOAD_FORMAT_VERSION,
    )
    .expect("future format marker");
    let before = fs::read(&path).expect("segment before rejection");
    let marker = segment_format_marker_path(&path).expect("marker path");
    let marker_before = fs::read(&marker).expect("marker before rejection");

    let error = match SegmentStore::open(control_path, options) {
        Ok(_) => panic!("future segment must be rejected"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StorageError::UnsupportedSegmentFormat {
            found,
            supported,
            ..
        } if found == SEGMENT_FORMAT_VERSION + 1 && supported == SEGMENT_FORMAT_VERSION
    ));
    assert_eq!(fs::read(path).expect("segment after rejection"), before);
    assert_eq!(
        fs::read(marker).expect("marker after rejection"),
        marker_before
    );
}

#[test]
fn version_one_control_store_migrates_source_identity_table() {
    let temp = TempDir::new().expect("temp dir");
    let control_path = temp.path().join("events.redb");
    let database = Database::create(&control_path).expect("create v1 control");
    let write = database.begin_write().expect("begin v1 control");
    {
        write
            .open_table(CONSUMER_OFFSETS)
            .expect("consumer offsets");
        let mut metadata = write.open_table(METADATA).expect("metadata");
        metadata.insert(CONTROL_FORMAT_KEY, 1).expect("v1 format");
    }
    write.commit().expect("commit v1 control");
    drop(database);
    write_control_format_marker(&control_path, 1).expect("write v1 marker");

    let store = SegmentStore::open(control_path.clone(), test_segment_options(10))
        .expect("migrate v1 control");
    store
        .bind_source_identity(
            "default",
            &SourceIdentity {
                system_identifier: "7412345678901234567".to_owned(),
                database_oid: "16384".to_owned(),
            },
        )
        .expect("write identity after migration");

    assert_eq!(
        read_control_format_marker(&control_path).expect("read marker"),
        Some(CONTROL_FORMAT_VERSION)
    );
}

#[test]
fn interrupted_uncommitted_segment_deletion_is_rolled_back() {
    let temp = TempDir::new().expect("temp dir");
    let control_path = temp.path().join("events.redb");
    let options = test_segment_options(1);
    let store = SegmentStore::open(control_path.clone(), options).expect("open segmented store");
    store.append_event(&event(1)).expect("append first segment");
    let first_path = read_state(&store.state).descriptors[0].path.clone();
    drop(store);

    let deleting = deleting_segment_path(&first_path).expect("deleting path");
    fs::rename(&first_path, &deleting).expect("simulate interrupted deletion");

    let reopened = SegmentStore::open(control_path, options).expect("recover interrupted deletion");
    assert_eq!(
        reopened
            .replay_from(1, 1, u64::MAX)
            .expect("replay restored"),
        [event(1)]
    );
    assert!(first_path.exists());
    assert!(!deleting.exists());
}

fn create_legacy_store(path: &Path) {
    let database = Database::create(path).expect("create legacy database");
    let write = database.begin_write().expect("begin legacy write");
    {
        let payload = serde_json::to_vec(&event(1)).expect("serialize legacy event");
        let mut events = write.open_table(EVENTS).expect("legacy events");
        events.insert(1, payload.as_slice()).expect("legacy event");
        let mut event_ids = write.open_table(REPLAY_IDS).expect("legacy ids");
        event_ids.insert("event-1", 1).expect("legacy id");
        let mut source_offsets = write
            .open_table(SOURCE_OFFSETS)
            .expect("legacy source offsets");
        source_offsets
            .insert("default", "0/1")
            .expect("legacy source offset");
        write
            .open_table(SOURCE_REPLAY_FLOORS)
            .expect("legacy replay floors");
        let mut consumer_offsets = write
            .open_table(CONSUMER_OFFSETS)
            .expect("legacy consumer offsets");
        consumer_offsets
            .insert(consumer_offset_key("orders", "search").as_str(), 1)
            .expect("legacy consumer offset");
        let mut metadata = write.open_table(METADATA).expect("legacy metadata");
        metadata
            .insert(EVENT_HIGH_WATERMARK_KEY, 1)
            .expect("legacy high watermark");
    }
    write.commit().expect("commit legacy store");
}

fn test_segment_options(max_events: u64) -> SegmentOptions {
    SegmentOptions {
        max_events,
        max_bytes: 1024 * 1024,
        max_age: Duration::from_secs(60),
        sealed_cache_capacity: 2,
        active_cache_bytes: 1024 * 1024,
        sealed_cache_bytes: 1024 * 1024,
    }
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
        after: Some(format!(r#"{{"id":{sequence}}}"#).into_bytes()),
        commit_timestamp_ms: Some(1_784_862_080_000 + sequence as i64),
    }
}
