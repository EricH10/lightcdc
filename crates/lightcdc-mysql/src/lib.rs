/// MySQL row-based binlog capture.
pub mod replication;

pub use replication::{
    BinlogPosition, CapturedTransaction, MySqlError, ReplicationReader, validate_source_config,
};
