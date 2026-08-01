//! Connects PostgreSQL logical replication to LightCDC's transaction model.

/// pgoutput protocol decoding.
pub mod decoder;
/// PostgreSQL logical replication capture.
pub mod replication;

pub use decoder::{PgOutputDecoder, PgOutputMessage};
pub use replication::{
    CapturedTransaction, LogicalHeartbeatEmitter, PostgresError, PublicationAlignment,
    ReplicationReader, SourceValidation, TransactionRead, validate_resume_lsn,
    validate_source_config, validate_source_config_with_plan,
};
