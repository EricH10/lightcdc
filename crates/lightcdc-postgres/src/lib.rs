/// pgoutput protocol decoding.
pub mod decoder;
/// PostgreSQL logical replication capture.
pub mod replication;

pub use decoder::{PgOutputDecoder, PgOutputMessage};
pub use replication::{
    CapturedTransaction, PostgresError, ReplicationReader, validate_source_config,
};
