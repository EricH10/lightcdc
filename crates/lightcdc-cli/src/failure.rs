//! Defines stable process exit classes for production supervisors.

use std::{fmt, process::ExitCode};

use lightcdc_postgres::PostgresError;
use lightcdc_storage::{StorageError, TransactionBufferError};

pub(crate) const CONFIGURATION_EXIT_CODE: u8 = 10;
pub(crate) const DATA_SAFETY_EXIT_CODE: u8 = 20;
const RUNTIME_EXIT_CODE: u8 = 1;

/// Stable terminal classes exposed to service managers and operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExitClass {
    Configuration,
    DataSafety,
    Runtime,
}

impl ExitClass {
    pub(crate) fn code(self) -> ExitCode {
        let code = match self {
            Self::Configuration => CONFIGURATION_EXIT_CODE,
            Self::DataSafety => DATA_SAFETY_EXIT_CODE,
            Self::Runtime => RUNTIME_EXIT_CODE,
        };
        ExitCode::from(code)
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Configuration => "configuration",
            Self::DataSafety => "data_safety",
            Self::Runtime => "runtime",
        }
    }
}

/// Marks errors whose underlying library type does not encode configuration intent.
#[derive(Debug)]
struct ConfigurationMarker;

impl fmt::Display for ConfigurationMarker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("configuration requires operator action")
    }
}

/// Adds a typed configuration marker while preserving the original error chain.
pub(crate) fn configuration(error: anyhow::Error) -> anyhow::Error {
    error.context(ConfigurationMarker)
}

/// Classifies a complete anyhow chain without relying on rendered error text.
pub(crate) fn classify(error: &anyhow::Error) -> ExitClass {
    if error.downcast_ref::<ConfigurationMarker>().is_some()
        || error.downcast_ref::<lightcdc_core::Error>().is_some()
        || error
            .downcast_ref::<lightcdc_api::ApiConfigurationFailure>()
            .is_some()
    {
        return ExitClass::Configuration;
    }

    if let Some(postgres) = error.downcast_ref::<PostgresError>() {
        return match postgres {
            PostgresError::ResumeLsnGap { .. } => ExitClass::DataSafety,
            PostgresError::TransactionBuffer(transaction_error)
                if transaction_error_is_data_safety(transaction_error) =>
            {
                ExitClass::DataSafety
            }
            PostgresError::Configuration(_)
            | PostgresError::Connect(_)
            | PostgresError::Replication(_)
            | PostgresError::StreamState(_)
            | PostgresError::Decode(_)
            | PostgresError::TransactionBuffer(_)
            | PostgresError::UnsupportedFeature(_) => ExitClass::Configuration,
        };
    }

    if error
        .downcast_ref::<StorageError>()
        .is_some_and(storage_error_is_data_safety)
        || error
            .downcast_ref::<TransactionBufferError>()
            .is_some_and(transaction_error_is_data_safety)
    {
        return ExitClass::DataSafety;
    }

    ExitClass::Runtime
}

fn storage_error_is_data_safety(error: &StorageError) -> bool {
    if let StorageError::TransactionBuffer(transaction_error) = error {
        return transaction_error_is_data_safety(transaction_error);
    }
    matches!(
        error,
        StorageError::SourceIdentityMismatch { .. }
            | StorageError::PartiallyPersistedTransaction { .. }
            | StorageError::InvalidConsumerOffsetKey(_)
            | StorageError::InvalidSegmentCatalog(_)
            | StorageError::Integrity(_)
            | StorageError::InvalidFormatMarker(_)
            | StorageError::MissingActiveSegment
            | StorageError::UnsupportedControlFormat { .. }
            | StorageError::UnsupportedSegmentFormat { .. }
            | StorageError::UnsupportedEventPayloadFormat { .. }
    )
}

fn transaction_error_is_data_safety(error: &TransactionBufferError) -> bool {
    matches!(
        error,
        TransactionBufferError::InvalidRecordLength
            | TransactionBufferError::InvalidStagingHeader
            | TransactionBufferError::UnsupportedStagingFormat { .. }
    )
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn classifies_configuration_data_safety_and_runtime_chains() {
        let config = anyhow::Error::new(lightcdc_core::Error::InvalidConfig {
            path: "lightcdc.toml".to_owned(),
            reason: "bad limit".to_owned(),
        })
        .context("load config");
        assert_eq!(classify(&config), ExitClass::Configuration);

        let gap = anyhow::Error::new(PostgresError::ResumeLsnGap {
            local_lsn: "0/10".to_owned(),
            confirmed_flush_lsn: "0/20".to_owned(),
        })
        .context("validate source");
        assert_eq!(classify(&gap), ExitClass::DataSafety);

        let corrupt = anyhow::Error::new(StorageError::Integrity("bad event".to_owned()))
            .context("open store");
        assert_eq!(classify(&corrupt), ExitClass::DataSafety);

        let runtime = anyhow::Error::new(io::Error::new(
            io::ErrorKind::AddrInUse,
            "listener already bound",
        ));
        assert_eq!(classify(&runtime), ExitClass::Runtime);
    }

    #[test]
    fn explicit_marker_classifies_untyped_configuration_errors() {
        let error = configuration(anyhow::anyhow!("secret environment variable is missing"));
        assert_eq!(classify(&error), ExitClass::Configuration);
    }
}
