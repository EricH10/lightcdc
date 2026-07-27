use std::fmt;

use futures_util::StreamExt;
use lightcdc_core::{
    ChangeEvent, MySqlSourceConfig, Operation, SourceMetadata, TransactionMetadata,
};
use mysql_async::{
    BinlogStream, BinlogStreamRequest, Conn, Opts, OptsBuilder, Row, Value,
    binlog::{
        events::{EventData, RowsEventData},
        row::BinlogRow,
        value::BinlogValue,
    },
    consts::{ColumnFlags, ColumnType},
    prelude::Queryable,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number};
use thiserror::Error;
use tracing::{debug, info};

/// Represents failures while connecting to or decoding MySQL binlog replication.
#[derive(Debug, Error)]
pub enum MySqlError {
    #[error("MySQL client error: {0}")]
    Client(#[from] mysql_async::Error),

    #[error("MySQL binlog decode error: {0}")]
    Decode(#[from] std::io::Error),

    #[error("invalid MySQL binlog checkpoint: {0}")]
    InvalidCheckpoint(#[from] serde_json::Error),

    #[error("MySQL source configuration error: {0}")]
    Configuration(String),

    #[error("MySQL row conversion error: {0}")]
    Row(String),
}

/// Identifies the next event position in a MySQL binary log file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct BinlogPosition {
    pub filename: String,
    pub position: u64,
}

impl BinlogPosition {
    /// Parses a checkpoint previously produced by this connector.
    pub fn parse(value: &str) -> Result<Self, MySqlError> {
        Ok(serde_json::from_str(value)?)
    }
}

impl fmt::Display for BinlogPosition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let encoded = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        formatter.write_str(&encoded)
    }
}

/// Bundles one committed MySQL transaction with its durable resume position.
#[derive(Debug, Clone)]
pub struct CapturedTransaction {
    pub events: Vec<ChangeEvent>,
    pub checkpoint: BinlogPosition,
}

/// Reads row-based MySQL binlog events and emits committed transactions.
pub struct ReplicationReader {
    source: MySqlSourceConfig,
    stream: Option<BinlogStream>,
    filename: String,
    sequence: u64,
    transaction_id: Option<u64>,
    pending_events: Vec<ChangeEvent>,
}

impl ReplicationReader {
    /// Connects from a stored checkpoint or the current end of the binlog.
    pub async fn connect_from(
        source: MySqlSourceConfig,
        checkpoint: Option<&str>,
    ) -> Result<Self, MySqlError> {
        let mut connection = Conn::new(connection_options(&source)).await?;
        let position = match checkpoint {
            Some(checkpoint) => BinlogPosition::parse(checkpoint)?,
            None => current_binlog_position(&mut connection).await?,
        };

        info!(
            source = %source.redacted_connection_string(),
            server_id = source.server_id,
            filename = %position.filename,
            position = position.position,
            "starting MySQL binlog replication"
        );

        let request = BinlogStreamRequest::new(source.server_id)
            .with_filename(position.filename.as_bytes())
            .with_pos(position.position);
        let stream = connection.get_binlog_stream(request).await?;

        Ok(Self {
            source,
            stream: Some(stream),
            filename: position.filename,
            sequence: 0,
            transaction_id: None,
            pending_events: Vec::new(),
        })
    }

    /// Sets the local sequence number assigned to the next captured event.
    pub fn set_next_sequence(&mut self, sequence: u64) {
        self.sequence = sequence.saturating_sub(1);
    }

    /// Waits until the next MySQL transaction commit.
    pub async fn next_transaction(&mut self) -> Result<Option<CapturedTransaction>, MySqlError> {
        loop {
            let event = match self
                .stream
                .as_mut()
                .expect("binlog stream is available until shutdown")
                .next()
                .await
                .transpose()?
            {
                Some(event) => event,
                None => return Ok(None),
            };
            let header = event.header();

            match event.read_data()? {
                Some(EventData::RotateEvent(rotate)) => {
                    self.filename = rotate.name().into_owned();
                    debug!(
                        filename = %self.filename,
                        position = rotate.position(),
                        "MySQL binlog rotated"
                    );
                }
                Some(EventData::GtidEvent(gtid)) => {
                    self.transaction_id = Some(gtid.gno());
                }
                Some(EventData::RowsEvent(rows)) => {
                    self.capture_rows(&rows, header.timestamp(), header.log_pos())?;
                }
                Some(EventData::XidEvent(xid)) => {
                    return Ok(Some(
                        self.finish_transaction(xid.xid, header.log_pos() as u64),
                    ));
                }
                Some(EventData::QueryEvent(query))
                    if query.query().trim().eq_ignore_ascii_case("COMMIT") =>
                {
                    let transaction_id = self.transaction_id.unwrap_or_default();
                    return Ok(Some(
                        self.finish_transaction(transaction_id, header.log_pos() as u64),
                    ));
                }
                _ => {}
            }
        }
    }

    /// Closes the MySQL binlog connection.
    pub async fn shutdown(&mut self) -> Result<(), MySqlError> {
        if let Some(stream) = self.stream.take() {
            stream.close().await?;
        }
        Ok(())
    }

    fn capture_rows(
        &mut self,
        rows: &RowsEventData<'_>,
        timestamp_seconds: u32,
        event_position: u32,
    ) -> Result<(), MySqlError> {
        let stream = self
            .stream
            .as_ref()
            .expect("binlog stream is available until shutdown");
        let table_map = stream.get_tme(rows.table_id()).cloned().ok_or_else(|| {
            MySqlError::Row(format!(
                "missing table map event for table id {}",
                rows.table_id()
            ))
        })?;
        let schema = table_map.database_name().into_owned();

        if schema != self.source.database {
            return Ok(());
        }

        let table = table_map.table_name().into_owned();
        let operation = operation(rows);
        let transaction = TransactionMetadata {
            transaction_id: self.transaction_id,
            begin_lsn: None,
            commit_lsn: None,
        };

        for (row_index, row) in rows.rows(&table_map).enumerate() {
            let (before, after) = row.map_err(|error| MySqlError::Row(error.to_string()))?;
            let key_row = after.as_ref().or(before.as_ref());
            let key = key_row.map(primary_key_json).transpose()?.flatten();
            let before = before.map(row_json).transpose()?;
            let after = after.map(row_json).transpose()?;
            self.sequence += 1;

            self.pending_events.push(ChangeEvent {
                sequence: self.sequence,
                event_id: format!(
                    "mysql:{}:{}:{}:{}",
                    self.source.database, self.filename, event_position, row_index
                ),
                source: SourceMetadata {
                    database: self.source.database.clone(),
                    slot: format!("server-id-{}", self.source.server_id),
                    lsn: format!("{}:{}", self.filename, event_position),
                },
                transaction: Some(transaction.clone()),
                schema: schema.clone(),
                table: table.clone(),
                operation,
                key,
                before,
                after,
                commit_timestamp_ms: Some(i64::from(timestamp_seconds) * 1_000),
            });
        }

        Ok(())
    }

    fn finish_transaction(&mut self, transaction_id: u64, position: u64) -> CapturedTransaction {
        let checkpoint = BinlogPosition {
            filename: self.filename.clone(),
            position,
        };
        let checkpoint_string = checkpoint.to_string();

        for event in &mut self.pending_events {
            if let Some(transaction) = &mut event.transaction {
                transaction.transaction_id = Some(transaction_id);
                transaction.commit_lsn = Some(checkpoint_string.clone());
            }
        }
        self.transaction_id = None;

        CapturedTransaction {
            events: std::mem::take(&mut self.pending_events),
            checkpoint,
        }
    }
}

/// Validates the MySQL settings required for deterministic row decoding.
pub async fn validate_source_config(config: &MySqlSourceConfig) -> Result<(), MySqlError> {
    if config.server_id == 0 {
        return Err(MySqlError::Configuration(
            "server_id must be a non-zero identifier unique among replication clients".to_owned(),
        ));
    }

    let mut connection = Conn::new(connection_options(config)).await?;
    let row: Row = connection
        .query_first(
            "SELECT @@global.log_bin, @@global.binlog_format, \
             @@global.binlog_row_image, @@global.binlog_row_metadata, \
             @@global.binlog_row_value_options",
        )
        .await?
        .ok_or_else(|| MySqlError::Configuration("validation query returned no row".to_owned()))?;

    let log_bin: u8 = required_column(&row, 0, "log_bin")?;
    let binlog_format: String = required_column(&row, 1, "binlog_format")?;
    let row_image: String = required_column(&row, 2, "binlog_row_image")?;
    let row_metadata: String = required_column(&row, 3, "binlog_row_metadata")?;
    let row_value_options: String = required_column(&row, 4, "binlog_row_value_options")?;

    if log_bin != 1 {
        return Err(MySqlError::Configuration(
            "binary logging must be enabled".to_owned(),
        ));
    }
    if !binlog_format.eq_ignore_ascii_case("ROW") {
        return Err(MySqlError::Configuration(format!(
            "binlog_format must be ROW, found {binlog_format}"
        )));
    }
    if !row_image.eq_ignore_ascii_case("FULL") {
        return Err(MySqlError::Configuration(format!(
            "binlog_row_image must be FULL, found {row_image}"
        )));
    }
    if !row_metadata.eq_ignore_ascii_case("FULL") {
        return Err(MySqlError::Configuration(format!(
            "binlog_row_metadata must be FULL, found {row_metadata}"
        )));
    }
    if !row_value_options.is_empty() {
        return Err(MySqlError::Configuration(format!(
            "binlog_row_value_options must be empty, found {row_value_options}"
        )));
    }

    info!(
        database = %config.database,
        user = %config.user,
        server_id = config.server_id,
        "validated MySQL source config"
    );
    connection.disconnect().await?;
    Ok(())
}

fn connection_options(config: &MySqlSourceConfig) -> Opts {
    OptsBuilder::default()
        .ip_or_hostname(&config.host)
        .tcp_port(config.port)
        .user(Some(&config.user))
        .pass(Some(&config.password))
        .db_name(Some(&config.database))
        .prefer_socket(false)
        .into()
}

async fn current_binlog_position(connection: &mut Conn) -> Result<BinlogPosition, MySqlError> {
    let row: Row = connection
        .query_first("SHOW BINARY LOG STATUS")
        .await?
        .ok_or_else(|| {
            MySqlError::Configuration("SHOW BINARY LOG STATUS returned no row".to_owned())
        })?;

    Ok(BinlogPosition {
        filename: required_column(&row, 0, "File")?,
        position: required_column(&row, 1, "Position")?,
    })
}

fn required_column<T>(row: &Row, index: usize, name: &str) -> Result<T, MySqlError>
where
    T: mysql_async::prelude::FromValue,
{
    row.get_opt(index)
        .ok_or_else(|| MySqlError::Configuration(format!("missing {name} column")))?
        .map_err(|error| MySqlError::Configuration(format!("invalid {name} column value: {error}")))
}

fn operation(rows: &RowsEventData<'_>) -> Operation {
    match rows {
        RowsEventData::WriteRowsEventV1(_) | RowsEventData::WriteRowsEvent(_) => Operation::Insert,
        RowsEventData::DeleteRowsEventV1(_) | RowsEventData::DeleteRowsEvent(_) => {
            Operation::Delete
        }
        RowsEventData::UpdateRowsEventV1(_)
        | RowsEventData::UpdateRowsEvent(_)
        | RowsEventData::PartialUpdateRowsEvent(_) => Operation::Update,
    }
}

fn row_json(row: BinlogRow) -> Result<Vec<u8>, MySqlError> {
    Ok(serde_json::to_vec(&row_object(&row, false)?)?)
}

fn primary_key_json(row: &BinlogRow) -> Result<Option<Vec<u8>>, MySqlError> {
    let object = row_object(row, true)?;
    if object.is_empty() {
        Ok(None)
    } else {
        Ok(Some(serde_json::to_vec(&object)?))
    }
}

fn row_object(
    row: &BinlogRow,
    primary_keys_only: bool,
) -> Result<Map<String, serde_json::Value>, MySqlError> {
    let mut object = Map::new();

    for (index, column) in row.columns_ref().iter().enumerate() {
        if primary_keys_only && !column.flags().contains(ColumnFlags::PRI_KEY_FLAG) {
            continue;
        }
        let value = row
            .as_ref(index)
            .ok_or_else(|| MySqlError::Row(format!("missing row value at column {index}")))?;
        object.insert(
            column.name_str().into_owned(),
            binlog_value_json(value.clone(), column.column_type())?,
        );
    }

    Ok(object)
}

fn binlog_value_json(
    value: BinlogValue<'_>,
    column_type: ColumnType,
) -> Result<serde_json::Value, MySqlError> {
    let value = Value::try_from(value).map_err(|error| MySqlError::Row(error.to_string()))?;

    Ok(match value {
        Value::NULL => serde_json::Value::Null,
        Value::Int(value) => serde_json::Value::Number(value.into()),
        Value::UInt(value) => serde_json::Value::Number(value.into()),
        Value::Float(value) => Number::from_f64(f64::from(value))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Double(value) => Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Bytes(value) if column_type == ColumnType::MYSQL_TYPE_JSON => {
            serde_json::from_slice(&value).map_err(|error| MySqlError::Row(error.to_string()))?
        }
        Value::Bytes(value) => match String::from_utf8(value) {
            Ok(value) => serde_json::Value::String(value),
            Err(error) => serde_json::Value::String(hex_bytes(error.as_bytes())),
        },
        Value::Date(year, month, day, hour, minute, second, micros) => {
            let suffix = if micros == 0 {
                String::new()
            } else {
                format!(".{micros:06}")
            };
            serde_json::Value::String(format!(
                "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{suffix}"
            ))
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let sign = if negative { "-" } else { "" };
            let total_hours = days * 24 + u32::from(hours);
            let suffix = if micros == 0 {
                String::new()
            } else {
                format!(".{micros:06}")
            };
            serde_json::Value::String(format!(
                "{sign}{total_hours:02}:{minutes:02}:{seconds:02}{suffix}"
            ))
        }
    })
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(2 + bytes.len() * 2);
    output.push_str("0x");
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binlog_position_round_trips_through_storage_string() {
        let position = BinlogPosition {
            filename: "mysql-bin.000042".to_owned(),
            position: 8_192,
        };

        assert_eq!(
            BinlogPosition::parse(&position.to_string()).expect("parse checkpoint"),
            position
        );
    }

    #[test]
    fn binary_values_use_a_stable_hex_representation() {
        assert_eq!(hex_bytes(&[0, 127, 255]), "0x007fff");
    }
}
