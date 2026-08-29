//! Decodes stateful PostgreSQL `pgoutput` messages into relation-aware row changes.

use std::collections::{HashMap, VecDeque};

use serde::Serialize;
use serde_json::{Map, Value};
use thiserror::Error;

/// Represents failures while parsing PostgreSQL pgoutput bytes.
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("unexpected end of pgoutput message")]
    Eof,

    #[error("unsupported pgoutput message tag: {0:?}")]
    UnsupportedMessage(char),

    #[error("unexpected tuple tag: {0:?}")]
    UnexpectedTupleTag(char),

    #[error("invalid utf-8 text value: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("tuple column count {actual} does not match relation column count {expected}")]
    ColumnCount { expected: usize, actual: usize },

    #[error("missing relation metadata for relation id {0}")]
    MissingRelation(u32),

    #[error("failed to serialize decoded row: {0}")]
    Json(#[from] serde_json::Error),
}

/// Represents one decoded pgoutput protocol message.
#[derive(Debug, Clone)]
pub enum PgOutputMessage {
    /// Announces or refreshes metadata for a table relation id.
    Relation(Relation),
    /// Contains the new tuple for an inserted row.
    Insert(RowChange),
    /// Contains the available old/key tuple and required new tuple.
    Update(RowChange),
    /// Contains the replica-identity key or old tuple for a deleted row.
    Delete(RowChange),
    /// Names every relation affected by one truncate command.
    Truncate(Vec<Relation>),
    /// Represents protocol metadata that capture does not currently consume.
    Ignored,
}

/// Describes a table relation announced by pgoutput.
#[derive(Debug, Clone)]
pub struct Relation {
    /// The pgoutput relation id used by later row messages.
    pub id: u32,
    /// The schema name for the relation.
    pub namespace: String,
    /// The table name for the relation.
    pub name: String,
    /// The PostgreSQL replica identity setting for the relation.
    pub replica_identity: u8,
    /// The ordered column metadata for the relation.
    pub columns: Vec<Column>,
}

/// Describes one column in a pgoutput relation message.
#[derive(Debug, Clone)]
pub struct Column {
    /// The pgoutput flags for this column.
    pub flags: u8,
    /// The column name.
    pub name: String,
    /// The PostgreSQL type OID for this column.
    pub type_oid: u32,
    /// The PostgreSQL type modifier for this column.
    pub type_modifier: i32,
}

/// Carries decoded row images for one insert, update, or delete.
#[derive(Debug, Clone)]
pub struct RowChange {
    /// The table relation that changed.
    pub relation: Relation,
    /// The key tuple when PostgreSQL sends it.
    pub key: Option<Vec<u8>>,
    /// The old tuple when PostgreSQL sends it.
    pub old: Option<Vec<u8>>,
    /// The new tuple when PostgreSQL sends it.
    pub new: Option<Vec<u8>>,
}

/// Maximum number of relation metadata entries cached per session.
///
/// pgoutput re-announces relation metadata on demand, so when the cache
/// exceeds this cap the least-recently-announced entry can be evicted
/// safely: a row message for an evicted relation is always preceded by a
/// fresh Relation message. Evicting the oldest entry (rather than clearing
/// the whole map) keeps every relation announced by the current transaction,
/// which a later Truncate message in the same transaction may still
/// reference. The cap bounds decoder memory under sustained schema churn
/// (e.g. many short-lived tables).
const RELATION_CACHE_CAP: usize = 4096;

/// Decodes pgoutput messages while remembering relation metadata.
#[derive(Debug, Default)]
pub struct PgOutputDecoder {
    relations: HashMap<u32, Relation>,
    /// Announcement order of cached relation ids; the front is the eviction
    /// candidate when the cache exceeds its cap.
    relation_order: VecDeque<u32>,
}

impl PgOutputDecoder {
    /// Decodes one pgoutput message from PostgreSQL.
    pub fn decode(&mut self, data: &[u8]) -> Result<PgOutputMessage, DecodeError> {
        let mut reader = PgOutputReader::new(data);
        let tag = reader.read_u8()? as char;

        match tag {
            'R' => {
                let relation = decode_relation(&mut reader)?;
                // pgoutput re-announces relation metadata on demand, so the
                // cache can be bounded: evict the least-recently-announced
                // relation when the cap is exceeded. Relations announced by
                // the current transaction are the newest entries and survive
                // eviction, so Truncate resolution (which references every
                // affected relation at once) keeps working.
                if !self.relations.contains_key(&relation.id) {
                    self.relation_order.push_back(relation.id);
                }
                self.relations.insert(relation.id, relation.clone());
                while self.relations.len() > RELATION_CACHE_CAP {
                    let id = self
                        .relation_order
                        .pop_front()
                        .expect("eviction order tracks cached relations");
                    self.relations.remove(&id);
                }
                Ok(PgOutputMessage::Relation(relation))
            }
            'I' => self.decode_insert(&mut reader),
            'U' => self.decode_update(&mut reader),
            'D' => self.decode_delete(&mut reader),
            'T' => self.decode_truncate(&mut reader),
            'O' | 'Y' => Ok(PgOutputMessage::Ignored),
            other => Err(DecodeError::UnsupportedMessage(other)),
        }
    }

    /// Decodes an insert message into a row change.
    fn decode_insert(
        &self,
        reader: &mut PgOutputReader<'_>,
    ) -> Result<PgOutputMessage, DecodeError> {
        let relation_id = reader.read_u32()?;
        let relation = self.relation(relation_id)?.clone();
        reader.expect_tag('N')?;
        let new = Some(decode_tuple_json(reader, &relation)?);

        Ok(PgOutputMessage::Insert(RowChange {
            relation,
            key: None,
            old: None,
            new,
        }))
    }

    /// Decodes an update message into a row change.
    fn decode_update(
        &self,
        reader: &mut PgOutputReader<'_>,
    ) -> Result<PgOutputMessage, DecodeError> {
        let relation_id = reader.read_u32()?;
        let relation = self.relation(relation_id)?.clone();

        let next = reader.read_u8()? as char;
        let (key, old, new_tag) = match next {
            'K' => (
                Some(decode_tuple_json(reader, &relation)?),
                None,
                reader.read_u8()? as char,
            ),
            'O' => (
                None,
                Some(decode_tuple_json(reader, &relation)?),
                reader.read_u8()? as char,
            ),
            'N' => (None, None, 'N'),
            other => return Err(DecodeError::UnexpectedTupleTag(other)),
        };

        if new_tag != 'N' {
            return Err(DecodeError::UnexpectedTupleTag(new_tag));
        }

        let new = Some(decode_tuple_json(reader, &relation)?);

        Ok(PgOutputMessage::Update(RowChange {
            relation,
            key,
            old,
            new,
        }))
    }

    /// Decodes a delete message into a row change.
    fn decode_delete(
        &self,
        reader: &mut PgOutputReader<'_>,
    ) -> Result<PgOutputMessage, DecodeError> {
        let relation_id = reader.read_u32()?;
        let relation = self.relation(relation_id)?.clone();

        let tuple_tag = reader.read_u8()? as char;
        let row = decode_tuple_json(reader, &relation)?;
        let (key, old) = match tuple_tag {
            'K' => (Some(row), None),
            'O' => (None, Some(row)),
            other => return Err(DecodeError::UnexpectedTupleTag(other)),
        };

        Ok(PgOutputMessage::Delete(RowChange {
            relation,
            key,
            old,
            new: None,
        }))
    }

    /// Resolves a truncate message into the affected table relations.
    fn decode_truncate(
        &self,
        reader: &mut PgOutputReader<'_>,
    ) -> Result<PgOutputMessage, DecodeError> {
        let relation_count = reader.read_u32()? as usize;
        let _options = reader.read_u8()?;
        let mut relations = Vec::with_capacity(relation_count);

        for _ in 0..relation_count {
            relations.push(self.relation(reader.read_u32()?)?.clone());
        }

        Ok(PgOutputMessage::Truncate(relations))
    }

    /// Finds relation metadata previously announced by pgoutput.
    fn relation(&self, relation_id: u32) -> Result<&Relation, DecodeError> {
        self.relations
            .get(&relation_id)
            .ok_or(DecodeError::MissingRelation(relation_id))
    }
}

/// Decodes relation metadata and stores its column order.
fn decode_relation(reader: &mut PgOutputReader<'_>) -> Result<Relation, DecodeError> {
    let id = reader.read_u32()?;
    let namespace = reader.read_cstring()?;
    let name = reader.read_cstring()?;
    let replica_identity = reader.read_u8()?;
    let column_count = reader.read_u16()? as usize;
    let mut columns = Vec::with_capacity(column_count);

    for _ in 0..column_count {
        columns.push(Column {
            flags: reader.read_u8()?,
            name: reader.read_cstring()?,
            type_oid: reader.read_u32()?,
            type_modifier: reader.read_i32()?,
        });
    }

    Ok(Relation {
        id,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

/// Decodes a pgoutput tuple into JSON bytes keyed by column name.
fn decode_tuple_json(
    reader: &mut PgOutputReader<'_>,
    relation: &Relation,
) -> Result<Vec<u8>, DecodeError> {
    let value_count = reader.read_u16()? as usize;
    if value_count != relation.columns.len() {
        return Err(DecodeError::ColumnCount {
            expected: relation.columns.len(),
            actual: value_count,
        });
    }

    let mut row = Map::with_capacity(value_count);
    for column in &relation.columns {
        let value_tag = reader.read_u8()? as char;
        let value = match value_tag {
            'n' => Value::Null,
            'u' => serde_json::json!({ "__unchanged_toast": true }),
            't' => {
                let bytes = reader.read_len_bytes()?;
                Value::String(std::str::from_utf8(bytes)?.to_owned())
            }
            'b' => {
                let bytes = reader.read_len_bytes()?;
                serde_json::json!({ "__binary_hex": hex_encode(bytes) })
            }
            other => return Err(DecodeError::UnexpectedTupleTag(other)),
        };

        row.insert(column.name.clone(), value);
    }

    serde_json::to_vec(&Value::Object(row)).map_err(DecodeError::from)
}

/// Encodes binary tuple values as lowercase hexadecimal text.
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Reads primitive values from a pgoutput message byte slice.
struct PgOutputReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> PgOutputReader<'a> {
    /// Creates a reader at the beginning of a pgoutput message.
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    /// Reads one unsigned byte.
    fn read_u8(&mut self) -> Result<u8, DecodeError> {
        let bytes = self.read_exact(1)?;
        Ok(bytes[0])
    }

    /// Reads a big-endian unsigned 16-bit integer.
    fn read_u16(&mut self) -> Result<u16, DecodeError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    /// Reads a big-endian unsigned 32-bit integer.
    fn read_u32(&mut self) -> Result<u32, DecodeError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Reads a big-endian signed 32-bit integer.
    fn read_i32(&mut self) -> Result<i32, DecodeError> {
        let bytes = self.read_exact(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Reads a null-terminated UTF-8 string.
    fn read_cstring(&mut self) -> Result<String, DecodeError> {
        let remaining = &self.data[self.offset..];
        let end = remaining
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(DecodeError::Eof)?;
        let value = std::str::from_utf8(&remaining[..end])?.to_owned();
        self.offset += end + 1;
        Ok(value)
    }

    /// Reads bytes prefixed by a 32-bit length.
    fn read_len_bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.read_u32()? as usize;
        self.read_exact(len)
    }

    /// Verifies that the next byte matches an expected pgoutput tag.
    fn expect_tag(&mut self, expected: char) -> Result<(), DecodeError> {
        let actual = self.read_u8()? as char;
        if actual == expected {
            Ok(())
        } else {
            Err(DecodeError::UnexpectedTupleTag(actual))
        }
    }

    /// Reads an exact number of bytes or returns EOF.
    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.offset.checked_add(len).ok_or(DecodeError::Eof)?;
        if end > self.data.len() {
            return Err(DecodeError::Eof);
        }

        let bytes = &self.data[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

/// Provides a serialization-friendly view of cached relation metadata.
#[derive(Debug, Serialize)]
pub struct RelationSummary<'a> {
    pub id: u32,
    pub namespace: &'a str,
    pub name: &'a str,
    pub columns: Vec<&'a str>,
}

impl Relation {
    /// Borrows the relation fields needed for diagnostic output.
    pub fn summary(&self) -> RelationSummary<'_> {
        RelationSummary {
            id: self.id,
            namespace: &self.namespace,
            name: &self.name,
            columns: self
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DecodeError, PgOutputDecoder, PgOutputMessage, RELATION_CACHE_CAP};

    /// Builds a pgoutput Relation message announcing `id` with one column.
    fn relation_message(id: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(b'R');
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.extend_from_slice(b"public\0");
        bytes.extend_from_slice(b"events\0");
        bytes.push(0); // replica identity: default
        bytes.extend_from_slice(&1u16.to_be_bytes()); // one column
        bytes.push(0); // column flags: none
        bytes.extend_from_slice(b"id\0");
        bytes.extend_from_slice(&23u32.to_be_bytes()); // int4 type oid
        bytes.extend_from_slice(&(-1i32).to_be_bytes()); // no type modifier
        bytes
    }

    /// Builds a pgoutput Insert message for `relation_id` with one null column.
    fn insert_message(relation_id: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(b'I');
        bytes.extend_from_slice(&relation_id.to_be_bytes());
        bytes.push(b'N');
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.push(b'n');
        bytes
    }

    #[test]
    fn relation_cache_stays_bounded_under_schema_churn() {
        let mut decoder = PgOutputDecoder::default();

        for id in 0..RELATION_CACHE_CAP as u32 {
            decoder.decode(&relation_message(id)).expect("relation decodes");
        }
        assert_eq!(decoder.relations.len(), RELATION_CACHE_CAP);

        // One more announcement evicts the least-recently-announced relation
        // and keeps the cache within its cap.
        decoder
            .decode(&relation_message(RELATION_CACHE_CAP as u32))
            .expect("relation decodes");
        assert_eq!(decoder.relations.len(), RELATION_CACHE_CAP);
        assert!(!decoder.relations.contains_key(&0));
        assert!(decoder.relations.contains_key(&(RELATION_CACHE_CAP as u32)));
    }

    #[test]
    fn reannounced_relation_decodes_rows_after_eviction() {
        let mut decoder = PgOutputDecoder::default();

        for id in 0..RELATION_CACHE_CAP as u32 {
            decoder.decode(&relation_message(id)).expect("relation decodes");
        }
        // Evicts relation 0.
        decoder
            .decode(&relation_message(RELATION_CACHE_CAP as u32))
            .expect("relation decodes");

        // Row messages for an evicted relation fail until pgoutput
        // re-announces it.
        assert!(matches!(
            decoder.decode(&insert_message(0)),
            Err(DecodeError::MissingRelation(0))
        ));

        // Re-announcing the relation makes its rows decodable again, and the
        // cache stays within its cap.
        decoder
            .decode(&relation_message(0))
            .expect("re-announced relation decodes");
        assert!(matches!(
            decoder.decode(&insert_message(0)),
            Ok(PgOutputMessage::Insert(_))
        ));
        assert_eq!(decoder.relations.len(), RELATION_CACHE_CAP);
    }

    #[test]
    fn truncate_resolves_relations_announced_by_the_current_transaction() {
        let mut decoder = PgOutputDecoder::default();
        // Fill the cache so the next announcements force evictions.
        for id in 0..RELATION_CACHE_CAP as u32 {
            decoder.decode(&relation_message(id)).expect("relation decodes");
        }

        // One transaction truncates two tables: both relations are announced
        // (evicting only pre-transaction entries) and then referenced by one
        // Truncate message, which must resolve both.
        let a = RELATION_CACHE_CAP as u32;
        let b = RELATION_CACHE_CAP as u32 + 1;
        decoder.decode(&relation_message(a)).expect("relation decodes");
        decoder.decode(&relation_message(b)).expect("relation decodes");

        let mut truncate = Vec::new();
        truncate.push(b'T');
        truncate.extend_from_slice(&2u32.to_be_bytes());
        truncate.push(0); // truncate options
        truncate.extend_from_slice(&a.to_be_bytes());
        truncate.extend_from_slice(&b.to_be_bytes());

        assert!(matches!(
            decoder.decode(&truncate),
            Ok(PgOutputMessage::Truncate(relations)) if relations.len() == 2
        ));
    }
}
