//! Converts normalized LightCDC events into deterministic Redis mutations.

use std::collections::HashSet;

use anyhow::{Context, anyhow};
use lightcdc_api::proto::{ChangeEvent, Operation};
use serde_json::{Map, Value};

use crate::config::{CacheAction, CacheRule};

/// One retry-safe Redis key mutation performed before LightCDC is acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CacheMutation {
    Delete {
        key: String,
    },
    Set {
        key: String,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    },
}

/// Builds all configured cache mutations for one ordered event.
pub(crate) fn map_event(
    rules: &[CacheRule],
    event: &ChangeEvent,
) -> anyhow::Result<Vec<CacheMutation>> {
    let qualified_table = format!("{}.{}", event.schema, event.table);
    let matching = rules
        .iter()
        .filter(|rule| rule.table == qualified_table)
        .collect::<Vec<_>>();
    if matching.is_empty() {
        return Ok(Vec::new());
    }

    let operation = Operation::try_from(event.operation).unwrap_or(Operation::Unspecified);
    if matches!(operation, Operation::Truncate | Operation::Unspecified) {
        return Err(anyhow!(
            "Redis connector cannot safely map {operation:?} for {qualified_table}; event {} was not acknowledged",
            event.sequence
        ));
    }

    let key = parse_object(event.key.as_deref(), "key", event.sequence)?;
    let before = parse_object(event.before.as_deref(), "before", event.sequence)?;
    let after = parse_object(event.after.as_deref(), "after", event.sequence)?;
    let mut output = Vec::new();
    for rule in matching {
        match (rule.action, operation) {
            (CacheAction::Invalidate, _) | (CacheAction::Upsert, Operation::Delete) => {
                let keys = render_distinct_keys(rule, [&key, &before, &after])?;
                if keys.is_empty() {
                    return Err(anyhow!(
                        "event {} has no payload capable of rendering Redis key {:?}",
                        event.sequence,
                        rule.key
                    ));
                }
                output.extend(keys.into_iter().map(|key| CacheMutation::Delete { key }));
            }
            (CacheAction::Upsert, Operation::Insert | Operation::Update) => {
                let after_row = after.as_ref().ok_or_else(|| {
                    anyhow!(
                        "event {} needs an after row for Redis upsert rule {:?}",
                        event.sequence,
                        rule.key
                    )
                })?;
                if after_row.values().any(contains_unchanged_toast) {
                    return Err(anyhow!(
                        "event {} contains an unchanged TOAST placeholder and cannot safely update a complete Redis row; use invalidation for this table",
                        event.sequence
                    ));
                }
                let target_key = render_key(&rule.key, after_row)?;
                for old_key in render_distinct_keys(rule, [&key, &before])? {
                    if old_key != target_key {
                        output.push(CacheMutation::Delete { key: old_key });
                    }
                }
                output.push(CacheMutation::Set {
                    key: target_key,
                    value: event.after.clone().expect("parsed after payload exists"),
                    ttl_seconds: rule.ttl_seconds,
                });
            }
            (CacheAction::Upsert, _) => unreachable!("unsupported operations returned above"),
        }
    }
    Ok(output)
}

fn contains_unchanged_toast(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.get("__unchanged_toast") == Some(&Value::Bool(true))
                || object.values().any(contains_unchanged_toast)
        }
        Value::Array(values) => values.iter().any(contains_unchanged_toast),
        _ => false,
    }
}

fn parse_object(
    payload: Option<&[u8]>,
    name: &str,
    sequence: u64,
) -> anyhow::Result<Option<Map<String, Value>>> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    let value: Value = serde_json::from_slice(payload)
        .with_context(|| format!("event {sequence} {name} payload is not valid JSON"))?;
    match value {
        Value::Object(object) => Ok(Some(object)),
        _ => Err(anyhow!(
            "event {sequence} {name} payload must be a JSON object"
        )),
    }
}

fn render_distinct_keys<'a>(
    rule: &CacheRule,
    rows: impl IntoIterator<Item = &'a Option<Map<String, Value>>>,
) -> anyhow::Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for row in rows.into_iter().flatten() {
        if let Ok(key) = render_key(&rule.key, row)
            && seen.insert(key.clone())
        {
            output.push(key);
        }
    }
    Ok(output)
}

fn render_key(template: &str, row: &Map<String, Value>) -> anyhow::Result<String> {
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        rendered.push_str(&rest[..start]);
        let placeholder = &rest[start + 1..];
        let end = placeholder.find('}').ok_or_else(|| {
            anyhow!("Redis key template {template:?} has an unclosed placeholder")
        })?;
        let field = &placeholder[..end];
        if field.is_empty() {
            return Err(anyhow!(
                "Redis key template {template:?} has an empty placeholder"
            ));
        }
        let value = row.get(field).ok_or_else(|| {
            anyhow!("Redis key template {template:?} needs missing field {field:?}")
        })?;
        match value {
            Value::String(value) => rendered.push_str(value),
            Value::Number(value) => rendered.push_str(&value.to_string()),
            Value::Bool(value) => rendered.push_str(if *value { "true" } else { "false" }),
            Value::Null | Value::Array(_) | Value::Object(_) => {
                return Err(anyhow!(
                    "Redis key field {field:?} must be a non-null scalar"
                ));
            }
        }
        rest = &placeholder[end + 1..];
    }
    if rest.contains('}') {
        return Err(anyhow!(
            "Redis key template {template:?} has an unmatched closing brace"
        ));
    }
    rendered.push_str(rest);
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(action: CacheAction) -> CacheRule {
        CacheRule {
            table: "public.orders".to_owned(),
            key: "tenant:{tenant_id}:order:{id}".to_owned(),
            action,
            ttl_seconds: (action == CacheAction::Upsert).then_some(60),
        }
    }

    fn event(
        operation: Operation,
        key: Option<&str>,
        before: Option<&str>,
        after: Option<&str>,
    ) -> ChangeEvent {
        ChangeEvent {
            sequence: 42,
            event_id: "event".to_owned(),
            source: None,
            transaction: None,
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            operation: operation as i32,
            key: key.map(str::as_bytes).map(ToOwned::to_owned),
            before: before.map(str::as_bytes).map(ToOwned::to_owned),
            after: after.map(str::as_bytes).map(ToOwned::to_owned),
            commit_timestamp_ms: None,
        }
    }

    #[test]
    fn invalidation_deletes_every_distinct_old_and_new_key() {
        let event = event(
            Operation::Update,
            Some(r#"{"tenant_id":"a","id":"1"}"#),
            Some(r#"{"tenant_id":"a","id":"1"}"#),
            Some(r#"{"tenant_id":"a","id":"2"}"#),
        );

        assert_eq!(
            map_event(&[rule(CacheAction::Invalidate)], &event).expect("map event"),
            [
                CacheMutation::Delete {
                    key: "tenant:a:order:1".to_owned()
                },
                CacheMutation::Delete {
                    key: "tenant:a:order:2".to_owned()
                }
            ]
        );
    }

    #[test]
    fn upsert_removes_an_old_key_then_sets_the_after_row() {
        let event = event(
            Operation::Update,
            Some(r#"{"tenant_id":"a","id":"1"}"#),
            None,
            Some(r#"{"tenant_id":"a","id":"2","status":"paid"}"#),
        );

        let mutations = map_event(&[rule(CacheAction::Upsert)], &event).expect("map event");
        assert_eq!(mutations.len(), 2);
        assert_eq!(
            mutations[0],
            CacheMutation::Delete {
                key: "tenant:a:order:1".to_owned()
            }
        );
        assert!(matches!(
            &mutations[1],
            CacheMutation::Set { key, ttl_seconds: Some(60), .. }
                if key == "tenant:a:order:2"
        ));
    }

    #[test]
    fn truncate_is_terminal_and_remains_unacknowledged() {
        let event = event(Operation::Truncate, None, None, None);
        assert!(
            map_event(&[rule(CacheAction::Invalidate)], &event)
                .expect_err("truncate must stop")
                .to_string()
                .contains("was not acknowledged")
        );
    }

    #[test]
    fn upsert_rejects_incomplete_toast_rows() {
        let event = event(
            Operation::Update,
            Some(r#"{"tenant_id":"a","id":"1"}"#),
            None,
            Some(r#"{"tenant_id":"a","id":"1","body":{"__unchanged_toast":true}}"#),
        );

        assert!(
            map_event(&[rule(CacheAction::Upsert)], &event)
                .expect_err("incomplete row must stop")
                .to_string()
                .contains("unchanged TOAST")
        );
    }
}
