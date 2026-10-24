use serde_json::{Map, Value};

use super::host::McpFailure;

pub(super) fn reject_unknown(map: &Map<String, Value>, allowed: &[&str]) -> Result<(), McpFailure> {
    if let Some(unknown) = map.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(McpFailure::invalid(format!("unknown argument {unknown}")));
    }
    Ok(())
}

pub(super) fn optional_string(
    map: &Map<String, Value>,
    key: &str,
) -> Result<Option<String>, McpFailure> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(McpFailure::invalid(format!("{key} must be a string"))),
    }
}

pub(super) fn object_schema(required: &[&str], properties: Vec<(&str, Value)>) -> Value {
    let mut map = Map::new();
    for (name, schema) in properties {
        map.insert(name.to_owned(), schema);
    }
    let mut schema = serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "properties": map,
    });
    if !required.is_empty() {
        schema["required"] = serde_json::json!(required);
    }
    schema
}

pub(super) fn string_prop(description: &str) -> Value {
    serde_json::json!({"type": "string", "description": description})
}

pub(super) fn bool_prop(description: &str) -> Value {
    serde_json::json!({"type": "boolean", "description": description})
}

pub(super) fn integer_prop(description: &str) -> Value {
    serde_json::json!({"type": "integer", "description": description})
}

pub(super) fn string_array_prop(description: &str) -> Value {
    serde_json::json!({
        "type": "array",
        "description": description,
        "items": {"type": "string"}
    })
}
