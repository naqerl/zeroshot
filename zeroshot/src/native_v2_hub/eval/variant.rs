//! Variant overrides: how a variant rewrites a captured case's run request. Pure, so the merge
//! rules are testable without a hub.
//!
//! - `graph=FILE` / `template=NAME` replace the topology (and each other).
//! - `runtime-config=FILE` / `uniform-runtime-config=FILE` replace the runtime file (and each
//!   other); `profile=NAME` replaces the profile.
//! - `environment=FILE` replaces the environment (and drops `--no-environment`).
//! - `model=ID`, `harness=ID`, `provider=ID` set that field at the top level of the runtime
//!   file and on every node that already names one; `runtime-patch=FILE` applies a JSON merge
//!   patch (RFC 7386) to the runtime file. These need a runtime file in the case or variant.
//!
//! Overrides apply in order, so `runtime-config=` followed by `model=` patches the new file.

use std::collections::BTreeMap;

use serde_json::Value;

use super::super::error::HubError;

/// Every key `--variant` accepts, with whether its value is a file the client uploads.
pub(crate) const KEYS: [(&str, bool); 10] = [
    ("graph", true),
    ("template", false),
    ("profile", false),
    ("runtime-config", true),
    ("uniform-runtime-config", true),
    ("environment", true),
    ("runtime-patch", true),
    ("model", false),
    ("harness", false),
    ("provider", false),
];

pub(crate) fn is_file_key(key: &str) -> bool {
    KEYS.iter().any(|(name, file)| *name == key && *file)
}

/// One file of a run request: its original name and content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Material {
    pub name: String,
    pub content: Vec<u8>,
}

/// A run request being rewritten. File flags in `run_args` carry a placeholder value; the
/// content lives in `files`, keyed by flag.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Request {
    pub run_args: Vec<String>,
    pub files: BTreeMap<String, Material>,
}

/// A resolved override: the key, its text value, and the content of a file key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Change {
    pub key: String,
    pub value: String,
    pub file: Option<Material>,
}

pub(crate) const RUNTIME_FLAGS: [&str; 2] = ["--runtime-config", "--uniform-runtime-config"];

impl Request {
    fn position(&self, flag: &str) -> Option<usize> {
        self.run_args.iter().position(|argument| argument == flag)
    }

    /// Remove `flag` and its value (or just `flag` when `valued` is false).
    fn remove(&mut self, flag: &str, valued: bool) {
        if let Some(position) = self.position(flag) {
            let end = if valued {
                (position + 2).min(self.run_args.len())
            } else {
                position + 1
            };
            self.run_args.drain(position..end);
        }
        self.files.remove(flag);
    }

    fn set_value(&mut self, flag: &str, value: &str) {
        match self.position(flag) {
            Some(position) if position + 1 < self.run_args.len() => {
                value.clone_into(&mut self.run_args[position + 1]);
            }
            _ => self.run_args.extend([flag.to_owned(), value.to_owned()]),
        }
    }

    fn set_file(&mut self, flag: &str, file: Material) {
        self.set_value(flag, &format!("file:{flag}"));
        self.files.insert(flag.to_owned(), file);
    }

    /// The runtime file flag in use, if any.
    pub(crate) fn runtime_flag(&self) -> Option<&'static str> {
        RUNTIME_FLAGS
            .into_iter()
            .find(|flag| self.files.contains_key(*flag))
    }

    /// Apply overrides in order.
    pub(crate) fn apply(mut self, changes: &[Change]) -> Result<Self, HubError> {
        for change in changes {
            self.apply_one(change)?;
        }
        Ok(self)
    }

    fn apply_one(&mut self, change: &Change) -> Result<(), HubError> {
        let file = || {
            change.file.clone().ok_or_else(|| {
                HubError::Usage(format!("variant key {} needs file content", change.key))
            })
        };
        match change.key.as_str() {
            "graph" => {
                self.remove("--template", true);
                self.set_file("--graph", file()?);
            }
            "template" => {
                self.remove("--graph", true);
                self.set_value("--template", &change.value);
            }
            "profile" => self.set_value("--profile", &change.value),
            "runtime-config" => {
                self.remove("--uniform-runtime-config", true);
                self.set_file("--runtime-config", file()?);
            }
            "uniform-runtime-config" => {
                self.remove("--runtime-config", true);
                self.set_file("--uniform-runtime-config", file()?);
            }
            "environment" => {
                self.remove("--no-environment", false);
                self.set_file("--environment", file()?);
            }
            "runtime-patch" => {
                let patch = json(&file()?, "runtime-patch")?;
                self.patch_runtime(&change.key, |document| merge_patch(document, &patch))?;
            }
            field @ ("model" | "harness" | "provider") => {
                let value = Value::String(change.value.clone());
                self.patch_runtime(field, |document| set_everywhere(document, field, &value))?;
            }
            other => {
                return Err(HubError::Usage(format!(
                    "unknown variant key {other:?}; use one of {}",
                    KEYS.map(|(key, _)| key).join(", ")
                )));
            }
        }
        Ok(())
    }

    fn patch_runtime(&mut self, key: &str, patch: impl FnOnce(&mut Value)) -> Result<(), HubError> {
        let Some(flag) = self.runtime_flag() else {
            return Err(HubError::Usage(format!(
                "variant key {key} patches the runtime file, but the case has no \
                 --runtime-config or --uniform-runtime-config; give the variant a \
                 runtime-config=FILE first"
            )));
        };
        let material = self
            .files
            .get_mut(flag)
            .ok_or_else(|| HubError::Usage(format!("{flag} has no captured content")))?;
        let mut document = json(material, flag)?;
        patch(&mut document);
        material.content = serde_json::to_vec_pretty(&document).map_err(|error| {
            HubError::Usage(format!("the patched {flag} could not be encoded: {error}"))
        })?;
        Ok(())
    }

    /// Model identifiers the runtime file names after all overrides.
    pub(crate) fn models(&self) -> Vec<String> {
        let mut models = self
            .runtime_flag()
            .and_then(|flag| self.files.get(flag))
            .and_then(|material| serde_json::from_slice::<Value>(&material.content).ok())
            .map(|document| {
                let mut models = Vec::new();
                if let Some(model) = document.get("model").and_then(Value::as_str) {
                    models.push(model.to_owned());
                }
                if let Some(nodes) = document.get("nodes").and_then(Value::as_object) {
                    models.extend(
                        nodes
                            .values()
                            .filter_map(|node| node.get("model").and_then(Value::as_str))
                            .map(str::to_owned),
                    );
                }
                models
            })
            .unwrap_or_default();
        models.sort();
        models.dedup();
        models
    }

    /// True when the runtime file binds any node to an explicit lane.
    pub(crate) fn lanes(&self) -> bool {
        self.runtime_flag()
            .and_then(|flag| self.files.get(flag))
            .and_then(|material| serde_json::from_slice::<Value>(&material.content).ok())
            .and_then(|document| document.get("nodes").and_then(Value::as_object).cloned())
            .is_some_and(|nodes| {
                nodes
                    .values()
                    .any(|node| node.get("lane").is_some_and(|lane| !lane.is_null()))
            })
    }
}

fn json(material: &Material, what: &str) -> Result<Value, HubError> {
    serde_json::from_slice(&material.content)
        .map_err(|error| HubError::Usage(format!("{what} {} is not JSON: {error}", material.name)))
}

/// Set `field` at the top level and on every node that already names it.
pub(crate) fn set_everywhere(document: &mut Value, field: &str, value: &Value) {
    let Some(object) = document.as_object_mut() else {
        return;
    };
    object.insert(field.to_owned(), value.clone());
    if let Some(nodes) = object.get_mut("nodes").and_then(Value::as_object_mut) {
        for node in nodes.values_mut().filter_map(Value::as_object_mut) {
            if node.contains_key(field) {
                node.insert(field.to_owned(), value.clone());
            }
        }
    }
}

/// JSON merge patch (RFC 7386): objects merge, `null` deletes, anything else replaces.
pub(crate) fn merge_patch(target: &mut Value, patch: &Value) {
    let Value::Object(patch) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(serde_json::Map::new());
    }
    if let Value::Object(object) = target {
        for (key, value) in patch {
            if value.is_null() {
                object.remove(key);
            } else {
                merge_patch(object.entry(key.clone()).or_insert(Value::Null), value);
            }
        }
    }
}
