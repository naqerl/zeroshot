//! Hub contexts: named hub endpoints, like docker contexts or kubectl contexts.
//!
//! `hub-contexts.json` in the config folder stores each context's URL and where its bearer token
//! is read from (a file or an environment variable), never the token. The built-in `local`
//! context addresses this machine's daemon: the address and token file its heartbeat records,
//! or the defaults.

use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use zeroshot_engine::native_v2_cli::hub::{HubContextCommand, HubTokenSource};

use super::daemon::{DEFAULT_LISTEN, TOKEN_FILE};
use super::error::HubError;
use super::paths::absolute;
use super::store::HubStore;
use super::write_json;

pub(crate) const LOCAL_CONTEXT: &str = "local";
/// Selects a context for one invocation, below `--hub` and above `hub context use`.
pub(crate) const CONTEXT_ENV: &str = "ZEROSHOT_HUB";
const CONTEXT_FILE: &str = "hub-contexts.json";

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ContextFile {
    #[serde(default)]
    pub current: Option<String>,
    #[serde(default)]
    pub contexts: Vec<ContextRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ContextRecord {
    pub name: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
}

/// Where a resolved context's token comes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TokenRef {
    File(PathBuf),
    Env(String),
}

impl TokenRef {
    fn view(&self) -> Value {
        match self {
            Self::File(path) => json!({ "tokenFile": path }),
            Self::Env(variable) => json!({ "tokenEnv": variable }),
        }
    }

    /// Read the token. Errors name the source, never its content.
    pub(crate) fn read(&self, context: &str, url: &str) -> Result<String, HubError> {
        let token = match self {
            Self::File(path) => match std::fs::read_to_string(path) {
                Ok(token) => token,
                // The local daemon creates its token on first start.
                Err(error)
                    if context == LOCAL_CONTEXT && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    return Err(HubError::Unreachable {
                        context: context.to_owned(),
                        url: url.to_owned(),
                        cause: format!(
                            "no hub daemon has started on this machine ({} does not exist)",
                            path.display()
                        ),
                    });
                }
                Err(error) => {
                    return Err(HubError::Usage(format!(
                        "hub context {context}: token file {} could not be read: {error}",
                        path.display()
                    )));
                }
            },
            Self::Env(variable) => std::env::var(variable).map_err(|_| {
                HubError::Usage(format!(
                    "hub context {context}: environment variable {variable} is not set"
                ))
            })?,
        };
        let token = token.trim().to_owned();
        if token.is_empty() {
            return Err(HubError::Usage(format!(
                "hub context {context}: the bearer token is empty"
            )));
        }
        Ok(token)
    }
}

/// A context chosen for one command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Resolved {
    pub name: String,
    pub url: String,
    pub token: TokenRef,
    /// `flag`, `env`, `current`, or `default`.
    pub source: &'static str,
}

/// Context precedence: `--hub`, then `ZEROSHOT_HUB`, then the current context, then `local`.
pub(crate) fn select(
    flag: Option<&str>,
    environment: Option<&str>,
    file: &ContextFile,
) -> (String, &'static str) {
    let nonempty = |value: Option<&str>| value.filter(|value| !value.is_empty()).map(str::to_owned);
    if let Some(name) = nonempty(flag) {
        return (name, "flag");
    }
    if let Some(name) = nonempty(environment) {
        return (name, "env");
    }
    if let Some(name) = nonempty(file.current.as_deref()) {
        return (name, "current");
    }
    (LOCAL_CONTEXT.to_owned(), "default")
}

/// Context file plus this machine's hub folder, for the `local` context.
pub(crate) struct Contexts {
    path: PathBuf,
    hub: HubStore,
}

impl Contexts {
    pub(crate) fn new(config_dir: &Path, hub: HubStore) -> Self {
        Self {
            path: config_dir.join(CONTEXT_FILE),
            hub,
        }
    }

    pub(crate) fn load(&self) -> Result<ContextFile, HubError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| {
                HubError::storage(&format!("{} is malformed", self.path.display()), error)
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(ContextFile::default())
            }
            Err(error) => Err(HubError::storage("hub contexts could not be read", error)),
        }
    }

    fn save(&self, file: &ContextFile) -> Result<(), HubError> {
        if let Some(parent) = self.path.parent() {
            super::service::create_private_directory(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(file)
            .map_err(|error| HubError::storage("hub contexts could not be encoded", error))?;
        super::service::replace_private(&self.path, &bytes)
    }

    /// The `local` context: the running daemon's recorded address and token file, or the
    /// defaults when no daemon ever ran here.
    pub(crate) fn local(&self) -> (String, TokenRef) {
        let heartbeat = self.hub.heartbeat().unwrap_or(Value::Null);
        let url = heartbeat
            .get("url")
            .and_then(Value::as_str)
            .map_or_else(|| format!("http://{DEFAULT_LISTEN}"), str::to_owned);
        let token = heartbeat
            .get("tokenFile")
            .and_then(Value::as_str)
            .map_or_else(|| self.hub.directory().join(TOKEN_FILE), PathBuf::from);
        (url, TokenRef::File(token))
    }

    fn record(&self, name: &str, file: &ContextFile) -> Result<(String, TokenRef), HubError> {
        if name == LOCAL_CONTEXT {
            return Ok(self.local());
        }
        let record = file
            .contexts
            .iter()
            .find(|record| record.name == name)
            .ok_or_else(|| {
                HubError::Usage(format!(
                    "hub context {name} does not exist; add it with `zeroshot hub context add \
                     {name} URL --token-file FILE`"
                ))
            })?;
        let token = match (&record.token_file, &record.token_env) {
            (Some(path), _) => TokenRef::File(path.clone()),
            (None, Some(variable)) => TokenRef::Env(variable.clone()),
            (None, None) => {
                return Err(HubError::Usage(format!(
                    "hub context {name} has no token source"
                )));
            }
        };
        Ok((record.url.clone(), token))
    }

    /// Resolve the context for a command from `--hub`, `ZEROSHOT_HUB`, and the context file.
    pub(crate) fn resolve(&self, flag: Option<&str>) -> Result<Resolved, HubError> {
        let file = self.load()?;
        let environment = std::env::var(CONTEXT_ENV).ok();
        let (name, source) = select(flag, environment.as_deref(), &file);
        let (url, token) = self.record(&name, &file)?;
        Ok(Resolved {
            name,
            url,
            token,
            source,
        })
    }

    pub(crate) fn execute(
        &self,
        flag: Option<&str>,
        command: HubContextCommand,
        output: &mut impl Write,
    ) -> Result<(), HubError> {
        let mut file = self.load()?;
        match command {
            HubContextCommand::Add { name, url, token } => {
                if name == LOCAL_CONTEXT {
                    return Err(HubError::Usage(
                        "`local` is built in and always addresses this machine's daemon".to_owned(),
                    ));
                }
                if file.contexts.iter().any(|record| record.name == name) {
                    return Err(HubError::Conflict(format!(
                        "hub context {name} already exists; remove it first"
                    )));
                }
                let url = normalize_url(&url)?;
                let (token_file, token_env) = match token {
                    HubTokenSource::File(path) => (Some(absolute(&path)?), None),
                    HubTokenSource::Env(variable) => (None, Some(variable)),
                };
                file.contexts.push(ContextRecord {
                    name: name.clone(),
                    url,
                    token_file,
                    token_env,
                });
                self.save(&file)?;
                let (url, token) = self.record(&name, &file)?;
                write_json(output, &context_document(&name, &url, &token, false))
            }
            HubContextCommand::List => {
                let environment = std::env::var(CONTEXT_ENV).ok();
                let (selected, source) = select(flag, environment.as_deref(), &file);
                let current = file
                    .current
                    .clone()
                    .unwrap_or_else(|| LOCAL_CONTEXT.to_owned());
                let mut names = vec![LOCAL_CONTEXT.to_owned()];
                names.extend(file.contexts.iter().map(|record| record.name.clone()));
                let contexts = names
                    .iter()
                    .filter_map(|name| {
                        let (url, token) = self.record(name, &file).ok()?;
                        let mut view =
                            json!({ "name": name, "url": url, "current": *name == current });
                        if let Some(token) = token.view().as_object() {
                            for (key, value) in token {
                                view[key] = value.clone();
                            }
                        }
                        view["builtIn"] = json!(name == LOCAL_CONTEXT);
                        Some(view)
                    })
                    .collect::<Vec<_>>();
                write_json(
                    output,
                    &json!({
                        "schema": "zeroshot.hub.contexts/v1",
                        "current": current,
                        "selected": selected,
                        "selectedBy": source,
                        "contexts": contexts,
                    }),
                )
            }
            HubContextCommand::Use { name } => {
                let (url, token) = self.record(&name, &file)?;
                file.current = (name != LOCAL_CONTEXT).then(|| name.clone());
                self.save(&file)?;
                write_json(output, &context_document(&name, &url, &token, true))
            }
            HubContextCommand::Remove { name } => {
                if name == LOCAL_CONTEXT {
                    return Err(HubError::Usage(
                        "the built-in `local` context cannot be removed".to_owned(),
                    ));
                }
                let before = file.contexts.len();
                file.contexts.retain(|record| record.name != name);
                if file.contexts.len() == before {
                    return Err(HubError::Usage(format!(
                        "hub context {name} does not exist"
                    )));
                }
                if file.current.as_deref() == Some(name.as_str()) {
                    file.current = None;
                }
                self.save(&file)?;
                write_json(
                    output,
                    &json!({ "schema": "zeroshot.hub.context/v1", "removed": name }),
                )
            }
        }
    }
}

fn context_document(name: &str, url: &str, token: &TokenRef, current: bool) -> Value {
    let mut context = json!({ "name": name, "url": url, "current": current });
    if let Some(token) = token.view().as_object() {
        for (key, value) in token {
            context[key] = value.clone();
        }
    }
    json!({ "schema": "zeroshot.hub.context/v1", "context": context })
}

/// Validate an http(s) base URL without credentials and drop a trailing slash.
pub(crate) fn normalize_url(text: &str) -> Result<String, HubError> {
    let url = url::Url::parse(text)
        .map_err(|error| HubError::Usage(format!("invalid hub URL {text:?}: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(HubError::Usage(format!(
            "invalid hub URL {text:?}: use http://HOST:PORT or https://HOST"
        )));
    }
    if !url.username().is_empty() || url.password().is_some() || url.query().is_some() {
        return Err(HubError::Usage(format!(
            "invalid hub URL {text:?}: credentials and queries belong in --token-file or \
             --token-env"
        )));
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}
