//! Resolution of folder and file paths the hub stores or pins into services.
//!
//! A stored context or an installed unit runs later and elsewhere, so every path is resolved
//! when it is captured: a leading `~` against the captured home folder, then a relative path
//! against the invoking folder.

use std::path::{Path, PathBuf};

use super::error::HubError;

/// `path` with home shorthand expanded and made absolute against the current folder.
pub(crate) fn absolute(path: &Path) -> Result<PathBuf, HubError> {
    absolute_from(path, home_folder().as_deref(), std::env::current_dir())
}

/// `path` with home shorthand expanded against `home` and made absolute against `invoking`.
pub(crate) fn absolute_from(
    path: &Path,
    home: Option<&Path>,
    invoking: std::io::Result<PathBuf>,
) -> Result<PathBuf, HubError> {
    let path = expand_home(path, home)?;
    if path.is_absolute() {
        return Ok(path);
    }
    invoking
        .map(|folder| folder.join(path))
        .map_err(|error| HubError::Usage(format!("the invoking folder is unavailable: {error}")))
}

/// Expand `~` and `~/REST` (also `~\REST` on Windows). `~user` forms are left alone.
fn expand_home(path: &Path, home: Option<&Path>) -> Result<PathBuf, HubError> {
    let Some(text) = path.to_str() else {
        return Ok(path.to_path_buf());
    };
    let rest = if text == "~" {
        Some("")
    } else {
        text.strip_prefix("~/")
            .or_else(|| text.strip_prefix("~\\").filter(|_| cfg!(windows)))
    };
    let Some(rest) = rest else {
        return Ok(path.to_path_buf());
    };
    let home = home.ok_or_else(|| {
        HubError::Usage(format!(
            "{text} starts with ~ but the home folder is unavailable"
        ))
    })?;
    Ok(if rest.is_empty() {
        home.to_path_buf()
    } else {
        home.join(rest)
    })
}

/// `HOME`, or `USERPROFILE` on Windows.
pub(crate) fn home_folder() -> Option<PathBuf> {
    let variable = |name| std::env::var_os(name).filter(|value| !value.is_empty());
    variable("HOME")
        .or_else(|| {
            if cfg!(windows) {
                variable("USERPROFILE")
            } else {
                None
            }
        })
        .map(PathBuf::from)
}
