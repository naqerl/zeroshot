use std::io::Write;
use std::path::Path;

use crate::native_v2_cli::{CliOutcome, NativeV2CliError};
use crate::native_v2_eval::{EvalDocument, EvalError, EvalStore};

use super::write_json;

pub(super) fn list(root: &Path, output: &mut impl Write) -> Result<CliOutcome, NativeV2CliError> {
    let summaries = EvalStore::new(root).list().map_err(cli_error)?;
    write_json(output, &summaries)?;
    Ok(CliOutcome::Completed)
}

pub(super) fn show(
    root: &Path,
    slug: &str,
    output: &mut impl Write,
) -> Result<CliOutcome, NativeV2CliError> {
    let document = EvalStore::new(root).load(slug).map_err(cli_error)?;
    write_json(output, &document)?;
    Ok(CliOutcome::Completed)
}

pub(super) fn create(
    root: &Path,
    file: &Path,
    overwrite: bool,
    output: &mut impl Write,
) -> Result<CliOutcome, NativeV2CliError> {
    let bytes = std::fs::read(file).map_err(|source| NativeV2CliError::Read {
        kind: "eval",
        path: file.to_path_buf(),
        source,
    })?;
    let document = serde_json::from_slice::<EvalDocument>(&bytes).map_err(|source| {
        NativeV2CliError::Json {
            kind: "eval",
            path: file.to_path_buf(),
            source,
        }
    })?;
    EvalStore::new(root)
        .create(&document, overwrite)
        .map_err(cli_error)?;
    write_json(
        output,
        &serde_json::json!({
            "slug": document.slug,
            "overwritten": overwrite,
        }),
    )?;
    Ok(CliOutcome::Completed)
}

fn cli_error(error: EvalError) -> NativeV2CliError {
    match &error {
        EvalError::Io { .. } | EvalError::Encode(_) => NativeV2CliError::Local(error.to_string()),
        EvalError::Invalid(_)
        | EvalError::InvalidSlug { .. }
        | EvalError::SlugMismatch { .. }
        | EvalError::DuplicateSlug { .. }
        | EvalError::AlreadyExists { .. }
        | EvalError::NotFound { .. } => NativeV2CliError::Usage(error.to_string()),
    }
}
