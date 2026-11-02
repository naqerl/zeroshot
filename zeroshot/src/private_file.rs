//! Owner-only files and folders for the CLI's local configuration and state.
//!
//! [`replace`] is the one atomic writer shared by the target registry and the hub: the bytes go to
//! a freshly created (`create_new`) temporary sibling with a random name and mode `0600`, are
//! synced, and are renamed over the destination. A reader never sees a truncated file, a
//! pre-planted link at the temporary name is never followed, and a link at the destination is
//! replaced rather than written through. A failed write removes its temporary file.

use std::fs::{DirBuilder, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Atomically replace `path` with `bytes`, owner-only.
pub(crate) fn replace(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temporary = temporary_sibling(path)?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    set_private_file_mode(&mut options);
    let result = options.open(&temporary).and_then(|mut file| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)
    });
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Create `path` and its missing parents, owner-only.
pub(crate) fn create_directory(path: &Path) -> io::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    set_private_directory_mode(&mut builder);
    builder.create(path)
}

fn temporary_sibling(path: &Path) -> io::Result<PathBuf> {
    let mut suffix = [0_u8; 8];
    getrandom::fill(&mut suffix).map_err(|error| io::Error::other(error.to_string()))?;
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path names no file"))?;
    let mut temporary = std::ffi::OsString::from(".");
    temporary.push(name);
    temporary.push(".tmp-");
    for byte in suffix {
        temporary.push(format!("{byte:02x}"));
    }
    Ok(path.with_file_name(temporary))
}

#[cfg(unix)]
pub(crate) fn set_private_file_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
}

#[cfg(not(unix))]
pub(crate) fn set_private_file_mode(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn set_private_directory_mode(builder: &mut DirBuilder) {
    use std::os::unix::fs::DirBuilderExt;
    builder.mode(0o700);
}

#[cfg(not(unix))]
fn set_private_directory_mode(_builder: &mut DirBuilder) {}

#[cfg(test)]
#[path = "private_file/tests.rs"]
mod tests;
