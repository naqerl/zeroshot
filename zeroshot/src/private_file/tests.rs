use super::*;

#[test]
fn replacement_is_private_complete_and_leaves_no_temporary_file() {
    let root = tempfile::tempdir().unwrap();
    let folder = root.path().join("nested").join("folder");
    create_directory(&folder).unwrap();
    let path = folder.join("state.json");
    replace(&path, b"a long first document").unwrap();
    replace(&path, b"short").unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"short");
    assert_eq!(std::fs::read_dir(&folder).unwrap().count(), 1);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(&folder), 0o700);
    }
}

#[cfg(unix)]
#[test]
fn a_linked_destination_is_replaced_without_touching_its_target() {
    let root = tempfile::tempdir().unwrap();
    let victim = root.path().join("victim");
    std::fs::write(&victim, b"untouched").unwrap();
    for hard_link in [false, true] {
        let folder = root.path().join(format!("folder-{hard_link}"));
        create_directory(&folder).unwrap();
        let destination = folder.join("state.json");
        if hard_link {
            std::fs::hard_link(&victim, &destination).unwrap();
        } else {
            std::os::unix::fs::symlink(&victim, &destination).unwrap();
        }
        replace(&destination, b"new document").unwrap();
        assert_eq!(std::fs::read(&victim).unwrap(), b"untouched");
        assert_eq!(std::fs::read(&destination).unwrap(), b"new document");
        assert!(
            !std::fs::symlink_metadata(&destination)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }
}

#[test]
fn a_failed_replacement_cleans_up_and_reports_the_error() {
    let root = tempfile::tempdir().unwrap();
    // The destination is a non-empty folder, so the final rename fails after the write.
    let destination = root.path().join("occupied");
    std::fs::create_dir(&destination).unwrap();
    std::fs::write(destination.join("keep"), b"x").unwrap();
    assert!(replace(&destination, b"document").is_err());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    // A missing parent fails before anything is created.
    assert!(replace(&root.path().join("missing").join("file"), b"document").is_err());
    // A path without a file name cannot have a sibling.
    assert!(replace(Path::new("/"), b"document").is_err());
}
