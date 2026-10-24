use std::path::{Path, PathBuf};

use super::super::paths::{absolute, absolute_from};

fn invoking() -> std::io::Result<PathBuf> {
    Ok(PathBuf::from(if cfg!(windows) {
        r"C:\invoking"
    } else {
        "/invoking"
    }))
}

fn home() -> PathBuf {
    PathBuf::from(if cfg!(windows) {
        r"C:\Users\captured"
    } else {
        "/captured/home"
    })
}

#[test]
fn home_shorthand_and_relative_paths_resolve_when_captured() {
    let home = home();
    let resolve = |path: &str| absolute_from(Path::new(path), Some(&home), invoking()).expect(path);
    assert_eq!(resolve("~"), home);
    assert_eq!(resolve("~/hub/state"), home.join("hub/state"));
    assert_eq!(resolve("hub"), invoking().unwrap().join("hub"));
    // Another user's home is not this shell's to expand, so it stays a relative name.
    assert_eq!(
        resolve("~other/hub"),
        invoking().unwrap().join("~other/hub")
    );
    let absolute_path = invoking().unwrap().join("already");
    assert_eq!(
        absolute_from(&absolute_path, None, Err(std::io::Error::other("gone"))).expect("absolute"),
        absolute_path
    );
    let backslash = resolve(r"~\hub");
    if cfg!(windows) {
        assert_eq!(backslash, home.join("hub"));
    } else {
        assert_eq!(backslash, invoking().unwrap().join(r"~\hub"));
    }
}

#[test]
fn unresolvable_paths_are_usage_errors() {
    let missing_home = absolute_from(Path::new("~/hub"), None, invoking()).expect_err("no home");
    assert!(
        missing_home
            .to_string()
            .contains("home folder is unavailable"),
        "{missing_home}"
    );
    let missing_folder = absolute_from(
        Path::new("hub"),
        Some(&home()),
        Err(std::io::Error::other("gone")),
    )
    .expect_err("no invoking folder");
    assert!(
        missing_folder
            .to_string()
            .contains("invoking folder is unavailable"),
        "{missing_folder}"
    );
    assert!(absolute(Path::new("relative")).expect("cwd").is_absolute());
}

/// Service installs read `HOME` and the user, which Windows does not provide.
#[cfg(unix)]
#[test]
fn installs_pin_absolute_folders_from_relative_and_home_relative_inputs() {
    use super::super::install::InstallEnv;
    let home = super::super::paths::home_folder().expect("HOME");
    let current = std::env::current_dir().expect("cwd");
    let env = InstallEnv::detect(
        Path::new("relative-config/targets.json"),
        Path::new("~/hub-dir"),
    )
    .expect("detect");
    assert_eq!(env.config_dir, current.join("relative-config"));
    assert_eq!(env.hub_dir, home.join("hub-dir"));
    assert!(env.state_dir.is_absolute());
    assert!(env.user_unit_dir.is_absolute());
    let pinned = env
        .pinned_environment("systemd")
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    for name in [
        "ZEROSHOT_CONFIG_DIR",
        "ZEROSHOT_HUB_DIR",
        "ZEROSHOT_STATE_DIR",
    ] {
        assert!(
            Path::new(&pinned[name]).is_absolute(),
            "{name}: {}",
            pinned[name]
        );
    }
}
