//! Forge selection for Git delivery.
//!
//! A run delivers to exactly one Git forge. github.com keeps the pinned GitHub CLI authority; any
//! other http(s) origin selects the Gitea/Forgejo REST authority. Selection is derived from the
//! workspace origin so the existing GitHub path is unchanged for GitHub repositories.

use url::Url;

/// The Git forge that owns the delivered repository.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryForge {
    /// github.com, delivered through the pinned GitHub CLI.
    GitHub,
    /// A Gitea or Forgejo instance reached over its REST API.
    Gitea(GiteaForge),
}

/// A Gitea or Forgejo instance origin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GiteaForge {
    /// Normalized instance origin such as `https://gitea.example.com`.
    pub base_url: String,
}

/// A delivery forge origin is not a supported http(s) Git forge.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DeliveryForgeError {
    #[error("delivery forge origin must be an http(s) URL without embedded credentials")]
    Origin,
}

const GITHUB_HOST: &str = "github.com";
const MAX_BASE_URL_BYTES: usize = 2_048;

impl DeliveryForge {
    #[must_use]
    pub const fn github() -> Self {
        Self::GitHub
    }

    /// Validates and normalizes a Gitea or Forgejo instance origin.
    pub fn gitea(base_url: impl Into<String>) -> Result<Self, DeliveryForgeError> {
        let base_url = base_url.into();
        if base_url.len() > MAX_BASE_URL_BYTES {
            return Err(DeliveryForgeError::Origin);
        }
        let url = Url::parse(&base_url).map_err(|_| DeliveryForgeError::Origin)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none_or(|host| host.is_empty())
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(DeliveryForgeError::Origin);
        }
        Ok(Self::Gitea(GiteaForge {
            base_url: url.origin().ascii_serialization(),
        }))
    }

    /// Selects the forge that owns a Git remote origin.
    ///
    /// github.com selects the GitHub authority; every other http(s) host selects the Gitea
    /// authority. scp-like origins default to the https instance origin.
    pub fn from_origin(origin: &str) -> Result<Self, DeliveryForgeError> {
        let (scheme, host, port, _) = split_origin(origin).ok_or(DeliveryForgeError::Origin)?;
        if host.eq_ignore_ascii_case(GITHUB_HOST) {
            return Ok(Self::GitHub);
        }
        // An SSH port is not the instance's HTTP(S) port, so it is never reused for the REST
        // endpoint. Only an explicit http(s) origin carries its port forward.
        let port = matches!(scheme.as_str(), "http" | "https")
            .then_some(port)
            .flatten();
        Self::gitea(instance_origin(&scheme, &host, port))
    }

    /// The environment variable that carries the delivery credential for this forge.
    #[must_use]
    pub const fn credential_environment(&self) -> &'static str {
        match self {
            Self::GitHub => super::GITHUB_TOKEN_ENV,
            Self::Gitea(_) => super::GITEA_TOKEN_ENV,
        }
    }

    /// The forge-neutral `owner/name` identity for a Git remote origin.
    pub fn repository(origin: &str) -> Option<String> {
        let (_, _, _, path) = split_origin(origin)?;
        repository_path(&path)
    }

    /// The authenticated HTTPS remote URL for a repository on this forge.
    #[must_use]
    pub fn remote_url(&self, repository: &str) -> String {
        match self {
            Self::GitHub => format!("https://github.com/{repository}.git"),
            Self::Gitea(forges) => format!("{}/{repository}.git", forges.base_url),
        }
    }
}

fn split_origin(origin: &str) -> Option<(String, String, Option<u16>, String)> {
    match Url::parse(origin) {
        Ok(url) => {
            let scheme = url.scheme().to_owned();
            if !matches!(scheme.as_str(), "http" | "https" | "ssh")
                || url.host_str().is_none_or(|host| host.is_empty())
            {
                return None;
            }
            if matches!(scheme.as_str(), "http" | "https")
                && (!url.username().is_empty() || url.password().is_some())
            {
                return None;
            }
            Some((
                scheme,
                url.host_str()?.to_owned(),
                url.port(),
                url.path().to_owned(),
            ))
        }
        Err(_) => {
            if origin.contains("://") {
                return None;
            }
            let (authority, path) = origin.split_once(':')?;
            let host = authority
                .rsplit_once('@')
                .map_or(authority, |(_, host)| host);
            if host.is_empty() || path.is_empty() || authority.starts_with('/') {
                return None;
            }
            Some(("ssh".to_owned(), host.to_owned(), None, path.to_owned()))
        }
    }
}

fn instance_origin(scheme: &str, host: &str, port: Option<u16>) -> String {
    let scheme = if scheme == "http" { "http" } else { "https" };
    match port {
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    }
}

fn repository_path(path: &str) -> Option<String> {
    let path = path.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    let mut segments = path.split('/');
    let owner = segments.next()?;
    let repository = segments.next()?;
    if owner.is_empty() || repository.is_empty() || segments.next().is_some() {
        return None;
    }
    Some(format!("{owner}/{repository}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn github_com_origins_select_the_github_forge() {
        for origin in [
            "https://github.com/acme/project.git",
            "https://github.com/acme/project",
            "http://github.com/acme/project.git",
            "git@github.com:acme/project.git",
            "ssh://git@github.com/acme/project.git",
        ] {
            assert_eq!(
                DeliveryForge::from_origin(origin).expect("github origin"),
                DeliveryForge::GitHub,
                "{origin}"
            );
        }
    }

    #[test]
    fn other_hosts_select_the_gitea_forge_with_a_normalized_origin() {
        let forge = DeliveryForge::from_origin("https://gitea.example.com/acme/project.git")
            .expect("gitea origin");
        assert_eq!(
            forge,
            DeliveryForge::gitea("https://gitea.example.com").expect("gitea base")
        );
        assert_eq!(forge.credential_environment(), "GITEA_TOKEN");

        let forge =
            DeliveryForge::from_origin("git@gitea.example.com:acme/project.git").expect("ssh");
        assert_eq!(
            forge,
            DeliveryForge::gitea("https://gitea.example.com").expect("gitea base")
        );

        let forge =
            DeliveryForge::from_origin("http://git.local:8080/acme/project.git").expect("http");
        assert_eq!(
            forge,
            DeliveryForge::gitea("http://git.local:8080").expect("gitea base")
        );

        // A nonstandard SSH port never becomes the instance's HTTPS port.
        let forge = DeliveryForge::from_origin("ssh://git@gitea.example.com:2222/acme/project.git")
            .expect("ssh");
        assert_eq!(
            forge,
            DeliveryForge::gitea("https://gitea.example.com").expect("gitea base")
        );
    }

    #[test]
    fn remote_urls_name_the_owning_forge() {
        assert_eq!(
            DeliveryForge::GitHub.remote_url("acme/project"),
            "https://github.com/acme/project.git"
        );
        assert_eq!(
            DeliveryForge::gitea("https://gitea.example.com")
                .expect("gitea")
                .remote_url("acme/project"),
            "https://gitea.example.com/acme/project.git"
        );
    }

    #[test]
    fn repository_identity_requires_exactly_owner_and_name() {
        assert_eq!(
            DeliveryForge::repository("https://github.com/acme/project.git").as_deref(),
            Some("acme/project")
        );
        assert_eq!(
            DeliveryForge::repository("git@gitea.example.com:acme/project").as_deref(),
            Some("acme/project")
        );
        assert!(DeliveryForge::repository("https://gitea.example.com/acme").is_none());
        assert!(DeliveryForge::repository("https://gitea.example.com/too/many/segments").is_none());
        assert!(DeliveryForge::repository("not-a-remote").is_none());
    }

    #[test]
    fn unsupported_origins_are_rejected() {
        for origin in [
            "",
            "not-a-url",
            "ftp://gitea.example.com/acme/project.git",
            "https://user:password@gitea.example.com/acme/project.git",
        ] {
            assert!(
                DeliveryForge::from_origin(origin).is_err(),
                "{origin} should not select a forge"
            );
        }
    }
}
