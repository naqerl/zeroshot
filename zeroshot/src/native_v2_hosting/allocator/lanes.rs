//! A hosted capsule builds one harness configuration per distinct effective lane.
use std::collections::BTreeMap;
use std::path::Path;

use crate::execution::process::{HostedProcessIdentity, HostedProcessPool, HostedProcessScope};
use crate::native_v2_candidate::NativeV2HarnessConfig;
use crate::native_v2_capsule::{CapsuleFilesystem, HarnessProcessEnvironment};
use crate::native_v2_claude::ClaudeAdapterConfig;
use crate::native_v2_cloud::CapsuleAllocationUnavailable;
use crate::native_v2_codex::NativeV2CodexConfig;
use crate::native_v2_contract::{AdmittedRun, RuntimeLane};
use crate::native_v2_hosting::environment;
use crate::native_v2_pi::PiConfig;
use super::ProductionCapsuleAllocator;

#[derive(Clone, Copy)]
struct HostedLaneInputs<'a> {
    filesystem: &'a CapsuleFilesystem,
    search_path: &'a str,
    base_environment: &'a BTreeMap<String, String>,
    process_pool: HostedProcessPool,
}

impl HostedLaneInputs<'_> {
    fn process_environment(
        self,
        base: &HarnessProcessEnvironment,
    ) -> Result<HarnessProcessEnvironment, CapsuleAllocationUnavailable> {
        let environment = base
            .for_capsule(&self.filesystem.runtime_home, self.search_path)
            .map_err(|_| CapsuleAllocationUnavailable::Runtime)?;
        let mut values = environment.clone_values();
        values.extend(self.base_environment.clone());
        HarnessProcessEnvironment::new(values).map_err(|_| CapsuleAllocationUnavailable::Runtime)
    }
}

impl ProductionCapsuleAllocator {
    pub(super) fn lanes(
        &self,
        admitted: &AdmittedRun,
        filesystem: &CapsuleFilesystem,
        process_pool: HostedProcessPool,
    ) -> Result<Vec<NativeV2HarnessConfig>, CapsuleAllocationUnavailable> {
        let run_root = filesystem
            .workspace
            .parent()
            .ok_or(CapsuleAllocationUnavailable::Runtime)?;
        let search_path = environment::search_path(run_root, &self.config.executable_search_path);
        let tools_directory = environment::tools_directory(run_root)
            .to_string_lossy()
            .into_owned();
        // Keep every hosted build's caches and fallback temp files on the capsule storage
        // root, not the target's root disk. The writer's private home already exists for the
        // provider session; Cargo creates its own cache and target subdirectories under it.
        let writer = process_pool
            .identity(HostedProcessScope::Writer)
            .map_err(|_| CapsuleAllocationUnavailable::Runtime)?;
        let build_home = HostedProcessScope::Writer.private_home(&filesystem.runtime_home);
        prepare_build_home(&build_home, writer)?;
        let base_environment = BTreeMap::from([
            ("ZEROSHOT_TOOLS".to_owned(), tools_directory),
            (
                "CARGO_HOME".to_owned(),
                build_home.join(".cargo").to_string_lossy().into_owned(),
            ),
            (
                "CARGO_TARGET_DIR".to_owned(),
                build_home
                    .join(".cargo-target")
                    .to_string_lossy()
                    .into_owned(),
            ),
            (
                "TMPDIR".to_owned(),
                build_home.to_string_lossy().into_owned(),
            ),
        ]);
        let inputs = HostedLaneInputs {
            filesystem,
            search_path: &search_path,
            base_environment: &base_environment,
            process_pool,
        };
        admitted
            .runtime
            .lanes()
            .into_iter()
            .map(|lane| self.lane_harness(lane, inputs))
            .collect()
    }

    fn lane_harness(
        &self,
        lane: RuntimeLane,
        inputs: HostedLaneInputs<'_>,
    ) -> Result<NativeV2HarnessConfig, CapsuleAllocationUnavailable> {
        let HostedLaneInputs {
            filesystem,
            search_path,
            base_environment,
            process_pool,
        } = inputs;
        match lane {
            RuntimeLane::Copilot { .. } => Ok(NativeV2HarnessConfig::Copilot(
                crate::native_v2_copilot::CopilotConfig {
                    executable: self.config.copilot_executable.clone(),
                    workspace: filesystem.workspace.clone(),
                    runtime_home: filesystem.runtime_home.clone(),
                    local_user: None,
                    base_environment: base_environment.clone(),
                    local_command_environment: std::collections::BTreeMap::new(),
                    search_path: search_path.to_owned(),
                    process_pool,
                },
            )),
            RuntimeLane::Codex { provider } => {
                Ok(NativeV2HarnessConfig::Codex(NativeV2CodexConfig {
                    provider,
                    executable: self.config.codex_executable.clone(),
                    workspace: filesystem.workspace.clone(),
                    runtime_home: filesystem.runtime_home.clone(),
                    local_user: None,
                    native_environment: Default::default(),
                    base_environment: base_environment.clone(),
                    search_path: search_path.to_owned(),
                    process_pool,
                }))
            }
            RuntimeLane::Claude { provider } => {
                let base_environment =
                    inputs.process_environment(&self.config.claude_process_environment)?;
                Ok(NativeV2HarnessConfig::Claude(ClaudeAdapterConfig {
                    provider,
                    executable: self.config.claude_executable.clone(),
                    prefix_arguments: self.config.claude_prefix_arguments.clone(),
                    workspace: filesystem.workspace.clone(),
                    runtime_home: filesystem.runtime_home.clone(),
                    local_user_home: None,
                    native_environment: Default::default(),
                    base_environment,
                    process_pool,
                }))
            }
            RuntimeLane::Pi { provider } => {
                let base_environment =
                    inputs.process_environment(&self.config.pi_process_environment)?;
                Ok(NativeV2HarnessConfig::Pi(PiConfig {
                    provider,
                    executable: self.config.pi_executable.clone(),
                    prefix_arguments: self.config.pi_prefix_arguments.clone(),
                    workspace: filesystem.workspace.clone(),
                    runtime_home: filesystem.runtime_home.clone(),
                    local_user_home: None,
                    native_environment: Default::default(),
                    base_environment,
                    process_pool,
                }))
            }
        }
    }
}

/// Create the writer-owned build home under the capsule runtime root. Ownership is best effort
/// so an unprivileged test composition can still inspect the lane; production runs as root.
fn prepare_build_home(
    path: &Path,
    identity: HostedProcessIdentity,
) -> Result<(), CapsuleAllocationUnavailable> {
    std::fs::create_dir_all(path).map_err(|_| CapsuleAllocationUnavailable::Runtime)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| CapsuleAllocationUnavailable::Runtime)?;
        let _ = std::os::unix::fs::chown(path, Some(identity.uid()), Some(identity.gid()));
    }
    #[cfg(not(unix))]
    let _ = identity;
    Ok(())
}
