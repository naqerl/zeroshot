//! The Pi lane's contained capsule configuration.

use crate::execution::process::HostedProcessPool;
use crate::native_v2_candidate::NativeV2HarnessConfig;
use crate::native_v2_capsule::CapsuleFilesystem;
use crate::native_v2_cloud::CapsuleAllocationUnavailable;
use crate::native_v2_contract::PiProvider;
use crate::native_v2_hosting::environment;
use crate::native_v2_pi::{PiConfig, PiProcessEnvironment};

use super::ProductionCapsuleConfig;

/// Builds the Pi harness configuration for one contained capsule.
///
/// The lane's own process environment supplies the base, and the run's tools directory is the one
/// value the capsule adds on top, so the arm only has to hand over the filesystem it was given. The
/// run root is the workspace's parent, exactly as the other lanes resolve it.
pub(super) fn harness_config(
    config: &ProductionCapsuleConfig,
    provider: PiProvider,
    filesystem: &CapsuleFilesystem,
    process_pool: HostedProcessPool,
) -> Result<NativeV2HarnessConfig, CapsuleAllocationUnavailable> {
    let run_root = filesystem
        .workspace
        .parent()
        .ok_or(CapsuleAllocationUnavailable::Runtime)?;
    let search_path = environment::search_path(run_root, &config.executable_search_path);
    let base_environment = config
        .pi_process_environment
        .for_capsule(&filesystem.runtime_home, &search_path)
        .map_err(|_| CapsuleAllocationUnavailable::Runtime)?;
    let mut values = base_environment.clone_values();
    values.insert(
        "ZEROSHOT_TOOLS".to_owned(),
        environment::tools_directory(run_root)
            .to_string_lossy()
            .into_owned(),
    );
    Ok(NativeV2HarnessConfig::Pi(PiConfig {
        provider,
        executable: config.pi_executable.clone(),
        prefix_arguments: config.pi_prefix_arguments.clone(),
        workspace: filesystem.workspace.clone(),
        runtime_home: filesystem.runtime_home.clone(),
        local_user_home: None,
        native_environment: Default::default(),
        base_environment: PiProcessEnvironment::new(values)
            .map_err(|_| CapsuleAllocationUnavailable::Runtime)?,
        process_pool,
    }))
}
