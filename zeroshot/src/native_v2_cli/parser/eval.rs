use std::path::PathBuf;

#[derive(Debug, clap::Subcommand)]
pub(super) enum EvalCommand {
    /// List stored eval summaries in slug order as JSON.
    List,
    /// Show one eval document as JSON.
    Show(EvalSlugArgs),
    /// Store a self-contained eval JSON document.
    Create(EvalCreateArgs),
    /// Remove a stored eval and the run bundles published for it.
    Clean(EvalSlugArgs),
    /// Run one stored eval's samples on its target and judge the delivered patches.
    Run(EvalRunArgs),
}

#[derive(Debug, clap::Args)]
pub(super) struct EvalSlugArgs {
    /// Eval slug, matching its file name without the .json suffix.
    #[arg(value_name = "SLUG")]
    pub(super) slug: String,
}

#[derive(Debug, clap::Args)]
pub(super) struct EvalCreateArgs {
    /// Self-contained eval JSON document.
    #[arg(value_name = "FILE")]
    pub(super) file: PathBuf,
    /// Replace an existing eval instead of refusing the write.
    #[arg(long)]
    pub(super) overwrite: bool,
}

#[derive(Debug, clap::Args)]
pub(super) struct EvalRunArgs {
    /// Stored eval to run.
    #[arg(value_name = "SLUG")]
    pub(super) slug: String,
    /// Named target that receives the sample runs; overrides the eval's own target.
    #[arg(long, value_name = "NAME")]
    pub(super) target: Option<String>,
}
