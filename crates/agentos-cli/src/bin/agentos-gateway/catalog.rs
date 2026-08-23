//! `agentos-gateway catalog` — write the derived catalogs, or check them.
//!
//! Roadmap item X4. The rendering lives in `agentos_core::config::catalog`,
//! which is where the config structs are; this is the part that decides which
//! files it lands in and whether a stale file is an error.
//!
//! `--check` is what CI runs. It renders and compares without writing, so a
//! pull request that changes a config field and does not regenerate fails with
//! the command to run rather than with a diff nobody reads.

use agentos_core::config::catalog;
use agentos_core::config::WorkspaceConfig;
use agentos_core::jobs::JobRegistry;
use agentos_core::memory::{InMemoryMemory, MemoryManager};
use agentos_core::runtime::{register_builtin_tool, BUILTIN_TOOL_NAMES};
use agentos_core::spill::SpillStore;
use agentos_core::tools::{
    JobKillTool, JobOutputTool, JobStatusTool, MemoryTool, SpillReadTool, ToolRegistry,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Where the catalogs live, relative to the repository root.
const CONFIG_CATALOG: &str = "docs/CONFIG_CATALOG.md";
const TOOL_CATALOG: &str = "docs/TOOL_CATALOG.md";

/// Header for a catalog file that does not exist yet, so the first run creates
/// something with the markers rather than failing on their absence.
fn scaffold(title: &str, intro: &str, begin: &str, end: &str) -> String {
    format!("# {title}\n\n{intro}\n\n{begin}\n{end}\n")
}

/// Render both catalogs and write them, or check them against what is on disk.
///
/// `root` is the repository root — the catalogs are checked-in documentation,
/// so they are located relative to the tree, not to `$AGENTOS_HOME`.
pub(super) fn run(root: &Path, check: bool) -> Result<(), String> {
    let config = catalog::config_markdown().map_err(|problems| {
        let mut message = String::from(
            "the config catalog cannot be rendered because some keys have no description:\n",
        );
        for problem in problems {
            message.push_str(&format!("  {problem}\n"));
        }
        message
    })?;
    let tools = catalog::tool_markdown(&builtin_specs()?);

    let mut stale = Vec::new();
    for (relative, body, begin, end, title, intro) in [
        (
            CONFIG_CATALOG,
            config,
            catalog::BEGIN_MARKER,
            catalog::END_MARKER,
            "Config catalog",
            "Every key `agent.toml` accepts, derived from the config structs. \
             Edit the doc comment on the field, not this table.",
        ),
        (
            TOOL_CATALOG,
            tools,
            catalog::TOOL_BEGIN_MARKER,
            catalog::TOOL_END_MARKER,
            "Tool catalog",
            "Every built-in tool, derived from its `ToolSpec`. `Side effect` \
             and `Persistence scope` drive blanket-authorization checks; \
             `Sandbox` is what the kernel enforces for the tool's child \
             processes; `Deadline` is the tool's own declaration, which a \
             deployment's `[limits]` can override.",
        ),
    ] {
        let path = root.join(relative);
        let existing =
            std::fs::read_to_string(&path).unwrap_or_else(|_| scaffold(title, intro, begin, end));
        let updated = catalog::splice(&existing, begin, end, &body)
            .map_err(|err| format!("{}: {err}", path.display()))?;

        if check {
            if existing != updated {
                stale.push(relative);
            }
            continue;
        }
        if existing == updated {
            println!("unchanged {relative}");
            continue;
        }
        write_catalog(&path, &updated)?;
        println!("wrote {relative}");
    }

    if stale.is_empty() {
        if check {
            println!("catalogs are current");
        }
        return Ok(());
    }
    Err(format!(
        "these catalogs are stale: {}\nrun `cargo run -p agentos-cli --bin agentos-gateway -- \
         catalog` and commit the result",
        stale.join(", ")
    ))
}

fn write_catalog(path: &Path, body: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|err| format!("cannot create {}: {err}", parent.display()))?;
    }
    std::fs::write(path, body).map_err(|err| format!("cannot write {}: {err}", path.display()))
}

/// Every built-in tool's spec.
///
/// Built from the default config rather than the deployment's, because the
/// catalog documents what this *build* offers: a machine that has disabled a
/// tool should not publish a catalog implying nobody else has it. Runtime-owned
/// tools receive inert in-memory handles because `spec()` does not use them.
fn builtin_specs() -> Result<Vec<agentos_interfaces::tool::ToolSpec>, String> {
    let limits = WorkspaceConfig::default().limits;
    let mut registry = ToolRegistry::new();
    for name in BUILTIN_TOOL_NAMES {
        register_builtin_tool(&mut registry, name, &limits, &[])?;
    }
    let memory = Arc::new(MemoryManager::new(Arc::new(InMemoryMemory::default())));
    let jobs = Arc::new(JobRegistry::default());
    registry.register(MemoryTool::with_manager(memory));
    registry.register(JobStatusTool::new(jobs.clone()));
    registry.register(JobOutputTool::new(jobs.clone()));
    registry.register(JobKillTool::new(jobs));
    registry.register(SpillReadTool::new(Arc::new(SpillStore::new(
        PathBuf::from("catalog-spec-only"),
    ))));
    Ok(registry.specs())
}

/// The repository root, for a command run from a checkout.
///
/// `CARGO_MANIFEST_DIR` is where this crate's `Cargo.toml` sits, so the root is
/// two levels up. A binary run outside a checkout has no catalogs to write, and
/// the caller can name a root explicitly.
pub(super) fn default_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .to_path_buf()
}
