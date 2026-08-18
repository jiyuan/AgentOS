//! Roadmap X4: the checked-in catalogs describe the code, or the build fails.
//!
//! `scripts/check-catalogs.sh` is what CI runs, but a contributor who never
//! runs it should still be told by `cargo test` — a doc that drifts silently is
//! the failure this item exists to close, and a check nobody runs locally
//! drifts until CI catches it a commit later.

use agentos_core::config::catalog;
use agentos_core::config::WorkspaceConfig;
use agentos_core::runtime::{register_builtin_tool, BUILTIN_TOOL_NAMES};
use agentos_core::tools::ToolRegistry;
use std::path::{Path, PathBuf};

fn repo_file(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative)
}

const REGENERATE: &str =
    "run `cargo run -p agentos-cli --bin agentos-gateway -- catalog` and commit the result";

#[test]
fn the_config_catalog_matches_the_config_structs() {
    let body = match catalog::config_markdown() {
        Ok(body) => body,
        Err(problems) => {
            let listed: Vec<String> = problems.iter().map(ToString::to_string).collect();
            panic!(
                "the config catalog cannot be rendered:\n  {}",
                listed.join("\n  ")
            );
        }
    };
    let path = repo_file("docs/CONFIG_CATALOG.md");
    let existing = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("{} is missing: {err}; {REGENERATE}", path.display()));
    let expected = catalog::splice(&existing, catalog::BEGIN_MARKER, catalog::END_MARKER, &body)
        .expect("the catalog keeps its markers");
    assert_eq!(
        existing, expected,
        "docs/CONFIG_CATALOG.md is stale; {REGENERATE}"
    );
}

#[test]
fn the_tool_catalog_matches_the_registered_specs() {
    let limits = WorkspaceConfig::default().limits;
    let mut registry = ToolRegistry::new();
    for name in BUILTIN_TOOL_NAMES {
        register_builtin_tool(&mut registry, name, &limits).expect("a built-in registers");
    }
    let body = catalog::tool_markdown(&registry.specs());

    let path = repo_file("docs/TOOL_CATALOG.md");
    let existing = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("{} is missing: {err}; {REGENERATE}", path.display()));
    let expected = catalog::splice(
        &existing,
        catalog::TOOL_BEGIN_MARKER,
        catalog::TOOL_END_MARKER,
        &body,
    )
    .expect("the catalog keeps its markers");
    assert_eq!(
        existing, expected,
        "docs/TOOL_CATALOG.md is stale; {REGENERATE}"
    );
}

/// Every built-in tool has a row. A tool missing from the catalog reads as a
/// tool this build does not have.
#[test]
fn every_built_in_tool_has_a_row() {
    let catalog = std::fs::read_to_string(repo_file("docs/TOOL_CATALOG.md"))
        .expect("the tool catalog exists");
    for name in BUILTIN_TOOL_NAMES {
        assert!(
            catalog.contains(&format!("| `{name}` |")),
            "'{name}' has no row in the tool catalog; {REGENERATE}"
        );
    }
}
