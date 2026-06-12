//! A4 closure: the deprecated `runtime::load_workspace_config` compatibility
//! wrapper and the canonical `WorkspaceConfig::load` must produce identical
//! results until the wrapper is deleted. These tests are the equivalence
//! gate for that deletion (docs/PLAN.md finding A4).

#![allow(deprecated)]

use agentos_core::config::WorkspaceConfig;
use agentos_core::runtime::load_workspace_config;
use std::path::{Path, PathBuf};

fn assert_loaders_agree(path: &Path) {
    let canonical = WorkspaceConfig::load(path).expect("canonical loader succeeds");
    let wrapped = load_workspace_config(path).expect("compatibility wrapper succeeds");
    assert_eq!(
        canonical,
        wrapped,
        "both loaders must produce the identical effective config for {}",
        path.display()
    );
}

#[test]
fn loaders_agree_on_repository_workspace_config() {
    // The real config exercises the full path: policy, channels, resources,
    // routing rules, plus subagents/*.toml and suborchs/*.toml sibling dirs.
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../workspace/agent.toml");
    assert_loaders_agree(&path);
}

#[test]
fn loaders_agree_on_minimal_fixture() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("config-loader-minimal");
    std::fs::create_dir_all(&dir).expect("fixture dir creates");
    let path = dir.join("agent.toml");
    std::fs::write(
        &path,
        r#"
[agent]
id = "fixture"
max_turns = 4

[policy]
default = "deny"
allowlist = ["file"]

[resources.tools]
enabled = ["file"]
"#,
    )
    .expect("fixture config writes");
    assert_loaders_agree(&path);
}

#[test]
fn loaders_agree_on_missing_config() {
    // A missing agent.toml resolves to the default config in both loaders.
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("config-loader-missing");
    std::fs::create_dir_all(&dir).expect("fixture dir creates");
    assert_loaders_agree(&dir.join("agent.toml"));
}
