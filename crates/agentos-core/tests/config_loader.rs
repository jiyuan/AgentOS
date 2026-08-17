//! A4 closure: the deprecated `runtime::load_workspace_config` compatibility
//! wrapper and the canonical `WorkspaceConfig::load` must produce identical
//! results until the wrapper is deleted. These tests are the equivalence
//! gate for that deletion (docs/PLAN.md finding A4).
//!
//! Roadmap X3 adds the other half: a value outside its range must fail *at
//! load*, naming the key. A limit that only fails hours into a run — at the
//! first oversized tool result, or on the turn that would have compacted — is
//! a limit an operator finds out about from a broken conversation.

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

/// Every bound is checked when the file is read, not when the value is first
/// used, and the error names the key so an operator can fix it without
/// reading the source.
#[test]
fn an_out_of_range_value_fails_loud_at_load() {
    let cases: &[(&str, &str, &str)] = &[
        (
            "inline-cap",
            "[limits]\ntool_result_inline_bytes = 16\n",
            "tool_result_inline_bytes",
        ),
        (
            "tool-deadline",
            "[limits]\ntool_timeout_ms = 5\n",
            "tool_timeout_ms",
        ),
        (
            "listing",
            "[limits]\ndirectory_list_entries = 0\n",
            "directory_list_entries",
        ),
        (
            "read-above-ceiling",
            "[limits]\nfile_read_bytes = 1048576\nfile_read_max_bytes = 65536\n",
            "file_read_max_bytes",
        ),
        (
            "output-cap",
            "[limits]\ntool_output_bytes = 16\n",
            "tool_output_bytes",
        ),
        (
            "pressure",
            "[compaction]\npressure_percent = 0\n",
            "pressure_percent",
        ),
        (
            "tail",
            "[compaction]\nretain_tail_turns = 1\n",
            "retain_tail_turns",
        ),
        (
            "summarizer-tier",
            "[compaction]\nmodel = \"cheapest\"\n",
            "model",
        ),
        (
            "spill-root",
            "[spill]\nroot = \"../../elsewhere\"\n",
            "spill.root",
        ),
        ("shards", "[gateway]\nshards = 640\n", "gateway.shards"),
        ("inbox", "[gateway]\ninbox_capacity = 0\n", "inbox_capacity"),
        (
            "approval-expiry",
            "[approval]\nexpiry_seconds = 5\n",
            "expiry_seconds",
        ),
        ("jobs", "[jobs]\nmax_concurrent = 0\n", "max_concurrent"),
        (
            "job-output",
            "[jobs]\noutput_limit_bytes = 16\n",
            "output_limit_bytes",
        ),
    ];

    for (label, body, expected_key) in cases {
        let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("config-range-{label}"));
        std::fs::create_dir_all(&dir).expect("fixture dir creates");
        let path = dir.join("agent.toml");
        std::fs::write(&path, body).expect("fixture config writes");

        let error = WorkspaceConfig::load(&path).expect_err(&format!(
            "{label}: an out-of-range value must fail the load"
        ));
        let message = error.to_string();
        assert!(
            message.contains(expected_key),
            "{label}: the load error must name '{expected_key}', got: {message}"
        );
    }
}

/// A typo'd key fails rather than being ignored. A limit that silently did
/// nothing would be worse than a load failure naming it.
#[test]
fn an_unknown_key_in_a_bounded_section_fails_the_load() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("config-unknown-key");
    std::fs::create_dir_all(&dir).expect("fixture dir creates");
    let path = dir.join("agent.toml");
    std::fs::write(&path, "[limits]\ntool_result_inline_byte = 2048\n")
        .expect("fixture config writes");

    let error = WorkspaceConfig::load(&path).expect_err("a typo must fail the load");
    assert!(error.to_string().contains("tool_result_inline_byte"));
}
