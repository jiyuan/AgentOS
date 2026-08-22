//! M0 deliverable 3: `docs/CAPABILITY_MATRIX.md` describes the code, or the
//! build fails.
//!
//! The matrix exists so a stable capability cannot be confused with a preview
//! one. A matrix nobody checks decays into exactly the overstated
//! documentation the audit found — so this is the two-way ratchet
//! `crates/agentos-core/src/config/undocumented.txt` established, applied to
//! capabilities:
//!
//!   - a tool or provider the code exposes with **no row** fails, so a new
//!     capability cannot ship undocumented;
//!   - a `tool:`/`provider:` row naming something that **no longer exists**
//!     also fails, so the matrix cannot go stale.
//!
//! Only the machine-enumerable capabilities are ratcheted. Rows for channels,
//! backends, and loop features are prose the compiler cannot check, so they are
//! held to the structural rules below instead: a known status, and — for
//! anything not Stable — a stated limitation.

use agentos_core::runtime::{BUILTIN_TOOL_NAMES, RUNTIME_TOOL_NAMES};
use agentos_llm::SUPPORTED_PROVIDERS;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn matrix_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("docs/CAPABILITY_MATRIX.md")
}

fn matrix() -> String {
    let path = matrix_path();
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("{} is missing: {err}", path.display()))
}

/// One capability row: the first cell, the status cell, and the limitations
/// cell. Header and alignment rows are skipped, and so is the two-column
/// status legend at the top, which is documentation rather than a capability.
struct Row {
    capability: String,
    status: String,
    required: String,
    limitations: String,
}

fn rows(body: &str) -> Vec<Row> {
    body.lines()
        .filter(|line| line.starts_with('|'))
        .filter_map(|line| {
            let cells: Vec<&str> = line.trim().trim_matches('|').split('|').collect();
            if cells.len() != 6 {
                return None;
            }
            let capability = cells[0].trim();
            if capability == "Capability" || capability.starts_with("---") {
                return None;
            }
            Some(Row {
                capability: capability.to_owned(),
                status: cells[1].trim().replace("**", ""),
                required: cells[3].trim().to_owned(),
                limitations: cells[4].trim().to_owned(),
            })
        })
        .collect()
}

/// The names a `tool:x` / `provider:x` row claims, by prefix.
///
/// Reads the first backticked span rather than the whole cell, so a row may
/// carry trailing prose — `` `provider:openai` (Responses API) `` — without
/// the name picking it up.
fn declared(prefix: &str) -> BTreeSet<String> {
    rows(&matrix())
        .into_iter()
        .filter_map(|row| {
            let name = row.capability.split('`').nth(1)?;
            name.strip_prefix(prefix).map(str::to_owned)
        })
        .collect()
}

#[test]
fn every_built_in_tool_has_a_row() {
    let declared = declared("tool:");
    let known: BTreeSet<String> = BUILTIN_TOOL_NAMES
        .iter()
        .chain(RUNTIME_TOOL_NAMES)
        .map(|name| (*name).to_owned())
        .collect();

    let missing: Vec<&String> = known.difference(&declared).collect();
    assert!(
        missing.is_empty(),
        "these tools exist but have no row in docs/CAPABILITY_MATRIX.md: {missing:?}. \
         Add one — status, platforms, required config, limitations, test level."
    );

    let stale: Vec<&String> = declared.difference(&known).collect();
    assert!(
        stale.is_empty(),
        "docs/CAPABILITY_MATRIX.md has rows for tools that no longer exist: {stale:?}. \
         Delete them; a matrix that keeps a removed capability is worse than none."
    );
}

#[test]
fn every_llm_provider_has_a_row() {
    let declared = declared("provider:");
    let known: BTreeSet<String> = SUPPORTED_PROVIDERS
        .iter()
        .map(|name| (*name).to_owned())
        .collect();

    assert_eq!(
        declared, known,
        "docs/CAPABILITY_MATRIX.md must have exactly one row per provider in \
         agentos_llm::SUPPORTED_PROVIDERS"
    );
}

#[test]
fn every_row_carries_a_status_the_release_policy_defines() {
    let body = matrix();
    let unknown: Vec<(String, String)> = rows(&body)
        .into_iter()
        .filter(|row| !matches!(row.status.as_str(), "Stable" | "Preview" | "Deferred"))
        .map(|row| (row.capability, row.status))
        .collect();
    assert!(
        unknown.is_empty(),
        "every capability needs a status of Stable, Preview, or Deferred \
         (§6 of the remediation plan); these do not: {unknown:?}"
    );
}

/// The rule the matrix exists to enforce. A Preview or Deferred row with an
/// empty Limitations cell is indistinguishable from a Stable one at a glance,
/// which is the confusion the whole document is meant to prevent.
#[test]
fn a_capability_short_of_stable_states_its_limitation() {
    let body = matrix();
    let silent: Vec<String> = rows(&body)
        .into_iter()
        .filter(|row| row.status != "Stable")
        .filter(|row| row.limitations.is_empty() || row.limitations == "—")
        .map(|row| row.capability)
        .collect();
    assert!(
        silent.is_empty(),
        "these are not Stable but name no limitation: {silent:?}. \
         Say what is missing and which milestone closes it."
    );
}

/// Guards the parser rather than the content: if the table shape changes and
/// `rows` silently matches nothing, every assertion above passes vacuously.
#[test]
fn the_matrix_parses_into_rows() {
    let parsed = rows(&matrix());
    assert!(
        parsed.len() > 50,
        "parsed only {} capability rows from docs/CAPABILITY_MATRIX.md; \
         the table shape probably changed and the other tests are now vacuous",
        parsed.len()
    );
}

/// A `Preview` capability must not be on by default (M6 / `STATE-001`,
/// deliverable 7).
///
/// A row that says "opt-in" is a claim about the shipped defaults, and the
/// matrix carried exactly that claim about streaming while both entrypoints
/// installed a `StreamSink` unconditionally. The check is against
/// `WorkspaceConfig::default()`, which is what a deployment gets before it
/// writes any `agent.toml`, so the row and the code cannot drift apart again.
#[test]
fn no_preview_capability_is_enabled_by_default() {
    let defaults = agentos_core::config::WorkspaceConfig::default();
    assert!(
        !defaults.channels.provisional_streaming,
        "provisional streaming is a Preview capability and must be opt-in: \
         output guardrails run after the chunks have been forwarded"
    );

    // The row has to keep saying so, because the assertion above is only
    // meaningful to someone who read it here.
    let streaming = rows(&matrix())
        .into_iter()
        .find(|row| row.capability == "Provisional streaming")
        .expect("the matrix has a streaming row");
    assert_eq!(streaming.status, "Preview");
    assert!(
        streaming.required.contains("provisional_streaming"),
        "the row must name the key that turns it on, got: {}",
        streaming.required
    );
    assert!(
        streaming.limitations.contains("Off by default"),
        "the row must say so where a reader is looking for the exposure, got: {}",
        streaming.limitations
    );
}
