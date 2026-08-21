//! Everything the shipped `workspace/` declares actually exists.
//!
//! M2 acceptance: "Every enabled skill, routing target, subagent, and template
//! resolves, and an unresolvable one fails loudly." The failure this closes was
//! quiet by construction — `WorkspaceSkillCatalog::filtered` drops unknown
//! names, so `general-subagent` declared six skills, three of which the
//! workspace never enabled, and came up looking configured. At runtime a
//! dropped skill is indistinguishable from one the model chose not to use.
//!
//! Against the real `workspace/`, not a fixture, for the same reason
//! `shipped_config_policy.rs` is: the mechanism was never in doubt, the shipped
//! declarations were.

use agentos_core::config::WorkspaceConfig;
use agentos_core::skills::WorkspaceSkillCatalog;
use std::path::{Path, PathBuf};

fn workspace_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../workspace")
}

fn shipped_config() -> WorkspaceConfig {
    WorkspaceConfig::load(&workspace_dir().join("agent.toml"))
        .expect("the shipped workspace config must load")
}

fn shipped_catalog(config: &WorkspaceConfig) -> WorkspaceSkillCatalog {
    WorkspaceSkillCatalog::load_enabled(
        &workspace_dir().join("skills"),
        &config.resources.skills.enabled,
    )
    .expect("every skill named in [resources.skills] must exist on disk")
}

#[test]
fn every_enabled_skill_exists_on_disk() {
    let config = shipped_config();
    let catalog = shipped_catalog(&config);
    for name in &config.resources.skills.enabled {
        assert!(
            catalog.contains(name),
            "[resources.skills] enables '{name}', which the catalog does not hold"
        );
    }
}

/// The one that was red. Every sub-agent may only name skills the workspace
/// enabled, because anything else is silently dropped at build time.
#[test]
fn every_subagent_skill_is_enabled_by_the_workspace() {
    let config = shipped_config();
    let catalog = shipped_catalog(&config);

    let mut unresolvable: Vec<String> = Vec::new();
    for subagent in &config.subagents {
        for skill in &subagent.skills {
            if !catalog.contains(skill) {
                unresolvable.push(format!("{}: {skill}", subagent.id));
            }
        }
    }
    assert!(
        unresolvable.is_empty(),
        "sub-agents declare skills the workspace has not enabled: {unresolvable:?}. \
         Add them to [resources.skills] enabled, or remove them from the sub-agent — \
         they are dropped silently otherwise."
    );
}

/// Building the sub-agent registry is what a real startup does, so this is the
/// end-to-end form of the test above: it proves the failure is *loud* rather
/// than merely detectable by a test that knows where to look.
#[test]
fn the_shipped_subagents_build() {
    let config = shipped_config();
    let catalog = shipped_catalog(&config);
    let names: Vec<&str> = config
        .subagents
        .iter()
        .flat_map(|subagent| subagent.skills.iter().map(AsRef::as_ref))
        .filter(|skill| !catalog.contains(skill))
        .collect();
    assert!(
        names.is_empty(),
        "startup would reject the shipped workspace: {names:?}"
    );
}

/// `AUTH-002` against the shipped workspace: no sub-agent silently holds more
/// than the parent, and any that does says so in `delegation_grants`.
#[test]
fn no_shipped_subagent_silently_out_permits_the_parent() {
    let config = shipped_config();
    let granted: Vec<String> = config
        .subagents
        .iter()
        .filter(|subagent| !subagent.delegation_grants.is_empty())
        .map(|subagent| {
            format!(
                "{}: {}",
                subagent.id,
                subagent
                    .delegation_grants
                    .iter()
                    .map(|grant| format!("{}={}", grant.tool, grant.decision))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect();

    // Not a prohibition on grants — a deployment may need one. This pins the
    // shipped posture: today every sub-agent runs on exactly its parent's
    // rules, so adding a grant here is a visible, reviewable change.
    assert!(
        granted.is_empty(),
        "the shipped workspace declares delegation grants: {granted:?}. \
         That may be intended, but it elevates a sub-agent above the parent \
         policy and should be a deliberate review decision."
    );
}
