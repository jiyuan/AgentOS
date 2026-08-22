//! `[policy]` and `[guardrails]`: who may act, and what content is allowed
//! through.
//!
//! Split out of `config/mod.rs` to keep it under the module ceiling. The two
//! sit together because they are the pair `AGENTS.md` insists is not the same
//! thing: `[policy]` decides *permission* and `[guardrails]` inspects
//! *content*, and a deployment needs both.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct PolicyConfig {
    /// What happens to an action no rule covers: `allow`, `ask_user`, or
    /// `deny`. `deny` is the shipped value and the only one that fails closed.
    pub default: Arc<str>,
    /// Tools whose per-operation gating is replaced by a blanket `allow`.
    ///
    /// A blunt instrument, and deliberately so — naming a tool here says "stop
    /// asking me about this one". It cannot name `memory`: `[memory.policy]`
    /// decides memory, and a config that says both fails to load rather than
    /// letting one silently win (M7 / `MEM-001`).
    pub allowlist: Vec<Arc<str>>,
    /// Senders who may answer any approval prompt, not only their own.
    ///
    /// Empty by default: a prompt is answerable by the person it was put to.
    /// In a group conversation that is what stops one participant deciding
    /// another's approval. Name a sender id here only when someone genuinely
    /// needs to unblock other people's prompts.
    pub approval_administrators: Vec<Arc<str>>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            default: Arc::from("deny"),
            allowlist: Vec::new(),
            approval_administrators: Vec::new(),
        }
    }
}

/// Programs the shell tool guardrail accepts when `agent.toml` declares no
/// `[guardrails]` section. Deliberately limited to read-only inspection
/// commands; widen it explicitly through `[guardrails] shell_allowlist`.
pub const DEFAULT_SHELL_ALLOWLIST: [&str; 8] =
    ["printf", "echo", "pwd", "ls", "find", "cat", "head", "tail"];

/// A program in the shell allowlist whose *arguments* the guardrail also
/// checks.
///
/// The allowlist alone matches on the program name, which is enough for a
/// command that can only inspect. It is not enough for a command that can be
/// argued into running something else: `python3 -c "<payload>"` and
/// `find . -exec sh -c "<payload>" \;` both clear a program-name check while
/// being arbitrary code execution. A profile names the shape of call that is
/// actually intended, and refuses the rest.
///
/// The two constraints are deliberately different mechanisms, because the two
/// escapes are:
///
/// - `require_first_arg_suffix` is an allowlist, and it is the right tool for
///   an interpreter. Once `python3`'s first argument is a script path, every
///   later argument belongs to the script rather than to the interpreter, so
///   pinning that one position closes `-c`, `-m`, `-i`, a bare `-`, and the
///   short-flag clusters (`-Bc`) that an exact-match denylist would miss.
/// - `deny_args` is a denylist, for a command whose flags do not cluster and
///   where only a handful of them are dangerous. `find` is the shipped case:
///   its actions are whole tokens, so naming them is exact.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ShellProfileConfig {
    /// Program this profile governs, as a bare name matching the call's
    /// `command` field. A profile also admits its program, so a program named
    /// here need not repeat itself in `shell_allowlist`; when it appears in
    /// both, the profile still applies.
    pub program: Arc<str>,
    /// When non-empty, the first entry of the structured args array must end
    /// with one of these suffixes. Pins an interpreter to a script file.
    #[serde(default)]
    pub require_first_arg_suffix: Vec<Arc<str>>,
    /// Arguments refused outright, compared literally against each entry of
    /// the structured args array.
    #[serde(default)]
    pub deny_args: Vec<Arc<str>>,
}

/// Argument profiles applied when `agent.toml` declares no `[guardrails]`
/// section. `find` is in the default allowlist and its action predicates
/// (`-exec`, `-delete`, …) run other programs and remove files, so the
/// default allowlist would otherwise ship a code-execution primitive.
pub fn default_shell_profiles() -> Vec<ShellProfileConfig> {
    vec![ShellProfileConfig {
        program: Arc::from("find"),
        require_first_arg_suffix: Vec::new(),
        deny_args: [
            "-exec", "-execdir", "-ok", "-okdir", "-delete", "-fprint", "-fprint0", "-fprintf",
            "-fls",
        ]
        .iter()
        .copied()
        .map(Arc::from)
        .collect(),
    }]
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct GuardrailsConfig {
    /// Programs the shell tool guardrail permits in a call's `command` field.
    /// Each entry is a bare program name — arguments belong in the structured
    /// args array, not here. Defaults to `DEFAULT_SHELL_ALLOWLIST`.
    pub shell_allowlist: Vec<Arc<str>>,
    /// Programs whose structured args array is checked too, not only the
    /// program name. Required for anything that can be argued into running
    /// other code. Defaults to `default_shell_profiles`.
    pub shell_profiles: Vec<ShellProfileConfig>,
}

impl Default for GuardrailsConfig {
    fn default() -> Self {
        Self {
            shell_allowlist: DEFAULT_SHELL_ALLOWLIST
                .iter()
                .copied()
                .map(Arc::from)
                .collect(),
            shell_profiles: default_shell_profiles(),
        }
    }
}
