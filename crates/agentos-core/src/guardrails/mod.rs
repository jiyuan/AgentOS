use crate::tools::{safe_workspace_path, skills_dir, workspace_root};
use agentos_interfaces::guardrail::{
    GuardrailError, GuardrailOutcome, Input, InputGuardrail, OutputGuardrail, ToolGuardrail,
};
use agentos_interfaces::orchestrator::RunContext;
use agentos_proto::{Message, ToolCall, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct PiiFilter;

#[async_trait]
impl InputGuardrail for PiiFilter {
    async fn check(
        &self,
        input: &Input,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        if contains_email_like(&input.message.content) {
            return Ok(GuardrailOutcome::Tripped(Arc::from(
                "input appears to contain an email address",
            )));
        }
        if contains_ssn_like(&input.message.content) {
            return Ok(GuardrailOutcome::Tripped(Arc::from(
                "input appears to contain a US social security number",
            )));
        }
        Ok(GuardrailOutcome::Passed)
    }
}

pub struct MaxOutputLength {
    max_chars: usize,
}

impl MaxOutputLength {
    pub fn new(max_chars: usize) -> Self {
        Self { max_chars }
    }
}

#[async_trait]
impl OutputGuardrail for MaxOutputLength {
    async fn check(
        &self,
        output: &Message,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        let chars = output.content.chars().count();
        if chars > self.max_chars {
            return Ok(GuardrailOutcome::Tripped(Arc::from(format!(
                "output has {chars} characters, limit is {}",
                self.max_chars
            ))));
        }
        Ok(GuardrailOutcome::Passed)
    }
}

pub struct ShellCommandAllowlist {
    allowed: BTreeSet<Arc<str>>,
}

impl ShellCommandAllowlist {
    pub fn new(commands: impl IntoIterator<Item = impl Into<Arc<str>>>) -> Self {
        Self {
            allowed: commands.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ShellCallArgs {
    command: String,
}

#[async_trait]
impl ToolGuardrail for ShellCommandAllowlist {
    async fn check_call(
        &self,
        call: &ToolCall,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        if call.name.as_ref() != "shell" {
            return Ok(GuardrailOutcome::Passed);
        }

        let parsed: ShellCallArgs = serde_json::from_str(call.args.get())
            .map_err(|err| GuardrailError::Backend(err.to_string().into()))?;
        if parsed.command.split_whitespace().nth(1).is_some() {
            return Ok(GuardrailOutcome::Tripped(Arc::from(format!(
                "shell command '{}' includes arguments in the command field; use command='<program>' and put arguments in the structured args array",
                parsed.command
            ))));
        }
        if self.allowed.contains(parsed.command.as_str()) {
            Ok(GuardrailOutcome::Passed)
        } else {
            Ok(GuardrailOutcome::Tripped(Arc::from(format!(
                "shell command '{}' is not allowlisted; allowed commands: {}",
                parsed.command,
                self.allowed
                    .iter()
                    .map(|command| command.as_ref())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))))
        }
    }

    async fn check_result(
        &self,
        _result: &ToolResult,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        Ok(GuardrailOutcome::Passed)
    }
}

/// Hard boundary: refuses `file` writes that resolve inside the skill-bundle
/// directory. Wired into every sub-agent that is not the designated skill
/// editor, so a sub-agent with blanket `file` write (e.g. the code-fix agent)
/// cannot tamper with `SKILL.md` bundles even if steered to. Reads and
/// directory listings under the skills tree are unaffected.
pub struct SkillBundleWriteGuardrail;

#[derive(Debug, Deserialize)]
struct FileWriteArgs {
    operation: String,
    path: PathBuf,
}

/// Pure decision core: returns the resolved on-disk target when `call` is a
/// `file` *write* that lands inside `skills_root`, else `None`. Resolution
/// reuses `safe_workspace_path`, so `..`, absolute, and root-prefixed paths
/// are rejected by the same logic the file tool enforces — a `Some` result
/// cannot be produced by a traversal trick that escapes the skills tree.
fn skill_bundle_write_target(
    call: &ToolCall,
    ws_root: &Path,
    skills_root: &Path,
) -> Option<PathBuf> {
    if call.name.as_ref() != "file" {
        return None;
    }
    let parsed: FileWriteArgs = serde_json::from_str(call.args.get()).ok()?;
    if parsed.operation != "write" {
        return None;
    }
    // A path the file tool would itself reject (absolute / `..` / empty) is
    // not a skill-bundle write — let the tool surface its own error.
    let resolved = safe_workspace_path(ws_root, &parsed.path).ok()?;
    resolved.starts_with(skills_root).then_some(resolved)
}

#[async_trait]
impl ToolGuardrail for SkillBundleWriteGuardrail {
    async fn check_call(
        &self,
        call: &ToolCall,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        match skill_bundle_write_target(call, &workspace_root(), &skills_dir()) {
            Some(target) => Ok(GuardrailOutcome::Tripped(Arc::from(format!(
                "write to '{}' is inside the skill-bundle directory; skill-bundle edits must go through the skill-editor sub-agent via the skill_ops route, not this sub-agent",
                target.display()
            )))),
            None => Ok(GuardrailOutcome::Passed),
        }
    }

    async fn check_result(
        &self,
        _result: &ToolResult,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        Ok(GuardrailOutcome::Passed)
    }
}

fn contains_email_like(input: &str) -> bool {
    input.split_whitespace().any(is_email_like_token)
}

/// Heuristic email detection for one whitespace-delimited token. Tightened to
/// avoid false positives on URLs and npm-style specs (e.g.
/// `https://host/@scope/pkg@1.2.3/file.zip`), which also contain `@` and a dot:
/// a token with a path separator is never an email, the local part must use only
/// email-legal characters, and the domain must end in an alphabetic TLD.
fn is_email_like_token(raw: &str) -> bool {
    // Trim surrounding punctuation like "(a@b.com),"; emails start and end with
    // an alphanumeric character.
    let token = raw.trim_matches(|c: char| !c.is_ascii_alphanumeric());
    // URLs and filesystem paths contain '/' — never an email address. This is
    // what rejects scheme-prefixed URLs and scoped-package paths.
    if token.contains('/') {
        return false;
    }
    let Some((local, domain)) = token.split_once('@') else {
        return false;
    };
    if local.is_empty()
        || !local
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '%' | '+' | '-'))
    {
        return false;
    }
    is_email_domain(domain)
}

/// A domain is email-like when it has at least two dot-separated labels, every
/// label uses only `[A-Za-z0-9-]` (not leading/trailing `-`), and the final
/// label (the TLD) is alphabetic and at least two characters — so `1.2.3` (a
/// version) and `host/path` are rejected while `sub.example.com` is accepted.
fn is_email_domain(domain: &str) -> bool {
    let mut label_count = 0;
    let mut tld = "";
    for label in domain.split('.') {
        if label.is_empty()
            || label.starts_with('-')
            || label.ends_with('-')
            || !label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        {
            return false;
        }
        label_count += 1;
        tld = label;
    }
    label_count >= 2 && tld.len() >= 2 && tld.chars().all(|c| c.is_ascii_alphabetic())
}

fn contains_ssn_like(input: &str) -> bool {
    input.as_bytes().windows(11).any(|window| {
        window[0].is_ascii_digit()
            && window[1].is_ascii_digit()
            && window[2].is_ascii_digit()
            && window[3] == b'-'
            && window[4].is_ascii_digit()
            && window[5].is_ascii_digit()
            && window[6] == b'-'
            && window[7].is_ascii_digit()
            && window[8].is_ascii_digit()
            && window[9].is_ascii_digit()
            && window[10].is_ascii_digit()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{ToolCall, ToolCallId};
    use serde_json::{json, value::RawValue};

    fn shell_call(command: &str) -> ToolCall {
        ToolCall {
            id: ToolCallId::new("shell-test"),
            name: Arc::from("shell"),
            args: RawValue::from_string(json!({ "command": command }).to_string()).unwrap(),
        }
    }

    fn file_call(operation: &str, path: &str) -> ToolCall {
        ToolCall {
            id: ToolCallId::new("file-test"),
            name: Arc::from("file"),
            args: RawValue::from_string(
                json!({ "operation": operation, "path": path, "content": "x" }).to_string(),
            )
            .unwrap(),
        }
    }

    fn roots() -> (PathBuf, PathBuf) {
        let ws = PathBuf::from("/ws");
        let skills = ws.join("workspace").join("skills");
        (ws, skills)
    }

    #[test]
    fn email_heuristic_matches_real_addresses() {
        assert!(contains_email_like(
            "reach me at john.doe@example.com please"
        ));
        assert!(contains_email_like("a@b.co"));
        assert!(contains_email_like("(jane_smith+tag@sub.example.org),"));
    }

    #[test]
    fn email_heuristic_ignores_urls_and_package_specs() {
        // The reported false positive: a scoped-package CDN URL.
        assert!(!contains_email_like(
            "https://unpkg.luckincoffeecdn.com/@luckin/my-coffee-skill@latest/dist/my-coffee-skill.zip"
        ));
        assert!(!contains_email_like("install @scope/pkg@1.2.3 now"));
        assert!(!contains_email_like("see https://example.com/path@v2/file"));
        // A version string with dots but no alphabetic TLD must not match.
        assert!(!contains_email_like("bump to release@1.2.30"));
    }

    #[test]
    fn trips_on_skill_bundle_write() {
        let (ws, skills) = roots();
        let target = skill_bundle_write_target(
            &file_call("write", "workspace/skills/audit-skill/SKILL.md"),
            &ws,
            &skills,
        );
        assert_eq!(
            target,
            Some(ws.join("workspace/skills/audit-skill/SKILL.md"))
        );
    }

    #[test]
    fn allows_skill_bundle_read_and_listing() {
        let (ws, skills) = roots();
        assert_eq!(
            skill_bundle_write_target(
                &file_call("read", "workspace/skills/audit-skill/SKILL.md"),
                &ws,
                &skills
            ),
            None
        );
    }

    #[test]
    fn allows_write_outside_skill_bundles() {
        let (ws, skills) = roots();
        assert_eq!(
            skill_bundle_write_target(
                &file_call("write", "crates/agentos-core/src/lib.rs"),
                &ws,
                &skills
            ),
            None
        );
    }

    #[test]
    fn traversal_into_skills_is_rejected_not_allowed() {
        let (ws, skills) = roots();
        // `safe_workspace_path` rejects any `..`, so this never resolves to a
        // skills path that slips past the guard.
        assert_eq!(
            skill_bundle_write_target(
                &file_call("write", "crates/../workspace/skills/x/SKILL.md"),
                &ws,
                &skills
            ),
            None
        );
    }

    #[test]
    fn non_file_tool_is_ignored() {
        let (ws, skills) = roots();
        assert_eq!(
            skill_bundle_write_target(&shell_call("ls"), &ws, &skills),
            None
        );
    }

    #[tokio::test]
    async fn shell_allowlist_rejects_arguments_in_command_field_with_hint() {
        let guardrail = ShellCommandAllowlist::new(["find"]);
        let ctx_state = agentos_interfaces::RunState::new(
            agentos_proto::RunId::new("run"),
            agentos_proto::AgentId::new("agent"),
        );
        let ctx = RunContext::from_state(&ctx_state);

        let outcome = guardrail
            .check_call(&shell_call("find workspace"), &ctx)
            .await
            .expect("guardrail should run");

        match outcome {
            GuardrailOutcome::Tripped(reason) => {
                assert!(reason.contains("structured args array"));
            }
            GuardrailOutcome::Passed => panic!("command with embedded args should trip"),
        }
    }
}
