//! Seatbelt: the macOS backend for [`super::Sandbox`].
//!
//! macOS has no in-process equivalent of Landlock's "give up access now, keep
//! running". The supported way to run something under a profile is to exec it
//! through `sandbox-exec`, so this backend rewrites the command rather than
//! restricting the child after fork.
//!
//! The generated profile allows everything and then subtracts: `deny
//! file-write*` globally, then `allow file-write*` beneath each writable root.
//! Subtracting is the only order that fails closed — a profile that listed what
//! is allowed would have to enumerate every path a shell command legitimately
//! touches, and would deny by accident rather than by decision.
//!
//! Profile construction is unit-tested on every platform. On macOS,
//! [`super::availability`] additionally runs and caches a real probe: a
//! permissive profile must start successfully and a read-only profile must
//! deny a control write. Binary presence alone is not support. `sandbox-exec`
//! is deprecated by Apple but remains the only unprivileged option; removal or
//! profile-application failure makes sandboxed tools fail closed.

use super::{Availability, Sandbox};
use agentos_interfaces::tool::SandboxMode;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Apple's profile interpreter. Deprecated, present on every supported release.
const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

pub(super) fn availability() -> Availability {
    static PROBED: OnceLock<Availability> = OnceLock::new();
    *PROBED.get_or_init(probe_availability)
}

fn probe_availability() -> Availability {
    if !Path::new(SANDBOX_EXEC).exists() {
        return Availability::Unavailable("this machine has no /usr/bin/sandbox-exec");
    }

    let permissive = Command::new(SANDBOX_EXEC)
        .args(["-p", "(version 1)\n(allow default)", "/usr/bin/true"])
        .output();
    if !permissive.is_ok_and(|output| output.status.success()) {
        return Availability::Unavailable("sandbox-exec cannot apply a Seatbelt profile");
    }

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let target = std::env::temp_dir().join(format!(
        ".agentos-seatbelt-probe-{}-{nonce}",
        std::process::id()
    ));
    if std::fs::write(&target, b"control").is_err() {
        return Availability::Unavailable("Seatbelt probe cannot write its control file");
    }
    let _ = std::fs::remove_file(&target);

    let sandbox = Sandbox::new(SandboxMode::ReadOnly, std::env::temp_dir());
    let denied = Command::new(SANDBOX_EXEC)
        .args(["-p", &profile(&sandbox), "/usr/bin/touch"])
        .arg(&target)
        .output();
    let restriction_held = denied.is_ok_and(|output| !output.status.success()) && !target.exists();
    let _ = std::fs::remove_file(&target);
    if !restriction_held {
        return Availability::Unavailable("Seatbelt probe did not deny a forbidden write");
    }

    Availability::Enforced("seatbelt")
}

/// Put the command inside `sandbox-exec -p <profile>`.
pub(super) fn wrap(sandbox: &Sandbox, program: &str, args: &[String]) -> (String, Vec<String>) {
    let mut wrapped = vec!["-p".to_owned(), profile(sandbox), program.to_owned()];
    wrapped.extend(args.iter().cloned());
    (SANDBOX_EXEC.to_owned(), wrapped)
}

/// Build the Seatbelt profile for this sandbox.
pub(super) fn profile(sandbox: &Sandbox) -> String {
    let mut profile = String::from("(version 1)\n(allow default)\n(deny file-write*)\n");
    // `/dev/null` for the same reason Landlock grants it: `2>/dev/null` is
    // ordinary shell, and refusing it reads as the tool being broken.
    profile.push_str("(allow file-write-data (literal \"/dev/null\"))\n");
    for path in sandbox.writable() {
        profile.push_str(&format!(
            "(allow file-write* (subpath \"{}\"))\n",
            escape(&path.to_string_lossy())
        ));
    }
    profile
}

/// Escape a path for a profile string literal. A path containing a quote or a
/// backslash would otherwise end the literal early and change what the rest of
/// the profile means.
fn escape(path: &str) -> String {
    path.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// The order matters: a profile that allowed writes before denying them
    /// would allow everything.
    #[test]
    fn read_only_denies_writes_and_grants_nothing_back() {
        let text = profile(&Sandbox::new(SandboxMode::ReadOnly, PathBuf::from("/ws")));
        let deny = text.find("(deny file-write*)").expect("denies writes");
        assert!(!text.contains("(subpath"), "read_only grants no subpath");
        assert!(text.find("(allow default)").expect("allows by default") < deny);
    }

    #[test]
    fn workspace_write_grants_the_workspace_after_the_deny() {
        let text = profile(&Sandbox::new(
            SandboxMode::WorkspaceWrite,
            PathBuf::from("/ws"),
        ));
        let deny = text.find("(deny file-write*)").expect("denies writes");
        let grant = text
            .find("(subpath \"/ws\")")
            .expect("grants the workspace");
        assert!(deny < grant, "the grant must come after the deny");
    }

    /// A quote in a path would otherwise close the literal and let the rest of
    /// the path be read as profile syntax.
    #[test]
    fn a_path_cannot_break_out_of_its_literal() {
        let text = profile(&Sandbox::new(
            SandboxMode::WorkspaceWrite,
            PathBuf::from("/ws\") (allow file-write*) (subpath \"/"),
        ));
        assert!(text.contains("\\\""), "quotes are escaped");
        assert_eq!(
            text.matches("(allow file-write* (subpath").count(),
            2,
            "the injected rule must not become a rule"
        );
    }

    #[test]
    fn wrapping_keeps_the_command_and_its_arguments() {
        let sandbox = Sandbox::new(SandboxMode::ReadOnly, PathBuf::from("/ws"));
        let (program, args) = wrap(&sandbox, "ls", &["-la".to_owned()]);
        assert_eq!(program, SANDBOX_EXEC);
        assert_eq!(args[0], "-p");
        assert_eq!(args[2], "ls");
        assert_eq!(args[3], "-la");
    }
}
