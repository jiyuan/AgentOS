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
//! **Verified by construction, not by running.** This module is compiled and
//! unit-tested on every platform, not just macOS — it is string building and
//! one `Path::exists`, so there is no reason for the profile builder or the
//! quote escaping to go unchecked on the machine that happens to run CI. What
//! cannot be checked anywhere but macOS is whether Seatbelt then honours the
//! profile, and the enforcement suite skips with a reason where
//! [`super::availability`] does not report it. `sandbox-exec` is also deprecated by Apple, still present
//! and still the only unprivileged option — if it is removed, `availability`
//! starts reporting unavailable and tools declaring a sandbox stop running
//! rather than quietly running unsandboxed.

use super::{Availability, Sandbox};
use std::path::Path;

/// Apple's profile interpreter. Deprecated, present on every supported release.
const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

pub(super) fn availability() -> Availability {
    if Path::new(SANDBOX_EXEC).exists() {
        Availability::Enforced("seatbelt")
    } else {
        Availability::Unavailable("this machine has no /usr/bin/sandbox-exec")
    }
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
    use agentos_interfaces::tool::SandboxMode;
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
