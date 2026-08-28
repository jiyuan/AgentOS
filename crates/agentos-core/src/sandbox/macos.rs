//! Seatbelt: the macOS backend for [`super::Sandbox`].
//!
//! macOS has no in-process equivalent of Landlock's "give up access now, keep
//! running". The supported way to run something under a profile is to exec it
//! through `sandbox-exec`, so this backend rewrites the command rather than
//! restricting the child after fork.
//!
//! The generated profile allows everything and then subtracts writes globally,
//! granting each canonical writable root back explicitly. Canonicalization is
//! load-bearing on macOS: `/var` is reached through `/private/var`, and
//! Seatbelt judges the resolved vnode path rather than the spelling an
//! operator supplied.
//!
//! **Verified by construction, and — for availability — by running.** The
//! profile builder and the quote escaping are compiled and unit-tested on every
//! platform, not just macOS: they are string building, so there is no reason to
//! leave them unchecked on the machine that happens to run CI.
//!
//! [`availability`] used to be one `Path::exists`, which reported
//! `Enforced("seatbelt")` for any machine that merely *had* the binary — a
//! strictly weaker claim than Linux's, which issues a real
//! `landlock_create_ruleset` version syscall. It now applies a profile that
//! denies writes and confirms a write is actually refused, so `Enforced` means
//! Seatbelt was observed enforcing rather than assumed to (M2 / `CI-001`,
//! §4.3 of the audit remediation plan). The result is cached: the probe spawns
//! a process, and nothing needs it more than once.
//!
//! `sandbox-exec` is deprecated by Apple, still present and still the only
//! unprivileged option — if it is removed, `availability` reports unavailable
//! and tools declaring a sandbox stop running rather than quietly running
//! unsandboxed.

use super::{Availability, Sandbox};
use std::path::Path;
use std::sync::OnceLock;

/// Apple's profile interpreter. Deprecated, present on every supported release.
const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

/// What the probe observed, from its two signals.
///
/// Pure, so the reasoning is unit-tested on every platform even though only
/// macOS can produce the inputs. The interesting case is the third: the
/// interpreter ran and reported success, and the write it was supposed to stop
/// landed anyway. Reporting `Enforced` there is exactly the overstatement this
/// probe exists to remove.
fn verdict(spawned: bool, refused_the_write: bool) -> Availability {
    match (spawned, refused_the_write) {
        (false, _) => {
            Availability::Unavailable("/usr/bin/sandbox-exec is missing or could not be run")
        }
        (true, true) => Availability::Enforced("seatbelt"),
        (true, false) => Availability::Unavailable(
            "sandbox-exec ran but a denied write still succeeded; Seatbelt is not enforcing here",
        ),
    }
}

pub(super) fn availability() -> Availability {
    static PROBED: OnceLock<Availability> = OnceLock::new();
    *PROBED.get_or_init(probe)
}

/// Apply a deny-writes profile and check that a write is actually refused.
///
/// The target is under the system temp directory and is removed either way, so
/// a machine where the probe *fails* to be enforced does not accumulate files.
fn probe() -> Availability {
    if !Path::new(SANDBOX_EXEC).exists() {
        return verdict(false, false);
    }
    let target =
        std::env::temp_dir().join(format!("agentos-seatbelt-probe-{}", std::process::id()));
    let _ = std::fs::remove_file(&target);

    // `(allow default)` then `(deny file-write*)`, the same subtract-from-open
    // shape `profile` builds, with no writable subpath granted back.
    let output = std::process::Command::new(SANDBOX_EXEC)
        .arg("-p")
        .arg("(version 1)\n(allow default)\n(deny file-write*)\n")
        .arg("/bin/sh")
        .arg("-c")
        .arg(format!("echo probe > {}", target.display()))
        .output();

    if output.is_err() {
        return verdict(false, false);
    }
    // Whether the file exists is the signal, and the exit status deliberately
    // is not: a shell redirection Seatbelt blocks fails the command, but a
    // shell reporting success while the write went nowhere is equally fine.
    // What must not happen is the file being there afterwards.
    let wrote = target.exists();
    let _ = std::fs::remove_file(&target);
    verdict(true, !wrote)
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
        let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
        profile.push_str(&format!(
            "(allow file-write* (subpath \"{}\"))\n",
            escape(&canonical.to_string_lossy())
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

    #[test]
    fn read_only_denies_writes_and_grants_nothing_back() {
        let text = profile(&Sandbox::new(SandboxMode::ReadOnly, PathBuf::from("/ws")));
        assert!(text.contains("(deny file-write*)"), "denies writes");
        assert!(!text.contains("(subpath"), "read_only grants no subpath");
        assert!(text.contains("(allow file-write-data (literal \"/dev/null\"))"));
    }

    #[test]
    fn workspace_write_grants_the_workspace_after_the_deny() {
        let text = profile(&Sandbox::new(
            SandboxMode::WorkspaceWrite,
            PathBuf::from("/ws"),
        ));
        assert!(text.contains("(allow file-write* (subpath \"/ws\"))"));
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

    /// The probe's reasoning, checked on every platform even though only macOS
    /// can produce its inputs.
    #[test]
    fn a_machine_without_the_interpreter_is_unavailable() {
        assert!(matches!(
            verdict(false, false),
            Availability::Unavailable(_)
        ));
        assert!(matches!(verdict(false, true), Availability::Unavailable(_)));
    }

    #[test]
    fn a_refused_write_is_the_only_thing_that_counts_as_enforced() {
        assert_eq!(verdict(true, true), Availability::Enforced("seatbelt"));
    }

    /// The case `Path::exists` reported as `Enforced` and this one does not:
    /// the interpreter is there, and it did not stop the write.
    #[test]
    fn an_interpreter_that_does_not_enforce_is_unavailable() {
        let Availability::Unavailable(reason) = verdict(true, false) else {
            panic!("a write that was not refused cannot be reported as enforced");
        };
        assert!(reason.contains("not enforcing"), "{reason}");
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
