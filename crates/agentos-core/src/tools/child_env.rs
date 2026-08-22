//! What a tool subprocess is allowed to know about the process that started
//! it.
//!
//! M4 / `PROC-001`. `tools/exec.rs` never called `env_clear`, so every child
//! inherited the gateway's whole environment. The gateway's environment is
//! where its credentials live: `ANTHROPIC_API_KEY`, `DEEPSEEK_API_KEY`,
//! `TELEGRAM_BOT_TOKEN`, `FEISHU_APP_SECRET`, whatever else the deployment
//! exports. Every shell command the model chose to run could read all of them
//! with `env`, and the isolation worker — the process whose entire purpose is
//! containment — inherited them too.
//!
//! The sandbox does not help here: Landlock and Seatbelt bound filesystem
//! *writes*, and a leaked key does not need to be written anywhere to be used.
//!
//! # An allowlist, not a denylist
//!
//! Naming the variables to strip would mean knowing every credential every
//! deployment might export, forever. Naming the ones to keep means a new
//! secret is excluded by default and the cost of being wrong is a tool that
//! needs an unusual variable and does not get it — visible, diagnosable, and
//! fixed with one config line ([`crate::config::IsolationConfig`]'s
//! `env_passthrough`).

use std::collections::BTreeMap;
use std::ffi::OsString;

/// Variables a child process needs to behave like a normal program, and that
/// carry no authority.
///
/// `PATH` so it can find its own binaries; `HOME` and `TMPDIR` so it has
/// somewhere to put things; the locale and `TZ` so its output does not depend
/// on where the gateway happens to be running; `TERM` so anything that checks
/// for a terminal gets a consistent answer.
const NEUTRAL: &[&str] = &[
    "PATH",
    "HOME",
    "TMPDIR",
    "TERM",
    "TZ",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LC_NUMERIC",
    "LC_TIME",
    "USER",
    "LOGNAME",
    "SHELL",
];

/// Proxy settings, which are reachability rather than authority.
///
/// Kept because a deployment behind a proxy is a deployment where dropping
/// these turns every outbound call a tool makes into a timeout, and the
/// failure looks nothing like its cause. A proxy URL *can* carry credentials
/// in its userinfo; a deployment that does that is handing them to every
/// program it runs already, and the honest fix is a proxy that does not need
/// them rather than an allowlist that pretends otherwise.
const PROXY: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "all_proxy",
];

/// AgentOS's own variables that a child legitimately needs.
///
/// `AGENTOS_HOME` in particular: the isolation worker resolves the workspace
/// root from it, so a worker started without it would sandbox the wrong
/// directory. Deliberately *not* a prefix match on `AGENTOS_*` — that would
/// re-admit `AGENTOS_TELEGRAM_BOT_TOKEN` and every other credential the
/// project namespaces to itself.
const AGENTOS: &[&str] = &["AGENTOS_HOME"];

/// The environment a tool subprocess starts with.
///
/// Ordered and deduplicated, so two calls with the same parent environment
/// produce byte-identical results — which is what makes a golden transcript
/// over a subprocess reproducible.
pub fn minimal(extra: &[String]) -> BTreeMap<OsString, OsString> {
    NEUTRAL
        .iter()
        .chain(PROXY)
        .chain(AGENTOS)
        .map(|name| (*name).to_owned())
        .chain(extra.iter().cloned())
        .filter_map(|name| {
            let value = std::env::var_os(&name)?;
            Some((OsString::from(name), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The finding, as a property: nothing the deployment exported as a
    /// credential survives into a child.
    #[test]
    fn a_credential_is_not_in_the_child_environment() {
        // SAFETY: single-threaded within this test, and a canary rather than
        // a real credential.
        unsafe {
            std::env::set_var("ANTHROPIC_API_KEY", "sk-canary");
            std::env::set_var("AGENTOS_TELEGRAM_BOT_TOKEN", "tg-canary");
        }

        let env = minimal(&[]);

        assert!(!env.contains_key(&OsString::from("ANTHROPIC_API_KEY")));
        assert!(!env.contains_key(&OsString::from("AGENTOS_TELEGRAM_BOT_TOKEN")));

        unsafe {
            std::env::remove_var("ANTHROPIC_API_KEY");
            std::env::remove_var("AGENTOS_TELEGRAM_BOT_TOKEN");
        }
    }

    /// The `AGENTOS_*` allowlist is by name, not by prefix — the prefix would
    /// have readmitted the token above.
    #[test]
    fn the_workspace_anchor_still_reaches_the_child() {
        // SAFETY: as above.
        unsafe { std::env::set_var("AGENTOS_HOME", "/tmp/agentos-child-env-probe") };
        let env = minimal(&[]);
        assert_eq!(
            env.get(&OsString::from("AGENTOS_HOME"))
                .map(|v| v.as_os_str()),
            Some(std::ffi::OsStr::new("/tmp/agentos-child-env-probe"))
        );
    }

    /// A deployment that needs an unusual variable says so once, rather than
    /// the allowlist growing to guess at every possible one.
    #[test]
    fn a_deployment_can_name_one_more() {
        // SAFETY: as above.
        unsafe { std::env::set_var("AGENTOS_CHILD_ENV_EXTRA", "wanted") };
        assert!(!minimal(&[]).contains_key(&OsString::from("AGENTOS_CHILD_ENV_EXTRA")));
        assert!(minimal(&["AGENTOS_CHILD_ENV_EXTRA".to_owned()])
            .contains_key(&OsString::from("AGENTOS_CHILD_ENV_EXTRA")));
        unsafe { std::env::remove_var("AGENTOS_CHILD_ENV_EXTRA") };
    }

    /// A name on the allowlist that the parent does not have is absent rather
    /// than empty: an empty `HTTPS_PROXY` means something different from no
    /// `HTTPS_PROXY` to most programs that read it.
    #[test]
    fn an_unset_allowlisted_name_is_not_invented() {
        // SAFETY: as above.
        unsafe { std::env::remove_var("ALL_PROXY") };
        assert!(!minimal(&[]).contains_key(&OsString::from("ALL_PROXY")));
    }
}
