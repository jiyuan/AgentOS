//! `[gateway]` — how the persistent gateway spreads conversations over threads.
//!
//! Roadmap item G1, extended by M8 / `GW-001`. Three numbers: how many shard
//! threads conversations are hashed across, how much one conversation may have
//! waiting before the gateway starts refusing its input, and how long a
//! `SIGTERM` waits for the turns already running. The first two are about
//! isolation rather than speed; the third is about not ending a turn between
//! two instructions.

use crate::gateway::{DEFAULT_INBOX_CAPACITY, DEFAULT_SHUTDOWN_GRACE_SECS};
use serde::{Deserialize, Serialize};

/// Ceiling on `shards`. Each shard is an OS thread with its own tokio runtime;
/// well past the core count they stop buying concurrency and start costing
/// context switches, and a typo (`shards = 640`) should fail rather than
/// quietly spawn 640 runtimes.
const MAX_SHARDS: usize = 64;

/// Ceiling on `shutdown_grace_secs`. Ten minutes is already far past what a
/// service manager will wait before sending `SIGKILL` itself, so a larger
/// value is a typo rather than a policy. Zero is allowed and means "do not
/// wait": abandon whatever is in flight and exit.
const MAX_SHUTDOWN_GRACE_SECS: u64 = 600;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewayConfig {
    /// Shard threads, or `0` for "one per core".
    ///
    /// The run loop's future is `!Send`, so a run cannot migrate between
    /// threads; conversations are instead hashed onto a fixed shard. More
    /// shards means more conversations running in parallel, never one
    /// conversation running faster.
    pub shards: usize,
    /// Envelopes one conversation may have waiting for a run of their own
    /// before the gateway tells the user it is behind.
    ///
    /// Per conversation, not per process: a user typing faster than the agent
    /// answers hits their own bound and nobody else's.
    pub inbox_capacity: usize,
    /// Seconds the gateway waits for in-flight turns after `SIGTERM` before
    /// exiting anyway.
    ///
    /// The router stops accepting immediately; this bounds the *drain*. A
    /// shard wedged on a tool that ignores its deadline must not turn a stop
    /// into a hang, so past this the gateway reports what it is abandoning —
    /// from the ingress ledger — and exits.
    pub shutdown_grace_secs: u64,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            shards: 0,
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            shutdown_grace_secs: DEFAULT_SHUTDOWN_GRACE_SECS,
        }
    }
}

impl GatewayConfig {
    /// Resolve `shards` against this machine, turning the `0` default into the
    /// core count. Always at least one — a gateway with no shards is a gateway
    /// that answers nothing.
    pub fn shard_count(&self) -> usize {
        if self.shards > 0 {
            return self.shards.min(MAX_SHARDS);
        }
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .min(MAX_SHARDS)
    }
}

/// Reject a misconfigured section at load rather than at the first message.
pub fn validate_gateway(config: &GatewayConfig) -> Result<(), String> {
    if config.shards > MAX_SHARDS {
        return Err(format!(
            "gateway.shards must be at most {MAX_SHARDS} (0 means one per core), got {}",
            config.shards
        ));
    }
    if config.inbox_capacity == 0 {
        return Err(
            "gateway.inbox_capacity must be at least 1; zero would refuse every message".to_owned(),
        );
    }
    if config.shutdown_grace_secs > MAX_SHUTDOWN_GRACE_SECS {
        return Err(format!(
            "gateway.shutdown_grace_secs must be at most {MAX_SHUTDOWN_GRACE_SECS}, got {}",
            config.shutdown_grace_secs
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_omitted_section_parses_to_the_default() {
        let parsed: GatewayConfig = toml::from_str("").expect("an empty section parses");
        assert_eq!(parsed, GatewayConfig::default());
        assert!(validate_gateway(&parsed).is_ok());
    }

    /// The default has to work on a one-core box as well as a big one.
    #[test]
    fn the_default_resolves_to_at_least_one_shard() {
        assert!(GatewayConfig::default().shard_count() >= 1);
    }

    #[test]
    fn an_explicit_count_is_taken_as_written() {
        let config = GatewayConfig {
            shards: 3,
            ..Default::default()
        };
        assert_eq!(config.shard_count(), 3);
    }

    /// A typo should fail the load, not spawn hundreds of runtimes.
    #[test]
    fn an_absurd_shard_count_is_rejected() {
        let config = GatewayConfig {
            shards: 640,
            ..Default::default()
        };
        assert!(validate_gateway(&config)
            .expect_err("640 shards must be rejected")
            .contains("gateway.shards"));
    }

    #[test]
    fn a_zero_inbox_is_rejected() {
        let config = GatewayConfig {
            inbox_capacity: 0,
            ..Default::default()
        };
        assert!(validate_gateway(&config)
            .expect_err("zero must be rejected")
            .contains("inbox_capacity"));
    }

    /// Zero is a policy ("do not wait"); ten minutes is a typo, because no
    /// service manager waits that long before sending `SIGKILL` itself.
    #[test]
    fn the_shutdown_grace_admits_zero_and_rejects_the_absurd() {
        let config = GatewayConfig {
            shutdown_grace_secs: 0,
            ..Default::default()
        };
        assert!(validate_gateway(&config).is_ok());

        let config = GatewayConfig {
            shutdown_grace_secs: 86_400,
            ..Default::default()
        };
        assert!(validate_gateway(&config)
            .expect_err("a day must be rejected")
            .contains("shutdown_grace_secs"));
    }

    #[test]
    fn an_unknown_key_is_rejected_rather_than_ignored() {
        let error = toml::from_str::<GatewayConfig>("shard = 2").expect_err("a typo must fail");
        assert!(error.to_string().contains("shard"));
    }
}
