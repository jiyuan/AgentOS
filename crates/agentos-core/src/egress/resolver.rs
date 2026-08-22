//! The DNS hook that makes [`super::policy`] the thing that actually decides.
//!
//! M4 / `NET-001`. Checking a URL's host before the request and then handing
//! the URL to a client that resolves it again is the classic DNS-rebinding
//! shape: the check sees a public address, the connect sees `127.0.0.1`, and
//! nothing has to be compromised except a DNS record with a short TTL.
//!
//! Filtering *inside* the resolver removes the gap. There is exactly one
//! lookup, its result is what the connection uses, and an address the policy
//! refuses never leaves this function. Redirects get it for free: each hop is
//! a fresh connection and therefore a fresh resolution.

use super::policy::{check_address, Blocked};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::net::SocketAddr;
use tracing::warn;

/// Resolves names and drops every address a tool may not reach.
#[derive(Debug, Default)]
pub struct GuardedResolver;

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            // Port 0: reqwest replaces it with the URL's port, or the scheme's
            // default. `lookup_host` needs *a* port to return `SocketAddr`s.
            let resolved: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|source| Blocked::Unresolvable {
                    host: host.clone(),
                    reason: source.to_string(),
                })?
                .collect();

            let mut refusals = Vec::new();
            let allowed: Vec<SocketAddr> = resolved
                .into_iter()
                .filter(|address| match check_address(address.ip()) {
                    Ok(()) => true,
                    Err(refusal) => {
                        refusals.push(refusal.to_string());
                        false
                    }
                })
                .collect();

            if allowed.is_empty() {
                // Named at warn rather than swallowed: a tool call that fails
                // because the name it asked for is internal is a thing an
                // operator wants to see, whether it was an accident or an
                // injected instruction.
                warn!(
                    host = host.as_str(),
                    refused = refusals.join("; ").as_str(),
                    "refusing an outbound tool request to a non-public address"
                );
                return Err(Box::new(Blocked::HostUnreachable {
                    host,
                    reason: refusals.join("; "),
                })
                    as Box<dyn std::error::Error + Send + Sync>);
            }

            // A partially-refused name still connects, to the addresses that
            // passed. Dual-stack hosts routinely have one family reachable and
            // the other not, and refusing the whole name for that would break
            // ordinary fetches without preventing anything.
            Ok(Box::new(allowed.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// `Addrs` is a boxed iterator with no `Debug`, so the usual
    /// `expect_err` is not available.
    async fn refusal(host: &str) -> String {
        match GuardedResolver
            .resolve(Name::from_str(host).expect("a valid name"))
            .await
        {
            Ok(mut addrs) => panic!(
                "{host} should not have resolved to anything reachable; got {:?}",
                addrs.next()
            ),
            Err(error) => error.to_string(),
        }
    }

    /// `localhost` is the case a hostname allowlist would have to special-case
    /// and an address check gets right for free.
    #[tokio::test]
    async fn a_name_that_resolves_to_loopback_resolves_to_nothing() {
        let error = refusal("localhost").await;
        assert!(error.contains("localhost"), "{error}");
    }

    #[tokio::test]
    async fn a_name_that_does_not_resolve_says_so_rather_than_being_allowed() {
        let error = refusal("agentos.invalid").await;
        assert!(error.contains("agentos.invalid"), "{error}");
    }
}
