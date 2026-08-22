//! Where a tool-driven HTTP request is allowed to go, and how much of the
//! answer it may keep.
//!
//! M4 / `NET-001`. Three separate holes, one module:
//!
//! - **Destination.** [`policy`] decides by resolved address, not by name, so
//!   `localtest.me` and an attacker's short-TTL record are the same case as a
//!   literal `127.0.0.1`.
//! - **Rebinding and redirects.** [`resolver`] puts that decision inside DNS
//!   resolution, so the address checked is the address connected to, on the
//!   first hop and on every redirect.
//! - **Size.** [`fetch`] streams a response and stops at a byte cap, rather
//!   than `.text()`-ing whatever the far end decided to send.

pub mod fetch;
pub mod policy;
pub mod resolver;

pub use fetch::{fetch_bounded, fetch_capped, FetchError, Fetched, MAX_REDIRECTS};
pub use policy::{check_address, check_scheme, check_url, Blocked};
pub use resolver::GuardedResolver;

use reqwest::redirect;
use reqwest::Client;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Bytes of a response body one tool call may keep.
///
/// Matched to `[limits].tool_result_inline_bytes`' order of magnitude rather
/// than to `exec`'s 4 MiB: a web page the model is going to read is a
/// different thing from a build log that will be spilled.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Longest a tool's HTTP request may take end to end.
const TOOL_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// Opt back in to using the ambient HTTP proxy for tool requests, accepting
/// that the proxy — not this policy — then decides where they go.
pub const PROXY_OPT_IN_ENV: &str = "AGENTOS_TOOL_EGRESS_PROXY";

/// The client for requests whose destination the *model* chose.
///
/// Deliberately not [`crate::http::shared_client`]. That one is for endpoints
/// the *operator* configured — a Qdrant on `localhost:6333`, an Ollama on
/// `127.0.0.1:11434`, a Telegram proxy on loopback — and applying this policy
/// to it would break every one of those for no security gain: an operator who
/// writes an address into `agent.toml` has already decided to reach it. The
/// split is the whole point. What this policy defends against is a
/// *destination the model was talked into*, which is a different trust
/// question with a different answer.
///
/// # Why the ambient proxy is off
///
/// A proxy resolves the destination itself. This client would connect to the
/// proxy — one address, always the same, always allowed — and hand it a
/// hostname, so [`GuardedResolver`] would never see the name that matters and
/// the whole policy would quietly be doing nothing. On a machine behind a
/// corporate proxy that is precisely the machine where reaching
/// `http://internal.corp/` matters most.
///
/// So `no_proxy` by default, and [`PROXY_OPT_IN_ENV`] to take it back. The
/// opt-in is loud rather than silent because what it buys is reachability and
/// what it costs is the policy — and a deployment that needs it should know it
/// is trading one for the other.
pub fn tool_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        let mut builder = Client::builder()
            .timeout(TOOL_REQUEST_TIMEOUT)
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .user_agent(concat!("agentos-core/", env!("CARGO_PKG_VERSION")))
            // Every name this client resolves is judged before it is
            // connected to, on the first hop and on every redirect.
            .dns_resolver(Arc::new(GuardedResolver))
            .redirect(guarded_redirects());
        if std::env::var_os(PROXY_OPT_IN_ENV).is_some() {
            tracing::warn!(
                variable = PROXY_OPT_IN_ENV,
                "tool HTTP requests will use the ambient proxy; the proxy, not the \
                 egress policy, decides which destinations are reachable"
            );
        } else {
            builder = builder.no_proxy();
        }
        builder
            .build()
            .expect("reqwest client builds with rustls + http2 features compiled in")
    })
}

/// Follow at most [`MAX_REDIRECTS`] hops, and refuse one that names an
/// address rather than a name.
///
/// The resolver covers names. A `Location: http://127.0.0.1:8080/` needs no
/// resolution at all, so without this it would be the one way out of the
/// policy.
fn guarded_redirects() -> redirect::Policy {
    redirect::Policy::custom(|attempt| {
        if attempt.previous().len() >= MAX_REDIRECTS {
            let host = attempt.url().to_string();
            return attempt.error(Blocked::HostUnreachable {
                host,
                reason: format!("more than {MAX_REDIRECTS} redirects"),
            });
        }
        match policy::check_url(attempt.url()) {
            Ok(()) => attempt.follow(),
            Err(refusal) => attempt.error(refusal),
        }
    })
}
