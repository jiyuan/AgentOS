//! SSRF, end to end, against a real listener.
//!
//! M4 / `NET-001`. The unit tests in `egress/policy.rs` decide addresses in
//! isolation; these drive the actual client the `http` tool uses, so they also
//! prove the policy is *wired in* — that the resolver is installed, that the
//! redirect policy is installed, and that the body cap is applied to a real
//! stream rather than to a `Content-Length` header.
//!
//! The listener binds `127.0.0.1`, which the policy refuses, so every request
//! that must be refused is aimed at it and every request that must succeed is
//! aimed at the same listener *through* something the policy allows. Nothing
//! here touches the network.

use agentos_core::egress::{fetch_bounded, fetch_capped, tool_client, Blocked, FetchError};
use agentos_core::tools::HttpTool;
use agentos_interfaces::tool::Tool;
use agentos_proto::{ToolCall, ToolCallId, ToolStatus};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use serde_json::value::RawValue;
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A one-shot HTTP server on loopback that answers every request with
/// `response`, and the address it is listening on.
async fn serve_once(response: &'static str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback binds");
    let address = listener.local_addr().expect("has an address");
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut scratch = [0u8; 4096];
            let _ = stream.read(&mut scratch).await;
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.flush().await;
        }
    });
    address
}

async fn http_tool_call(url: &str) -> agentos_proto::ToolResult {
    let tool = HttpTool::default();
    let args = serde_json::json!({ "url": url }).to_string();
    let args = RawValue::from_string(args).expect("valid JSON");
    let call = ToolCall {
        id: ToolCallId::new("call-1"),
        name: Arc::from("http"),
        args: RawValue::from_string("{}".to_owned()).expect("valid JSON"),
    };
    tool.call(&call, &args)
        .await
        .expect("a refusal is a result")
}

/// The plain case, and the one an operator is most likely to have running: a
/// service on loopback that answers HTTP.
#[tokio::test]
async fn a_loopback_service_is_not_reachable_by_address() {
    let address = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecret").await;
    let result = http_tool_call(&format!("http://{address}/")).await;

    assert_eq!(result.status, ToolStatus::Failed);
    assert!(
        !result.content.contains("secret"),
        "the body must not have been read: {}",
        result.content
    );
    assert_eq!(
        result.metadata.get("egress_refused"),
        Some(&serde_json::Value::Bool(true))
    );
}

/// The same service reached by a *name* that resolves to it. A URL check
/// cannot see this; the resolver can.
#[tokio::test]
async fn a_name_that_resolves_to_loopback_is_not_reachable_either() {
    let address = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecret").await;
    let result = http_tool_call(&format!("http://localhost:{}/", address.port())).await;

    assert_eq!(result.status, ToolStatus::Failed);
    assert!(!result.content.contains("secret"), "{}", result.content);
}

/// The metadata endpoint, named as such rather than lumped in with "private".
#[tokio::test]
async fn the_cloud_metadata_endpoint_is_refused_by_name() {
    let result =
        http_tool_call("http://169.254.169.254/latest/meta-data/iam/security-credentials/").await;

    assert_eq!(result.status, ToolStatus::Failed);
    assert!(
        result.content.contains("metadata"),
        "the refusal should say what it refused: {}",
        result.content
    );
}

/// The bypass a URL check alone would leave open: the first hop is fine and
/// the `Location` header is not.
#[tokio::test]
async fn a_redirect_into_the_private_network_is_not_followed() {
    let secret = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecret").await;
    let redirector = Box::leak(
        format!(
            "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{}/\r\nContent-Length: 0\r\n\r\n",
            secret.port()
        )
        .into_boxed_str(),
    );
    let hop = serve_once(redirector).await;

    // `fetch_capped` skips the check on the *requested* URL, so the first hop
    // succeeds and this test is about the second one and nothing else. The
    // client is still `tool_client`, so the redirect policy is live.
    let error = fetch_capped(
        tool_client(),
        &format!("http://{hop}/"),
        64 * 1024,
        Duration::from_secs(5),
    )
    .await
    .expect_err("the redirect target is not reachable");

    assert!(
        !error.to_string().contains("secret"),
        "the redirect target must not have been fetched: {error}"
    );
    // Names the target rather than the hop, which is what distinguishes "the
    // redirect guard fired" from "the first request failed".
    assert!(
        error.to_string().contains(&secret.ip().to_string()),
        "the refusal should name the redirect target: {error}"
    );
    assert!(error.is_egress_refusal(), "{error:?}");
}

/// The control for the test above: the same first hop, redirecting somewhere
/// the policy allows, is followed. Without this, a client that refused every
/// redirect would pass.
#[tokio::test]
async fn a_redirect_the_policy_allows_is_still_followed() {
    let destination = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello").await;
    let redirector = Box::leak(
        format!(
            "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{}/\r\nContent-Length: 0\r\n\r\n",
            destination.port()
        )
        .into_boxed_str(),
    );
    let hop = serve_once(redirector).await;

    // An unguarded client, so the *only* difference from the test above is
    // whether the policy is in the way.
    let plain = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("a default client builds");
    let fetched = fetch_capped(
        &plain,
        &format!("http://{hop}/"),
        64 * 1024,
        Duration::from_secs(5),
    )
    .await
    .expect("an ordinary redirect is followed");

    assert_eq!(fetched.body, "hello");
    assert_eq!(
        fetched.final_url.as_ref(),
        &format!("http://{destination}/")
    );
}

/// A scheme that is not the web reaches nothing at all, and says so before any
/// connection is attempted.
#[tokio::test]
async fn a_file_url_is_refused_as_a_scheme_rather_than_read() {
    let error = fetch_bounded(
        tool_client(),
        "file:///etc/passwd",
        64 * 1024,
        Duration::from_secs(5),
    )
    .await
    .expect_err("not a web URL");
    assert!(
        matches!(error, FetchError::Blocked(Blocked::Scheme { .. })),
        "{error:?}"
    );
}

/// The bound is on what is *read*, not on what the sender claims. A response
/// that lies about its length in either direction is still cut off at the cap.
#[tokio::test]
async fn a_body_is_cut_off_at_the_cap_whatever_the_headers_say() {
    // Deliberately no `Content-Length` and a body far past the cap: chunked or
    // length-less responses are exactly the case a header-based check misses.
    let body = "x".repeat(200_000);
    let response =
        Box::leak(format!("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{body}").into_boxed_str());
    let address = serve_once(response).await;

    // The unguarded client, because this test is about the size bound and the
    // listener is on loopback. The destination policy has its own tests above.
    let plain = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("a default client builds");
    let fetched = fetch_capped(
        &plain,
        &format!("http://{address}/"),
        1_024,
        Duration::from_secs(5),
    )
    .await
    .expect("the fetch succeeds, bounded");

    assert_eq!(fetched.body.len(), 1_024);
    assert!(fetched.truncated);
}

/// The control. Without it, every assertion above could be satisfied by a
/// client that never works.
#[tokio::test]
async fn an_allowed_destination_still_gets_through() {
    let address = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello").await;
    let plain = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("a default client builds");
    let fetched = fetch_capped(
        &plain,
        &format!("http://{address}/"),
        64 * 1024,
        Duration::from_secs(5),
    )
    .await
    .expect("an ordinary fetch succeeds");

    assert_eq!(fetched.status.as_u16(), 200);
    assert_eq!(fetched.body, "hello");
    assert!(!fetched.truncated);
}

/// AF-038: deprecated IPv6 site-local space is an internal destination both
/// when written literally and after a hostname resolves to it.
#[test]
fn ipv6_site_local_is_refused_by_literal_and_resolution() {
    let literal = reqwest::Url::parse("http://[fec0::1]/secret").expect("valid literal URL");
    assert!(
        agentos_core::egress::check_url(&literal).is_err(),
        "a literal site-local destination must be refused before DNS"
    );

    for resolved in [
        Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 1),
        Ipv6Addr::new(0xfeff, 0xffff, 0, 0, 0, 0, 0, 1),
    ] {
        assert!(
            agentos_core::egress::check_address(IpAddr::V6(resolved)).is_err(),
            "the resolver's address policy must drop {resolved}"
        );
    }
}

/// Counts how many times it is asked, and always answers with one address.
struct CountingResolver {
    answer: SocketAddr,
    calls: Arc<AtomicUsize>,
}

impl Resolve for CountingResolver {
    fn resolve(&self, _name: Name) -> Resolving {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let answer = self.answer;
        Box::pin(async move { Ok(Box::new(std::iter::once(answer)) as Addrs) })
    }
}

/// DNS rebinding, as the property that actually defeats it.
///
/// A rebind needs *two* lookups: one the check sees, and a second, between the
/// check and the connect, that the attacker's short-TTL record answers
/// differently. Filtering inside the resolver removes the second — the address
/// the policy judged is the address the connection uses.
///
/// A real rebinding fixture would need a DNS server that lies on the second
/// query. This asserts the thing that makes such a server useless: one lookup
/// per request, and the connection lands on exactly what it returned.
#[tokio::test]
async fn a_name_is_resolved_once_and_the_connection_uses_that_answer() {
    let listener = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nlanded").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let client = reqwest::Client::builder()
        // As `tool_client` does, and for the same reason: a proxy would
        // resolve the name itself and the resolver below would never be
        // consulted.
        .no_proxy()
        .dns_resolver(Arc::new(CountingResolver {
            answer: listener,
            calls: Arc::clone(&calls),
        }))
        .build()
        .expect("a client with a custom resolver builds");

    // A name the resolver will be asked about, pointed at the listener. If
    // anything re-resolved between the decision and the connection, the count
    // would be higher — and that gap is the whole attack.
    let fetched = fetch_capped(
        &client,
        &format!("http://agentos-rebind.invalid:{}/", listener.port()),
        64 * 1024,
        Duration::from_secs(5),
    )
    .await
    .expect("the resolver's answer is reachable");

    assert_eq!(fetched.body, "landed");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a second lookup between the check and the connect is what a rebind needs"
    );
}
