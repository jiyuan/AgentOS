//! Async TLS WebSocket transport for the Feishu long-connection channel.
//!
//! Uses `tokio-tungstenite` with `rustls-tls-webpki-roots` so cert validation
//! works without a system trust store (i.e. inside `distroless` / scratch
//! containers). Replaces the earlier `openssl s_client` subprocess and its
//! hand-rolled HTTP/1.1 upgrade.
//!
//! `HTTPS_PROXY` / `https_proxy` are honored for `http://`-scheme proxies via
//! HTTP CONNECT tunneling. Other schemes (`https://`, `socks5://`) are not
//! supported in the rustls path; the transport logs a warning and connects
//! directly so users see their proxy is being ignored instead of silently
//! failing closed.

use super::limits::MAX_FRAME_BYTES;
use agentos_interfaces::ChannelError;
use futures_util::{SinkExt, StreamExt};
use std::env;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::Request;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{
    client_async_tls_with_config, client_async_with_config, connect_async_with_config,
    MaybeTlsStream, WebSocketStream,
};
use tracing::warn;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(super) struct WebSocketConnection {
    stream: WsStream,
}

impl WebSocketConnection {
    pub(super) async fn connect(url: &str) -> Result<Self, ChannelError> {
        let target = TargetUrl::parse(url)?;
        let proxy = https_proxy_for_target(&target.host);

        let stream = match proxy {
            Some(proxy) => connect_via_proxy(&proxy, &target).await?,
            None => connect_direct(url).await?,
        };

        Ok(Self { stream })
    }

    pub(super) async fn read_frame(&mut self) -> Result<Vec<u8>, ChannelError> {
        loop {
            match self.stream.next().await {
                Some(Ok(Message::Binary(payload))) => return Ok(payload),
                Some(Ok(Message::Text(text))) => return Ok(text.into_bytes()),
                Some(Ok(Message::Ping(payload))) => {
                    if let Err(err) = self.stream.send(Message::Pong(payload)).await {
                        return Err(ChannelError::Backend(Arc::from(format!(
                            "Feishu WebSocket pong send failed: {err}"
                        ))));
                    }
                }
                Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                Some(Ok(Message::Close(_))) => {
                    return Err(ChannelError::Backend(Arc::from(
                        "Feishu WebSocket closed by server",
                    )));
                }
                Some(Err(err)) => {
                    return Err(ChannelError::Backend(Arc::from(format!(
                        "Feishu WebSocket read failed: {err}"
                    ))));
                }
                None => {
                    return Err(ChannelError::Backend(Arc::from(
                        "Feishu WebSocket stream ended",
                    )));
                }
            }
        }
    }

    pub(super) async fn write_frame(&mut self, payload: &[u8]) -> Result<(), ChannelError> {
        self.stream
            .send(Message::Binary(payload.to_vec()))
            .await
            .map_err(|err| {
                ChannelError::Backend(Arc::from(format!("Feishu WebSocket write failed: {err}")))
            })
    }
}

async fn connect_direct(url: &str) -> Result<WsStream, ChannelError> {
    let request: Request<()> = url.into_client_request().map_err(|err| {
        ChannelError::Backend(Arc::from(format!(
            "Feishu WebSocket URL is not a valid request: {err}"
        )))
    })?;
    let (stream, _response) = connect_async_with_config(request, Some(frame_config()), false)
        .await
        .map_err(|err| {
            ChannelError::Backend(Arc::from(format!(
                "Feishu WebSocket direct connect failed: {err}"
            )))
        })?;
    Ok(stream)
}

async fn connect_via_proxy(proxy: &Proxy, target: &TargetUrl) -> Result<WsStream, ChannelError> {
    let mut tcp = TcpStream::connect((proxy.host.as_str(), proxy.port))
        .await
        .map_err(|err| {
            ChannelError::Backend(Arc::from(format!(
                "Feishu proxy connect ({}:{}) failed: {err}",
                proxy.host, proxy.port
            )))
        })?;

    let connect_line = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n",
        host = target.host,
        port = target.port,
    );
    tcp.write_all(connect_line.as_bytes())
        .await
        .map_err(|err| {
            ChannelError::Backend(Arc::from(format!(
                "Feishu proxy CONNECT write failed: {err}"
            )))
        })?;

    let mut reader = BufReader::new(tcp);
    let mut status = String::new();
    reader.read_line(&mut status).await.map_err(|err| {
        ChannelError::Backend(Arc::from(format!(
            "Feishu proxy CONNECT response read failed: {err}"
        )))
    })?;
    if !status.contains(" 200 ") {
        return Err(ChannelError::Backend(Arc::from(format!(
            "Feishu proxy CONNECT rejected: {}",
            status.trim()
        ))));
    }
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await.map_err(|err| {
            ChannelError::Backend(Arc::from(format!(
                "Feishu proxy CONNECT header read failed: {err}"
            )))
        })?;
        if line == "\r\n" || line == "\n" || line.is_empty() {
            break;
        }
    }
    let tunneled = reader.into_inner();

    let request: Request<()> =
        target
            .original_url
            .as_str()
            .into_client_request()
            .map_err(|err| {
                ChannelError::Backend(Arc::from(format!(
                    "Feishu WebSocket URL is not a valid request: {err}"
                )))
            })?;
    let (stream, _response) = if target.tls {
        client_async_tls_with_config(request, tunneled, Some(frame_config()), None).await
    } else {
        client_async_with_config(
            request,
            MaybeTlsStream::Plain(tunneled),
            Some(frame_config()),
        )
        .await
    }
    .map_err(|err| {
        ChannelError::Backend(Arc::from(format!(
            "Feishu WebSocket tunnelled handshake failed: {err}"
        )))
    })?;
    Ok(stream)
}

fn frame_config() -> WebSocketConfig {
    WebSocketConfig {
        max_message_size: Some(MAX_FRAME_BYTES),
        max_frame_size: Some(MAX_FRAME_BYTES),
        ..WebSocketConfig::default()
    }
}

#[derive(Clone, Debug)]
struct TargetUrl {
    host: String,
    port: u16,
    /// Whether the endpoint uses TLS (`wss://`). Plain `ws://` is accepted so
    /// the channel can talk to a local mock long-connection server in tests.
    tls: bool,
    original_url: String,
}

impl TargetUrl {
    fn parse(url: &str) -> Result<Self, ChannelError> {
        let (rest, tls) = if let Some(rest) = url.strip_prefix("wss://") {
            (rest, true)
        } else if let Some(rest) = url.strip_prefix("ws://") {
            (rest, false)
        } else {
            return Err(ChannelError::Backend(Arc::from(
                "Feishu WebSocket URL must use ws:// or wss://",
            )));
        };
        let (authority, _path) = rest.split_once('/').unwrap_or((rest, ""));
        if authority.is_empty() {
            return Err(ChannelError::Backend(Arc::from(
                "Feishu WebSocket URL is missing a host",
            )));
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => {
                let parsed = port.parse::<u16>().map_err(|err| {
                    ChannelError::Backend(Arc::from(format!("invalid WebSocket port: {err}")))
                })?;
                (host.to_owned(), parsed)
            }
            None => (authority.to_owned(), if tls { 443 } else { 80 }),
        };
        Ok(Self {
            host,
            port,
            tls,
            original_url: url.to_owned(),
        })
    }
}

#[derive(Clone, Debug)]
struct Proxy {
    host: String,
    port: u16,
}

/// Resolve the effective HTTP proxy for the Feishu long connection.
///
/// Reads `HTTPS_PROXY` / `https_proxy` (and `NO_PROXY` / `no_proxy`).
/// `http://` and bare `host:port` values are accepted as HTTP CONNECT proxies;
/// `https://`, `socks5://`, and `socks5h://` are not supported in the rustls
/// path and produce a `tracing::warn!` instead of being silently dropped, so
/// operators notice their configured proxy is being ignored.
fn https_proxy_for_target(host: &str) -> Option<Proxy> {
    if no_proxy_matches(host) {
        return None;
    }
    let raw = env::var("HTTPS_PROXY")
        .or_else(|_| env::var("https_proxy"))
        .ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (scheme, body) = match trimmed.split_once("://") {
        Some((scheme, body)) => (Some(scheme.to_ascii_lowercase()), body),
        None => (None, trimmed),
    };
    match scheme.as_deref() {
        None | Some("http") => parse_proxy_authority(body, &raw),
        Some("https") => {
            warn!(
                proxy = %raw,
                "https:// proxy schemes are not supported in the rustls Feishu transport; \
                 connecting directly. Switch to an http:// proxy or unset HTTPS_PROXY."
            );
            None
        }
        Some("socks5") | Some("socks5h") | Some("socks4") | Some("socks4a") => {
            warn!(
                proxy = %raw,
                "SOCKS proxies are not supported in the rustls Feishu transport; \
                 connecting directly."
            );
            None
        }
        Some(other) => {
            warn!(
                proxy = %raw,
                scheme = %other,
                "unrecognised HTTPS_PROXY scheme; connecting directly."
            );
            None
        }
    }
}

fn parse_proxy_authority(body: &str, raw: &str) -> Option<Proxy> {
    // Strip a userinfo prefix (`user:pass@…`) and a trailing path / query.
    let after_userinfo = body.rsplit_once('@').map_or(body, |(_, host)| host);
    let host_port = after_userinfo
        .split('/')
        .next()
        .unwrap_or(after_userinfo)
        .trim();
    if host_port.is_empty() {
        warn!(proxy = %raw, "HTTPS_PROXY contained no host; connecting directly.");
        return None;
    }
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => {
            let parsed = match port.parse::<u16>() {
                Ok(value) => value,
                Err(_) => {
                    warn!(
                        proxy = %raw,
                        "HTTPS_PROXY port is not a number; connecting directly."
                    );
                    return None;
                }
            };
            (host.to_owned(), parsed)
        }
        None => (host_port.to_owned(), 80),
    };
    if host.is_empty() {
        warn!(proxy = %raw, "HTTPS_PROXY host is empty; connecting directly.");
        return None;
    }
    Some(Proxy { host, port })
}

fn no_proxy_matches(host: &str) -> bool {
    env::var("NO_PROXY")
        .or_else(|_| env::var("no_proxy"))
        .ok()
        .is_some_and(|no_proxy| {
            no_proxy
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .any(|entry| no_proxy_entry_matches(host, entry))
        })
}

fn no_proxy_entry_matches(host: &str, entry: &str) -> bool {
    entry == "*"
        || host.eq_ignore_ascii_case(entry)
        || entry
            .strip_prefix('.')
            .is_some_and(|suffix| host.ends_with(suffix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(values: &[(&str, Option<&str>)], body: F) {
        let _guard = ENV_LOCK.lock().unwrap();
        let saved: Vec<(String, Option<String>)> = values
            .iter()
            .map(|(key, _)| ((*key).to_owned(), env::var(key).ok()))
            .collect();
        for (key, value) in values {
            match value {
                Some(v) => env::set_var(key, v),
                None => env::remove_var(key),
            }
        }
        body();
        for (key, original) in saved {
            match original {
                Some(v) => env::set_var(&key, v),
                None => env::remove_var(&key),
            }
        }
    }

    #[test]
    fn target_url_parses_host_and_default_port() {
        let parsed = TargetUrl::parse("wss://example.com/path?x=1").unwrap();
        assert_eq!(parsed.host, "example.com");
        assert_eq!(parsed.port, 443);
    }

    #[test]
    fn target_url_parses_explicit_port() {
        let parsed = TargetUrl::parse("wss://example.com:8443/").unwrap();
        assert_eq!(parsed.port, 8443);
    }

    #[test]
    fn target_url_accepts_plain_ws_for_mocks() {
        let parsed = TargetUrl::parse("ws://127.0.0.1:9001/feishu").unwrap();
        assert_eq!(parsed.host, "127.0.0.1");
        assert_eq!(parsed.port, 9001);
        assert!(!parsed.tls);
    }

    #[test]
    fn target_url_defaults_ws_port_to_80() {
        let parsed = TargetUrl::parse("ws://example.com/path").unwrap();
        assert_eq!(parsed.port, 80);
        assert!(!parsed.tls);
    }

    #[test]
    fn target_url_rejects_non_ws_scheme() {
        assert!(TargetUrl::parse("https://example.com").is_err());
    }

    #[tokio::test]
    async fn an_oversized_websocket_frame_is_rejected_by_the_transport() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback binds");
        let address = listener.local_addr().expect("listener has an address");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client connects");
            let mut socket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("server handshake succeeds");
            let _ = socket
                .send(Message::Binary(vec![0; MAX_FRAME_BYTES + 1]))
                .await;
        });

        let stream = connect_direct(&format!("ws://{address}/"))
            .await
            .expect("client handshake succeeds");
        let error = WebSocketConnection { stream }
            .read_frame()
            .await
            .expect_err("the frame ceiling fires before a payload reaches channel code");
        assert!(error.to_string().contains("limit exceeded"), "{error}");
    }

    #[test]
    fn proxy_accepts_http_scheme() {
        with_env(
            &[
                ("HTTPS_PROXY", Some("http://corp:3128")),
                ("https_proxy", None),
                ("NO_PROXY", None),
                ("no_proxy", None),
            ],
            || {
                let proxy = https_proxy_for_target("example.com").unwrap();
                assert_eq!(proxy.host, "corp");
                assert_eq!(proxy.port, 3128);
            },
        );
    }

    #[test]
    fn proxy_accepts_bare_host_port() {
        with_env(
            &[
                ("HTTPS_PROXY", Some("corp:3128")),
                ("https_proxy", None),
                ("NO_PROXY", None),
                ("no_proxy", None),
            ],
            || {
                let proxy = https_proxy_for_target("example.com").unwrap();
                assert_eq!(proxy.host, "corp");
                assert_eq!(proxy.port, 3128);
            },
        );
    }

    #[test]
    fn proxy_strips_userinfo() {
        with_env(
            &[
                ("HTTPS_PROXY", Some("http://user:pass@corp:3128/")),
                ("https_proxy", None),
                ("NO_PROXY", None),
                ("no_proxy", None),
            ],
            || {
                let proxy = https_proxy_for_target("example.com").unwrap();
                assert_eq!(proxy.host, "corp");
                assert_eq!(proxy.port, 3128);
            },
        );
    }

    #[test]
    fn proxy_rejects_https_scheme_with_warning() {
        // We can't intercept tracing here; just verify the function returns
        // None instead of silently treating https:// as a CONNECT proxy.
        with_env(
            &[
                ("HTTPS_PROXY", Some("https://corp:3128")),
                ("https_proxy", None),
                ("NO_PROXY", None),
                ("no_proxy", None),
            ],
            || {
                assert!(https_proxy_for_target("example.com").is_none());
            },
        );
    }

    #[test]
    fn proxy_rejects_socks_scheme() {
        with_env(
            &[
                ("HTTPS_PROXY", Some("socks5://corp:1080")),
                ("https_proxy", None),
                ("NO_PROXY", None),
                ("no_proxy", None),
            ],
            || {
                assert!(https_proxy_for_target("example.com").is_none());
            },
        );
    }

    #[test]
    fn proxy_honours_no_proxy() {
        with_env(
            &[
                ("HTTPS_PROXY", Some("http://corp:3128")),
                ("https_proxy", None),
                ("NO_PROXY", Some("example.com,.internal")),
                ("no_proxy", None),
            ],
            || {
                assert!(https_proxy_for_target("example.com").is_none());
                assert!(https_proxy_for_target("foo.internal").is_none());
                assert!(https_proxy_for_target("other.com").is_some());
            },
        );
    }
}
