//! One bounded GET.
//!
//! M4 / `NET-001`. `HttpTool` called `response.text().await`, which reads the
//! whole body into memory with no ceiling — a `Content-Length` of four
//! gigabytes, or a chunked response that never ends, was an out-of-memory kill
//! for the gateway and every conversation on it. Nothing here is about
//! authorization; [`super::policy`] handles that. This is about not letting
//! the far end decide how much memory the runtime spends.

use super::policy::{check_url, Blocked};
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::timeout;

/// Redirect hops a tool request may follow.
///
/// Enough for the ordinary `http → https → www` chain and the shortener that
/// follows it; short enough that a redirect loop ends in an error rather than
/// in `reqwest`'s own default of ten round trips.
pub const MAX_REDIRECTS: usize = 5;

#[derive(Debug, Error)]
pub enum FetchError {
    #[error(transparent)]
    Blocked(#[from] Blocked),
    #[error("{0} is not a URL")]
    NotAUrl(String),
    #[error("the request to {url} failed: {}", chain(source))]
    Request {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("{url} did not answer within {timeout_ms} ms")]
    TimedOut { url: String, timeout_ms: u64 },
}

/// Every message in an error's source chain, joined.
///
/// `reqwest`'s own `Display` for a connect failure is "error sending request
/// for url (…)", which says nothing about *why*. The reason a guarded request
/// failed — "169.254.169.254 is a cloud metadata endpoint" — is a
/// [`Blocked`] several links down the chain, and it is the only part worth
/// reading.
fn chain(error: &reqwest::Error) -> String {
    let mut parts = vec![error.to_string()];
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        parts.push(cause.to_string());
        source = cause.source();
    }
    parts.join(": ")
}

/// A response, as much of it as was allowed.
#[derive(Clone, Debug)]
pub struct Fetched {
    pub status: StatusCode,
    /// The URL the response actually came from, which is not the requested one
    /// when redirects were followed.
    pub final_url: Arc<str>,
    pub body: String,
    /// Whether the body was cut off at the cap.
    pub truncated: bool,
}

/// GET `url`, refusing what the policy refuses and keeping at most
/// `max_bytes`.
///
/// Checks the two things a resolver never sees: the scheme, because a `file:`
/// URL reaches no DNS, and a host written as a literal address, because
/// reqwest connects straight to one. Everything else — names, and every hop of
/// a redirect chain — is judged inside the client's resolver
/// ([`super::resolver::GuardedResolver`]), which is what makes the address
/// checked the address connected to.
pub async fn fetch_bounded(
    client: &Client,
    url: &str,
    max_bytes: usize,
    deadline: Duration,
) -> Result<Fetched, FetchError> {
    let parsed = reqwest::Url::parse(url).map_err(|_| FetchError::NotAUrl(url.to_owned()))?;
    check_url(&parsed)?;
    fetch_capped(client, url, max_bytes, deadline).await
}

/// GET `url` under a deadline, keeping at most `max_bytes`, with **no**
/// destination policy.
///
/// For an endpoint the *operator* configured — a Qdrant on loopback, a local
/// model server — where the address in `agent.toml` is itself the decision.
/// A destination the *model* chose goes through [`fetch_bounded`] instead;
/// the difference is a trust question, not a technical one, and this function
/// exists so that answering it is a choice a caller makes rather than one it
/// makes by accident.
pub async fn fetch_capped(
    client: &Client,
    url: &str,
    max_bytes: usize,
    deadline: Duration,
) -> Result<Fetched, FetchError> {
    let parsed = reqwest::Url::parse(url).map_err(|_| FetchError::NotAUrl(url.to_owned()))?;

    let fetched = timeout(deadline, async {
        let response =
            client
                .get(parsed.clone())
                .send()
                .await
                .map_err(|source| FetchError::Request {
                    url: url.to_owned(),
                    source,
                })?;
        let status = response.status();
        let final_url = Arc::from(response.url().as_str());
        let (body, truncated) = read_capped(response, max_bytes, url).await?;
        Ok::<_, FetchError>(Fetched {
            status,
            final_url,
            body,
            truncated,
        })
    })
    .await;

    match fetched {
        Ok(result) => result,
        Err(_elapsed) => Err(FetchError::TimedOut {
            url: url.to_owned(),
            timeout_ms: deadline.as_millis() as u64,
        }),
    }
}

/// Read a response chunk by chunk, stopping at the cap.
///
/// Streamed rather than `.text()`: the point is to never hold more than
/// `max_bytes` at once, and `Content-Length` is a claim the sender makes, not
/// a bound the sender is held to. Stopping mid-body simply drops the
/// connection, which is the correct thing to do to a peer that is sending more
/// than was asked for.
async fn read_capped(
    mut response: reqwest::Response,
    max_bytes: usize,
    url: &str,
) -> Result<(String, bool), FetchError> {
    let mut body = Vec::new();
    let mut truncated = false;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|source| FetchError::Request {
            url: url.to_owned(),
            source,
        })?
    {
        let room = max_bytes.saturating_sub(body.len());
        if chunk.len() >= room {
            body.extend_from_slice(&chunk[..room]);
            truncated = true;
            break;
        }
        body.extend_from_slice(&chunk);
    }
    // Lossy rather than an error: a cap can land mid-character, and a body
    // that is 99% readable text is more use to the model than a decode
    // failure.
    Ok((String::from_utf8_lossy(&body).into_owned(), truncated))
}

impl FetchError {
    /// Whether this failure was the egress policy refusing a destination,
    /// rather than the destination failing to answer.
    ///
    /// A refusal can arrive two ways: directly, from the checks in
    /// [`fetch_bounded`], or wrapped several layers deep in a `reqwest`
    /// connect error when [`super::resolver::GuardedResolver`] declined to
    /// hand back an address. The caller wants to report those the same way, so
    /// it has to be able to recognise the second.
    pub fn is_egress_refusal(&self) -> bool {
        match self {
            Self::Blocked(_) | Self::NotAUrl(_) => true,
            Self::Request { source, .. } => {
                let mut cause = std::error::Error::source(source);
                while let Some(error) = cause {
                    if error.downcast_ref::<Blocked>().is_some() {
                        return true;
                    }
                    cause = error.source();
                }
                false
            }
            Self::TimedOut { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> Client {
        Client::builder()
            .no_proxy()
            .build()
            .expect("a default client builds")
    }

    #[tokio::test]
    async fn a_scheme_that_is_not_the_web_is_refused_before_anything_is_opened() {
        for url in [
            "file:///etc/passwd",
            "ftp://example.com/x",
            "gopher://example.com/",
        ] {
            let error = fetch_bounded(&client(), url, 1024, Duration::from_secs(1))
                .await
                .expect_err("not a web URL");
            assert!(
                matches!(error, FetchError::Blocked(Blocked::Scheme { .. })),
                "{url}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn something_that_is_not_a_url_at_all_is_a_typed_error() {
        let error = fetch_bounded(&client(), "not a url", 1024, Duration::from_secs(1))
            .await
            .expect_err("not a URL");
        assert!(matches!(error, FetchError::NotAUrl(_)), "{error:?}");
    }
}
