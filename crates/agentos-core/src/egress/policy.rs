//! Which addresses a tool-driven request is allowed to reach.
//!
//! M4 / `NET-001`. The `http` tool checked that a URL started with `http://`
//! or `https://` and then fetched it. A model that has read an untrusted web
//! page — or simply been asked nicely — could therefore fetch
//! `http://169.254.169.254/latest/meta-data/iam/security-credentials/`, or any
//! service on the loopback interface, or anything on the operator's private
//! network that happens to answer HTTP. That is server-side request forgery
//! with the runtime as the confused deputy, and the sandbox does not touch it:
//! Landlock and Seatbelt bound filesystem writes.
//!
//! # Deny by address, not by name
//!
//! A hostname is not a destination. `localtest.me` resolves to `127.0.0.1`;
//! any attacker-controlled domain can resolve to anything at all. The decision
//! therefore happens on the resolved [`IpAddr`], after resolution and before
//! the connection, which is also what makes DNS rebinding a non-issue: there
//! is no second lookup between the check and the connect for an attacker to
//! win.
//!
//! # What counts as private
//!
//! Everything that is not globally routable unicast, plus the cloud metadata
//! addresses, which *are* globally routable in the sense the IANA registries
//! care about and are the single highest-value SSRF target in existence.
//! Refusing the whole non-global space rather than enumerating RFC1918 covers
//! loopback, link-local, CGNAT, benchmarking, documentation, multicast,
//! IPv4-mapped IPv6, unique-local IPv6 and the rest without a list that has to
//! be kept in step with IANA.

use reqwest::Url;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use thiserror::Error;

/// The IMDS address on AWS, GCP, Azure, DigitalOcean, and Oracle Cloud.
/// Globally routable, and never a destination a tool should reach.
const IPV4_METADATA: Ipv4Addr = Ipv4Addr::new(169, 254, 169, 254);

/// The IPv6 form of the same service on GCP and Azure.
const IPV6_METADATA: Ipv6Addr = Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254);

/// Why an address was refused.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum Blocked {
    #[error("{0} is a cloud metadata endpoint")]
    Metadata(IpAddr),
    #[error("{0} is not a globally routable address")]
    NotGlobal(IpAddr),
    #[error("{scheme}:// is not a scheme tools may request")]
    Scheme { scheme: String },
    #[error("{host} resolves only to addresses tools may not reach: {reason}")]
    HostUnreachable { host: String, reason: String },
    #[error("could not resolve {host}: {reason}")]
    Unresolvable { host: String, reason: String },
}

/// Schemes a tool may request. Anything else — `file:`, `gopher:`, `ftp:`,
/// `dict:` — is a way to reach something that is not a web server, and none of
/// them has a use here that `http` does not already cover.
pub const ALLOWED_SCHEMES: &[&str] = &["http", "https"];

/// Whether `scheme` is one a tool may request.
pub fn check_scheme(scheme: &str) -> Result<(), Blocked> {
    if ALLOWED_SCHEMES.contains(&scheme) {
        return Ok(());
    }
    Err(Blocked::Scheme {
        scheme: scheme.to_owned(),
    })
}

/// Whether a tool-driven request may be sent to `url` at all.
///
/// Two checks a resolver cannot make. A `file:` URL never reaches DNS, and a
/// URL whose host is a *literal* address never reaches DNS either — reqwest
/// connects straight to it. The literal case is how a redirect would otherwise
/// walk around [`super::resolver::GuardedResolver`]: `Location:
/// http://127.0.0.1:8080/` needs no name to resolve.
pub fn check_url(url: &Url) -> Result<(), Blocked> {
    check_scheme(url.scheme())?;
    let Some(host) = url.host_str() else {
        // No host at all; the request fails on its own terms.
        return Ok(());
    };
    // `host_str` keeps the brackets an IPv6 literal is written with.
    let literal = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(host);
    match literal.parse::<IpAddr>() {
        Ok(address) => check_address(address),
        // A name, which the resolver will judge.
        Err(_) => Ok(()),
    }
}

/// Whether a tool-driven request may connect to `address`.
///
/// The metadata check comes first so its error names what was actually being
/// attempted; `169.254.169.254` is link-local and would otherwise be reported
/// as merely non-global, which tells an operator reading a log much less.
pub fn check_address(address: IpAddr) -> Result<(), Blocked> {
    if is_metadata(address) {
        return Err(Blocked::Metadata(address));
    }
    if is_globally_routable(address) {
        return Ok(());
    }
    Err(Blocked::NotGlobal(address))
}

fn is_metadata(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => v4 == IPV4_METADATA,
        IpAddr::V6(v6) => {
            v6 == IPV6_METADATA
                // An IPv4-mapped metadata address is the same destination
                // wearing a different notation.
                || v6.to_ipv4_mapped() == Some(IPV4_METADATA)
        }
    }
}

/// Whether `address` is ordinary public unicast.
///
/// Written out rather than using `IpAddr::is_global`, which is still unstable.
/// The shape is deliberately "list what is *not* allowed and default to
/// allowed at the end", because the alternative — enumerating the public
/// space — is not expressible.
fn is_globally_routable(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => is_global_v4(v4),
        IpAddr::V6(v6) => {
            // A v4 address written as v6 is a v4 address. Judging it by IPv6
            // rules would let `::ffff:127.0.0.1` through.
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_global_v4(mapped);
            }
            // `::/96` — the deprecated IPv4-compatible form, and also where
            // `::` and `::1` live. Refused wholesale rather than translated to
            // v4: `Ipv6Addr::to_ipv4` reads `::1` as `0.0.0.1`, which is an
            // ordinary-looking public address and exactly how loopback would
            // otherwise slip through.
            if v6.segments()[..6] == [0, 0, 0, 0, 0, 0] {
                return false;
            }
            is_global_v6(v6)
        }
    }
}

fn is_global_v4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    !(address.is_loopback()
        || address.is_private()
        || address.is_link_local()
        || address.is_broadcast()
        || address.is_documentation()
        || address.is_unspecified()
        || address.is_multicast()
        // 100.64.0.0/10, carrier-grade NAT: the operator's own network on
        // plenty of hosted infrastructure.
        || (a == 100 && (64..128).contains(&b))
        // 192.0.0.0/24, IETF protocol assignments.
        || (a == 192 && b == 0 && c == 0)
        // 198.18.0.0/15, benchmarking.
        || (a == 198 && (b == 18 || b == 19))
        // 240.0.0.0/4, reserved, plus 255.255.255.255 which `is_broadcast`
        // already covers.
        || a >= 240)
}

fn is_global_v6(address: Ipv6Addr) -> bool {
    let first = address.segments()[0];
    !(address.is_loopback()
        || address.is_unspecified()
        || address.is_multicast()
        // fc00::/7, unique local.
        || (first & 0xfe00) == 0xfc00
        // fe80::/10, link local.
        || (first & 0xffc0) == 0xfe80
        // 2001:db8::/32, documentation.
        || (first == 0x2001 && address.segments()[1] == 0x0db8)
        // 100::/64, discard-only.
        || (first == 0x0100 && address.segments()[1..4] == [0, 0, 0]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn blocked(address: &str) -> Blocked {
        check_address(IpAddr::from_str(address).expect("a literal address"))
            .expect_err("must be refused")
    }

    fn allowed(address: &str) {
        check_address(IpAddr::from_str(address).expect("a literal address"))
            .expect("ordinary public address");
    }

    #[test]
    fn an_ordinary_public_address_is_reachable() {
        allowed("93.184.216.34");
        allowed("1.1.1.1");
        allowed("2606:4700:4700::1111");
    }

    /// The highest-value SSRF target there is, and the one that is *not*
    /// caught by "block RFC1918".
    #[test]
    fn the_cloud_metadata_endpoint_is_named_in_its_own_refusal() {
        assert!(matches!(blocked("169.254.169.254"), Blocked::Metadata(_)));
        assert!(matches!(blocked("fd00:ec2::254"), Blocked::Metadata(_)));
        // The same destination in IPv4-mapped notation.
        assert!(matches!(
            blocked("::ffff:169.254.169.254"),
            Blocked::Metadata(_)
        ));
    }

    /// `Ipv6Addr::to_ipv4` reads `::1` as `0.0.0.1`, so a v6-first
    /// implementation that translates before it checks lets loopback through.
    /// This is the shape that bug takes.
    #[test]
    fn the_deprecated_v4_compatible_block_is_refused_wholesale() {
        for address in ["::1", "::", "::0.0.0.1", "::93.184.216.34"] {
            assert!(
                check_address(IpAddr::from_str(address).expect("literal")).is_err(),
                "{address} must not be reachable"
            );
        }
    }

    #[test]
    fn loopback_is_refused_in_both_families() {
        assert!(matches!(blocked("127.0.0.1"), Blocked::NotGlobal(_)));
        assert!(matches!(blocked("127.9.9.9"), Blocked::NotGlobal(_)));
        assert!(matches!(blocked("::1"), Blocked::NotGlobal(_)));
    }

    /// The notation trick: an IPv6 address that is really a v4 loopback.
    /// Judging it by IPv6 rules alone would let it through.
    #[test]
    fn a_v4_address_written_as_v6_is_judged_as_v4() {
        assert!(matches!(blocked("::ffff:127.0.0.1"), Blocked::NotGlobal(_)));
        assert!(matches!(blocked("::ffff:10.0.0.1"), Blocked::NotGlobal(_)));
        assert!(matches!(
            blocked("::ffff:192.168.1.1"),
            Blocked::NotGlobal(_)
        ));
        // And the public one still is.
        allowed("::ffff:93.184.216.34");
    }

    #[test]
    fn the_private_ranges_are_refused() {
        for address in [
            "10.0.0.1",
            "172.16.0.1",
            "172.31.255.254",
            "192.168.0.1",
            "169.254.1.1",
            "100.64.0.1",
            "198.18.0.1",
            "192.0.0.1",
            "0.0.0.0",
            "240.0.0.1",
            "255.255.255.255",
            "224.0.0.1",
            "fc00::1",
            "fe80::1",
            "::",
            "ff02::1",
        ] {
            assert!(
                check_address(IpAddr::from_str(address).expect("literal")).is_err(),
                "{address} must not be reachable"
            );
        }
    }

    /// `172.32.0.0` is one past the private block and public; the boundary is
    /// where an off-by-one would hide.
    #[test]
    fn the_boundaries_of_the_private_blocks_are_where_they_should_be() {
        allowed("172.32.0.1");
        allowed("11.0.0.1");
        allowed("100.128.0.1");
        allowed("198.20.0.1");
        assert!(check_address(IpAddr::from_str("172.16.0.0").expect("literal")).is_err());
        assert!(check_address(IpAddr::from_str("100.127.255.255").expect("literal")).is_err());
    }

    /// The redirect bypass: a `Location` header naming a literal address
    /// never touches DNS, so the resolver never sees it.
    #[test]
    fn a_url_naming_a_literal_private_address_is_refused() {
        for url in [
            "http://127.0.0.1:8080/admin",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]:9200/_cluster/health",
            "http://10.0.0.5/",
            "http://[::ffff:127.0.0.1]/",
        ] {
            let parsed = Url::parse(url).expect("a URL");
            assert!(check_url(&parsed).is_err(), "{url} must be refused");
        }
    }

    #[test]
    fn a_url_naming_a_public_literal_or_an_ordinary_name_is_allowed() {
        for url in [
            "https://example.com/x",
            "http://93.184.216.34/",
            "https://[2606:4700:4700::1111]/",
        ] {
            let parsed = Url::parse(url).expect("a URL");
            assert!(check_url(&parsed).is_ok(), "{url} should be allowed");
        }
    }

    #[test]
    fn only_http_and_https_are_requestable() {
        assert!(check_scheme("http").is_ok());
        assert!(check_scheme("https").is_ok());
        for scheme in ["file", "ftp", "gopher", "dict", "data", "ws"] {
            assert!(
                matches!(check_scheme(scheme), Err(Blocked::Scheme { .. })),
                "{scheme} must not be requestable"
            );
        }
    }
}
