//! DNS-aware webhook URL validation and connection-pinned egress controls.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use reqwest::{redirect::Policy, Client, Url};

use crate::WebhookError;

const MAX_RESOLVED_ADDRESSES: usize = 16;

/// Parses and validates a configured webhook URL.
pub fn validate_destination_url(
    raw: &str,
    allowed_hosts: &[String],
    allow_private_ips: bool,
    allow_http: bool,
) -> Result<Url, WebhookError> {
    let url = Url::parse(raw).map_err(|error| WebhookError::InvalidUrl(error.to_string()))?;
    match url.scheme() {
        "https" => {}
        "http" if allow_http => {}
        scheme => {
            return Err(WebhookError::InvalidUrl(format!(
                "scheme {scheme:?} is not permitted"
            )))
        }
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(WebhookError::InvalidUrl(
            "embedded credentials are not permitted".to_owned(),
        ));
    }
    if url.fragment().is_some() {
        return Err(WebhookError::InvalidUrl(
            "URL fragments are not permitted".to_owned(),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| WebhookError::InvalidUrl("host is required".to_owned()))?;
    let host = normalized_host(host);
    if !allowed_hosts.is_empty()
        && !allowed_hosts
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(host))
    {
        return Err(WebhookError::HostNotAllowed(host.to_owned()));
    }
    if !allow_private_ips && host.parse::<IpAddr>().is_ok_and(is_private_or_special) {
        return Err(WebhookError::InvalidUrl(
            "private or special destination address is not allowed".to_owned(),
        ));
    }
    Ok(url)
}

/// Enforces an explicit destination port allowlist.
pub fn validate_destination_port(url: &Url, allowed_ports: &[u16]) -> Result<(), WebhookError> {
    let port = url
        .port_or_known_default()
        .ok_or_else(|| WebhookError::InvalidUrl("destination port is required".to_owned()))?;
    let permitted = if allowed_ports.is_empty() {
        (url.scheme() == "https" && port == 443) || (url.scheme() == "http" && port == 80)
    } else {
        allowed_ports.contains(&port)
    };
    if !permitted {
        return Err(WebhookError::InvalidUrl(format!(
            "destination port {port} is not permitted"
        )));
    }
    Ok(())
}

/// Resolves, validates every address, and pins one client for exactly one attempt.
pub async fn pinned_client(
    url: &Url,
    allow_private_ips: bool,
    timeout: Duration,
) -> Result<Client, WebhookError> {
    let host = url
        .host_str()
        .ok_or_else(|| WebhookError::InvalidUrl("host is required".to_owned()))?;
    let host = normalized_host(host);
    let port = url
        .port_or_known_default()
        .ok_or_else(|| WebhookError::InvalidUrl("destination port is required".to_owned()))?;
    let mut addresses = Vec::new();
    if let Ok(address) = host.parse::<IpAddr>() {
        addresses.push(SocketAddr::new(address, port));
    } else {
        let resolved = tokio::net::lookup_host((host, port)).await.map_err(|_| {
            WebhookError::InvalidUrl("destination DNS resolution failed".to_owned())
        })?;
        for address in resolved {
            if addresses.len() == MAX_RESOLVED_ADDRESSES {
                return Err(WebhookError::InvalidUrl(
                    "destination resolves to too many addresses".to_owned(),
                ));
            }
            if !addresses.contains(&address) {
                addresses.push(address);
            }
        }
    }
    if addresses.is_empty() {
        return Err(WebhookError::InvalidUrl(
            "destination resolved to no addresses".to_owned(),
        ));
    }
    if !allow_private_ips
        && addresses
            .iter()
            .any(|address| is_private_or_special(address.ip()))
    {
        return Err(WebhookError::InvalidUrl(
            "destination DNS includes a private or special address".to_owned(),
        ));
    }
    Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .connect_timeout(timeout)
        .timeout(timeout)
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(WebhookError::Client)
}

fn normalized_host(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host)
}

fn is_private_or_special(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => ipv4_private_or_special(address),
        IpAddr::V6(address) => ipv6_private_or_special(address),
    }
}
fn ipv4_private_or_special(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_broadcast()
        || address.is_documentation()
        || address.is_unspecified()
        || address.is_multicast()
        || a == 0
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 88 && c == 99)
        || (a == 198 && (18..=19).contains(&b))
        || a >= 240
}
fn ipv6_private_or_special(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    let first = segments[0];
    address.is_loopback()
        || address.is_unspecified()
        || address.is_multicast()
        || (first & 0xfe00) == 0xfc00
        || (first & 0xffc0) == 0xfe80
        || (first == 0x0064 && segments[1] == 0xff9b)
        || (first == 0x0100 && segments[1..4] == [0, 0, 0])
        || (first == 0x2001 && segments[1] < 0x0200)
        || (first == 0x2001 && segments[1] == 0x0db8)
        || first == 0x2002
        || (first == 0x3fff && segments[1] < 0x1000)
        || first == 0x5f00
        || address
            .to_ipv4_mapped()
            .is_some_and(ipv4_private_or_special)
}

#[cfg(test)]
mod tests {
    use super::{pinned_client, validate_destination_port, validate_destination_url};
    use std::time::Duration;
    #[test]
    fn rejects_private_literal_by_default() {
        assert!(validate_destination_url(
            "https://127.0.0.1/hook",
            &["127.0.0.1".to_owned()],
            false,
            false
        )
        .is_err());
    }
    #[test]
    fn rejects_additional_special_ipv4_ranges() {
        for address in [
            "100.64.0.1",
            "192.0.0.1",
            "192.88.99.1",
            "198.18.0.1",
            "240.0.0.1",
        ] {
            assert!(
                validate_destination_url(&format!("https://{address}/hook"), &[], false, false,)
                    .is_err(),
                "{address}"
            );
        }
    }
    #[test]
    fn rejects_special_ipv6_ranges() {
        for address in [
            "64:ff9b:1::1",
            "100::1",
            "2001:db8::1",
            "2002::1",
            "3fff::1",
            "5f00::1",
            "fc00::1",
            "fe80::1",
        ] {
            assert!(
                validate_destination_url(&format!("https://[{address}]/hook"), &[], false, false,)
                    .is_err(),
                "{address}"
            );
        }
    }
    #[test]
    fn rejects_http_by_default() {
        assert!(validate_destination_url(
            "http://example.com/hook",
            &["example.com".to_owned()],
            false,
            false
        )
        .is_err());
    }
    #[test]
    fn restricts_destination_ports() {
        let url = validate_destination_url("https://example.com:8443/hook", &[], false, false)
            .expect("URL");
        assert!(validate_destination_port(&url, &[]).is_err());
        assert!(validate_destination_port(&url, &[8443]).is_ok());
    }
    #[tokio::test]
    async fn rejects_dns_names_when_any_address_is_private() {
        let url = validate_destination_url(
            "https://localhost/hook",
            &["localhost".to_owned()],
            false,
            false,
        )
        .expect("URL");
        assert!(pinned_client(&url, false, Duration::from_secs(1))
            .await
            .is_err());
    }
}
