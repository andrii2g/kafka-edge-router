//! Static URL validation and baseline SSRF controls.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use reqwest::Url;

use crate::WebhookError;

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
    let effective_hosts: Vec<&str> = if allowed_hosts.is_empty() {
        vec![host]
    } else {
        allowed_hosts.iter().map(String::as_str).collect()
    };
    if !effective_hosts
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(host))
    {
        return Err(WebhookError::HostNotAllowed(host.to_owned()));
    }
    if !allow_private_ips {
        if let Ok(address) = host.parse::<IpAddr>() {
            if is_private_or_special(address) {
                return Err(WebhookError::PrivateAddress(address));
            }
        }
    }
    Ok(url)
}

fn is_private_or_special(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => ipv4_private_or_special(address),
        IpAddr::V6(address) => ipv6_private_or_special(address),
    }
}

fn ipv4_private_or_special(address: Ipv4Addr) -> bool {
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_broadcast()
        || address.is_documentation()
        || address.is_unspecified()
        || address.is_multicast()
        || address.octets()[0] == 0
}

fn ipv6_private_or_special(address: Ipv6Addr) -> bool {
    let first = address.segments()[0];
    address.is_loopback()
        || address.is_unspecified()
        || address.is_multicast()
        || (first & 0xfe00) == 0xfc00
        || (first & 0xffc0) == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::validate_destination_url;

    #[test]
    fn rejects_private_literal_by_default() {
        let result = validate_destination_url(
            "https://127.0.0.1/hook",
            &["127.0.0.1".to_owned()],
            false,
            false,
        );
        assert!(result.is_err());
    }

    #[test]
    fn rejects_http_by_default() {
        let result = validate_destination_url(
            "http://example.com/hook",
            &["example.com".to_owned()],
            false,
            false,
        );
        assert!(result.is_err());
    }
}
