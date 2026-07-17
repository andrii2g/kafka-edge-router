//! Tenant-scoped authentication modes suitable for local and proxied deployments.

use std::{collections::BTreeMap, sync::Arc};

use http::{header::AUTHORIZATION, HeaderMap};
use serde::Deserialize;
use tonic::metadata::MetadataMap;

use crate::ApiError;

fn default_tenant_header() -> String {
    "x-tenant-id".to_owned()
}

/// Authentication mode selected in configuration.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// Development mode. Tenant is accepted from the request or `default_tenant`.
    #[default]
    Disabled,
    /// Maps an opaque bearer token to exactly one tenant.
    StaticBearer,
    /// Trusts a tenant header injected by an authenticated reverse proxy.
    TrustedHeader,
}

/// Authentication settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Authentication strategy.
    pub mode: AuthMode,
    /// Fallback tenant in disabled mode.
    pub default_tenant: Option<String>,
    /// Header containing a proxy-verified tenant in trusted-header mode.
    pub tenant_header: String,
    /// Opaque token to tenant mapping in static-bearer mode.
    pub bearer_tokens: BTreeMap<String, String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::Disabled,
            default_tenant: None,
            tenant_header: default_tenant_header(),
            bearer_tokens: BTreeMap::new(),
        }
    }
}

/// Authenticated request identity.
#[derive(Clone, Debug)]
pub struct Principal {
    /// Tenant to which the entire connection is restricted.
    pub tenant_id: Arc<str>,
}

/// HTTP and gRPC authentication implementation.
#[derive(Clone, Debug)]
pub struct Authenticator {
    config: Arc<AuthConfig>,
}

impl Authenticator {
    /// Creates an authenticator from immutable configuration.
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Authenticates HTTP headers and resolves the connection tenant.
    pub fn authenticate_http(
        &self,
        headers: &HeaderMap,
        requested_tenant: Option<&str>,
    ) -> Result<Principal, ApiError> {
        match self.config.mode {
            AuthMode::Disabled => self.disabled_principal(requested_tenant),
            AuthMode::StaticBearer => {
                let value = headers
                    .get(AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or(ApiError::Unauthorized)?;
                self.token_principal(parse_bearer(value)?)
            }
            AuthMode::TrustedHeader => {
                let value = headers
                    .get(self.config.tenant_header.as_str())
                    .and_then(|value| value.to_str().ok())
                    .ok_or(ApiError::Unauthorized)?;
                principal(value)
            }
        }
    }

    /// Authenticates gRPC metadata and resolves the connection tenant.
    pub fn authenticate_grpc(
        &self,
        metadata: &MetadataMap,
        requested_tenant: Option<&str>,
    ) -> Result<Principal, ApiError> {
        match self.config.mode {
            AuthMode::Disabled => self.disabled_principal(requested_tenant),
            AuthMode::StaticBearer => {
                let value = metadata
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .ok_or(ApiError::Unauthorized)?;
                self.token_principal(parse_bearer(value)?)
            }
            AuthMode::TrustedHeader => {
                let value = metadata
                    .get(self.config.tenant_header.as_str())
                    .and_then(|value| value.to_str().ok())
                    .ok_or(ApiError::Unauthorized)?;
                principal(value)
            }
        }
    }

    fn disabled_principal(&self, requested_tenant: Option<&str>) -> Result<Principal, ApiError> {
        requested_tenant
            .filter(|tenant| !tenant.trim().is_empty())
            .or(self.config.default_tenant.as_deref())
            .map(principal)
            .transpose()?
            .ok_or_else(|| ApiError::BadRequest("tenant_id is required".to_owned()))
    }

    fn token_principal(&self, token: &str) -> Result<Principal, ApiError> {
        self.config
            .bearer_tokens
            .get(token)
            .map(String::as_str)
            .map(principal)
            .transpose()?
            .ok_or(ApiError::Unauthorized)
    }
}

fn parse_bearer(value: &str) -> Result<&str, ApiError> {
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .ok_or(ApiError::Unauthorized)
}

fn principal(tenant: &str) -> Result<Principal, ApiError> {
    let tenant = tenant.trim();
    if tenant.is_empty() || tenant.len() > 256 || tenant.chars().any(char::is_control) {
        return Err(ApiError::BadRequest("invalid tenant identity".to_owned()));
    }
    Ok(Principal {
        tenant_id: Arc::from(tenant),
    })
}
