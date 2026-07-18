//! Tenant-scoped authentication with bounded JWT/JWKS and proxy identity modes.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use http::{header::AUTHORIZATION, HeaderMap};
use jsonwebtoken::{decode, decode_header, jwk::JwkSet, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::Value;
use tonic::metadata::MetadataMap;

use crate::ApiError;

fn default_tenant_header() -> String {
    "x-tenant-id".to_owned()
}
fn default_identity_header() -> String {
    "x-client-identity".to_owned()
}
fn default_tenant_claim() -> String {
    "tenant_id".to_owned()
}
fn default_scope_claim() -> String {
    "scope".to_owned()
}
fn default_subscribe_scope() -> String {
    "router.subscribe".to_owned()
}
fn default_publish_scope() -> String {
    "router.publish".to_owned()
}
fn default_clock_skew_secs() -> u64 {
    30
}
fn default_jwks_refresh_secs() -> u64 {
    300
}
fn default_max_jwks_bytes() -> usize {
    262_144
}
fn default_max_jwks_keys() -> usize {
    64
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
    /// Validates a signed JWT against a bounded, reloadable JWKS.
    Jwt,
    /// Trusts a proxy-injected identity created from a verified mTLS client certificate.
    ProxyMtls,
}

/// JWT issuer, audience, claims, algorithms, and bounded JWKS rotation settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct JwtConfig {
    /// Mounted JWKS JSON file.
    pub jwks_path: PathBuf,
    /// Required exact issuer.
    pub issuer: String,
    /// Required audience.
    pub audience: String,
    /// Allowed asymmetric algorithms: RS256/384/512, ES256/384, or `EdDSA`.
    pub algorithms: Vec<String>,
    /// Allowed clock skew for exp/nbf validation.
    pub clock_skew_secs: u64,
    /// Background file reload interval.
    pub refresh_interval_secs: u64,
    /// Top-level tenant claim.
    pub tenant_claim: String,
    /// Top-level string or string-array scope claim.
    pub scope_claim: String,
    /// Scope required for subscriptions.
    pub subscribe_scope: String,
    /// Scope required for publishing.
    pub publish_scope: String,
    /// Maximum JWKS file size.
    pub max_jwks_bytes: usize,
    /// Maximum accepted keys per JWKS.
    pub max_jwks_keys: usize,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            jwks_path: PathBuf::new(),
            issuer: String::new(),
            audience: String::new(),
            algorithms: vec!["ES256".to_owned()],
            clock_skew_secs: default_clock_skew_secs(),
            refresh_interval_secs: default_jwks_refresh_secs(),
            tenant_claim: default_tenant_claim(),
            scope_claim: default_scope_claim(),
            subscribe_scope: default_subscribe_scope(),
            publish_scope: default_publish_scope(),
            max_jwks_bytes: default_max_jwks_bytes(),
            max_jwks_keys: default_max_jwks_keys(),
        }
    }
}

/// One proxy-verified mTLS identity mapping.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ProxyIdentity {
    /// Tenant assigned to the certificate identity.
    pub tenant_id: String,
    /// Permit subscription APIs.
    pub subscribe: bool,
    /// Permit publish APIs.
    pub publish: bool,
}

/// Authentication settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Selected identity verification mode.
    pub mode: AuthMode,
    /// Development-only fallback tenant.
    pub default_tenant: Option<String>,
    /// Tenant header trusted in proxy-header mode.
    pub tenant_header: String,
    /// Static bearer token to tenant mappings.
    pub bearer_tokens: BTreeMap<String, String>,
    /// Tenants allowed to publish in legacy authentication modes.
    pub publish_tenants: BTreeSet<String>,
    /// Cryptographic JWT validation settings.
    pub jwt: JwtConfig,
    /// Header set only by the protected proxy after mTLS verification.
    pub proxy_identity_header: String,
    /// Exact proxy-verified certificate identity mappings.
    pub proxy_identities: BTreeMap<String, ProxyIdentity>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::Disabled,
            default_tenant: None,
            tenant_header: default_tenant_header(),
            bearer_tokens: BTreeMap::new(),
            publish_tenants: BTreeSet::new(),
            jwt: JwtConfig::default(),
            proxy_identity_header: default_identity_header(),
            proxy_identities: BTreeMap::new(),
        }
    }
}

/// Authenticated request identity and explicit capabilities.
#[derive(Clone, Debug)]
pub struct Principal {
    /// Authoritative authenticated tenant id.
    pub tenant_id: Arc<str>,
    pub(crate) can_subscribe: bool,
    pub(crate) can_publish: bool,
}

#[derive(Debug, Default)]
struct JwksCache {
    keys: HashMap<String, Arc<DecodingKey>>,
}

/// HTTP and gRPC authentication implementation.
#[derive(Clone, Debug)]
pub struct Authenticator {
    config: Arc<AuthConfig>,
    jwks: Arc<RwLock<JwksCache>>,
}

impl Authenticator {
    /// Constructs an authenticator with an initially empty JWKS cache.
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config: Arc::new(config),
            jwks: Arc::new(RwLock::new(JwksCache::default())),
        }
    }

    /// Reloads and atomically replaces the bounded JWKS file.
    pub async fn reload_jwks(&self) -> Result<(), String> {
        if self.config.mode != AuthMode::Jwt {
            return Ok(());
        }
        let metadata = tokio::fs::metadata(&self.config.jwt.jwks_path)
            .await
            .map_err(|error| format!("failed to inspect JWKS file: {error}"))?;
        if metadata.len() > self.config.jwt.max_jwks_bytes as u64 {
            return Err("JWKS file exceeds configured size limit".to_owned());
        }
        let bytes = tokio::fs::read(&self.config.jwt.jwks_path)
            .await
            .map_err(|error| format!("failed to read JWKS file: {error}"))?;
        let set: JwkSet = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid JWKS JSON: {error}"))?;
        if set.keys.is_empty() || set.keys.len() > self.config.jwt.max_jwks_keys {
            return Err("JWKS key count is empty or exceeds configured limit".to_owned());
        }
        let mut keys = HashMap::with_capacity(set.keys.len());
        for jwk in &set.keys {
            let kid = jwk
                .common
                .key_id
                .as_deref()
                .filter(|kid| !kid.is_empty() && kid.len() <= 256)
                .ok_or_else(|| "every JWKS key requires a bounded kid".to_owned())?;
            let key = DecodingKey::from_jwk(jwk)
                .map_err(|error| format!("invalid JWKS key {kid}: {error}"))?;
            if keys.insert(kid.to_owned(), Arc::new(key)).is_some() {
                return Err(format!("duplicate JWKS kid {kid}"));
            }
        }
        let mut cache = self
            .jwks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.keys = keys;
        Ok(())
    }

    /// JWT JWKS refresh interval when JWT mode is active.
    pub fn jwks_refresh_interval(&self) -> Option<std::time::Duration> {
        (self.config.mode == AuthMode::Jwt)
            .then(|| std::time::Duration::from_secs(self.config.jwt.refresh_interval_secs))
    }

    /// Authenticates HTTP headers and returns an authoritative principal.
    pub fn authenticate_http(
        &self,
        headers: &HeaderMap,
        requested_tenant: Option<&str>,
    ) -> Result<Principal, ApiError> {
        match self.config.mode {
            AuthMode::Disabled => self.disabled_principal(requested_tenant),
            AuthMode::StaticBearer => self.static_token(http_authorization(headers)?),
            AuthMode::TrustedHeader => {
                let tenant = http_header(headers, &self.config.tenant_header)?;
                principal_with_permissions(
                    tenant,
                    true,
                    self.config.publish_tenants.contains(tenant),
                )
            }
            AuthMode::Jwt => self.jwt_principal(http_authorization(headers)?),
            AuthMode::ProxyMtls => {
                self.proxy_identity(http_header(headers, &self.config.proxy_identity_header)?)
            }
        }
    }

    /// Authenticates gRPC metadata and returns an authoritative principal.
    pub fn authenticate_grpc(
        &self,
        metadata: &MetadataMap,
        requested_tenant: Option<&str>,
    ) -> Result<Principal, ApiError> {
        match self.config.mode {
            AuthMode::Disabled => self.disabled_principal(requested_tenant),
            AuthMode::StaticBearer => self.static_token(grpc_metadata(metadata, "authorization")?),
            AuthMode::TrustedHeader => {
                let tenant = grpc_metadata(metadata, &self.config.tenant_header)?;
                principal_with_permissions(
                    tenant,
                    true,
                    self.config.publish_tenants.contains(tenant),
                )
            }
            AuthMode::Jwt => self.jwt_principal(grpc_metadata(metadata, "authorization")?),
            AuthMode::ProxyMtls => {
                self.proxy_identity(grpc_metadata(metadata, &self.config.proxy_identity_header)?)
            }
        }
    }

    /// Requires the explicit subscription capability.
    #[allow(
        clippy::unused_self,
        reason = "authorization stays owned by the authenticator"
    )]
    pub fn authorize_subscribe(&self, principal: &Principal) -> Result<(), ApiError> {
        principal
            .can_subscribe
            .then_some(())
            .ok_or(ApiError::Forbidden)
    }

    /// Requires the explicit publishing capability.
    #[allow(
        clippy::unused_self,
        reason = "authorization stays owned by the authenticator"
    )]
    pub fn authorize_publish(&self, principal: &Principal) -> Result<(), ApiError> {
        principal
            .can_publish
            .then_some(())
            .ok_or(ApiError::Forbidden)
    }

    fn disabled_principal(&self, requested: Option<&str>) -> Result<Principal, ApiError> {
        let tenant = requested
            .filter(|v| !v.trim().is_empty())
            .or(self.config.default_tenant.as_deref())
            .ok_or_else(|| ApiError::BadRequest("tenant_id is required".to_owned()))?;
        principal_with_permissions(tenant, true, self.config.publish_tenants.contains(tenant))
    }

    fn static_token(&self, value: &str) -> Result<Principal, ApiError> {
        let token = parse_bearer(value)?;
        let tenant = self
            .config
            .bearer_tokens
            .get(token)
            .ok_or(ApiError::Unauthorized)?;
        principal_with_permissions(tenant, true, self.config.publish_tenants.contains(tenant))
    }

    fn proxy_identity(&self, identity: &str) -> Result<Principal, ApiError> {
        let mapping = self
            .config
            .proxy_identities
            .get(identity)
            .ok_or(ApiError::Unauthorized)?;
        principal_with_permissions(&mapping.tenant_id, mapping.subscribe, mapping.publish)
    }

    fn jwt_principal(&self, value: &str) -> Result<Principal, ApiError> {
        let token = parse_bearer(value)?;
        let header = decode_header(token).map_err(|_| ApiError::Unauthorized)?;
        let kid = header.kid.as_deref().ok_or(ApiError::Unauthorized)?;
        let allowed = configured_algorithms(&self.config.jwt.algorithms)
            .map_err(|_| ApiError::Unauthorized)?;
        if !allowed.contains(&header.alg) {
            return Err(ApiError::Unauthorized);
        }
        let key = {
            let cache = self
                .jwks
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::clone(cache.keys.get(kid).ok_or(ApiError::Unauthorized)?)
        };
        let mut validation = Validation::new(header.alg);
        validation.algorithms = allowed;
        validation.leeway = self.config.jwt.clock_skew_secs;
        validation.set_audience(&[self.config.jwt.audience.as_str()]);
        validation.set_issuer(&[self.config.jwt.issuer.as_str()]);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        let claims = decode::<Value>(token, &key, &validation)
            .map_err(|_| ApiError::Unauthorized)?
            .claims;
        let tenant = claims
            .get(&self.config.jwt.tenant_claim)
            .and_then(Value::as_str)
            .ok_or(ApiError::Unauthorized)?;
        let scopes =
            claim_scopes(claims.get(&self.config.jwt.scope_claim)).ok_or(ApiError::Unauthorized)?;
        principal_with_permissions(
            tenant,
            scopes.contains(self.config.jwt.subscribe_scope.as_str()),
            scopes.contains(self.config.jwt.publish_scope.as_str()),
        )
    }
}

fn configured_algorithms(values: &[String]) -> Result<Vec<Algorithm>, String> {
    values
        .iter()
        .map(|value| match value.as_str() {
            "RS256" => Ok(Algorithm::RS256),
            "RS384" => Ok(Algorithm::RS384),
            "RS512" => Ok(Algorithm::RS512),
            "ES256" => Ok(Algorithm::ES256),
            "ES384" => Ok(Algorithm::ES384),
            "EdDSA" => Ok(Algorithm::EdDSA),
            _ => Err(format!("unsupported or symmetric JWT algorithm {value}")),
        })
        .collect()
}

fn claim_scopes(value: Option<&Value>) -> Option<BTreeSet<&str>> {
    match value? {
        Value::String(scopes) => Some(scopes.split_ascii_whitespace().collect()),
        Value::Array(scopes) => scopes.iter().map(Value::as_str).collect(),
        _ => None,
    }
}

fn http_authorization(headers: &HeaderMap) -> Result<&str, ApiError> {
    http_header(headers, AUTHORIZATION.as_str())
}
fn http_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, ApiError> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::Unauthorized)
}
fn grpc_metadata<'a>(metadata: &'a MetadataMap, name: &str) -> Result<&'a str, ApiError> {
    metadata
        .get(name)
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::Unauthorized)
}
fn parse_bearer(value: &str) -> Result<&str, ApiError> {
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .ok_or(ApiError::Unauthorized)
}
fn principal_with_permissions(
    tenant: &str,
    subscribe: bool,
    publish: bool,
) -> Result<Principal, ApiError> {
    let tenant = tenant.trim();
    if tenant.is_empty() || tenant.len() > 256 || tenant.chars().any(char::is_control) {
        return Err(ApiError::BadRequest("invalid tenant identity".to_owned()));
    }
    Ok(Principal {
        tenant_id: Arc::from(tenant),
        can_subscribe: subscribe,
        can_publish: publish,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::Serialize;
    use uuid::Uuid;

    use super::{AuthConfig, AuthMode, Authenticator, JwtConfig};
    use crate::ApiError;

    const KEY_1_DER: &[u8] = include_bytes!("../tests/fixtures/jwt-key-1-private.der");
    const KEY_2_DER: &[u8] = include_bytes!("../tests/fixtures/jwt-key-2-private.der");
    const KEY_1_JWKS: &[u8] = include_bytes!("../tests/fixtures/jwt-key-1-jwks.json");
    const KEY_2_JWKS: &[u8] = include_bytes!("../tests/fixtures/jwt-key-2-jwks.json");

    struct TestKey {
        kid: &'static str,
        der: &'static [u8],
        jwks: &'static [u8],
    }

    fn key_1() -> TestKey {
        TestKey {
            kid: "key-1",
            der: KEY_1_DER,
            jwks: KEY_1_JWKS,
        }
    }

    fn key_2() -> TestKey {
        TestKey {
            kid: "key-2",
            der: KEY_2_DER,
            jwks: KEY_2_JWKS,
        }
    }
    #[derive(Clone, Serialize)]
    struct Claims {
        exp: u64,
        iss: String,
        aud: String,
        tenant_id: String,
        scope: String,
    }

    fn jwt_config(path: PathBuf) -> AuthConfig {
        AuthConfig {
            mode: AuthMode::Jwt,
            jwt: JwtConfig {
                jwks_path: path,
                issuer: "https://issuer.example".to_owned(),
                audience: "kafka-edge-router".to_owned(),
                algorithms: vec!["ES256".to_owned()],
                clock_skew_secs: 0,
                refresh_interval_secs: 60,
                ..JwtConfig::default()
            },
            ..AuthConfig::default()
        }
    }

    fn claims() -> Claims {
        Claims {
            exp: now().saturating_add(300),
            iss: "https://issuer.example".to_owned(),
            aud: "kafka-edge-router".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            scope: "router.subscribe router.publish".to_owned(),
        }
    }

    fn token(key: &TestKey, claims: &Claims) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(key.kid.to_owned());
        encode(&header, claims, &EncodingKey::from_ec_der(key.der)).expect("JWT")
    }

    fn authenticate(auth: &Authenticator, token: &str) -> Result<super::Principal, ApiError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("authorization"),
        );
        auth.authenticate_http(&headers, Some("tenant-a"))
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_secs()
    }

    #[tokio::test]
    async fn jwt_rejects_invalid_contracts_and_algorithm_confusion() {
        let key = key_1();
        let path = std::env::temp_dir().join(format!("router-jwks-{}.json", Uuid::new_v4()));
        tokio::fs::write(&path, &key.jwks)
            .await
            .expect("write JWKS");
        let auth = Authenticator::new(jwt_config(path.clone()));
        auth.reload_jwks().await.expect("load JWKS");

        let valid = token(&key, &claims());
        let principal = authenticate(&auth, &valid).expect("valid JWT");
        assert_eq!(principal.tenant_id.as_ref(), "tenant-a");
        auth.authorize_subscribe(&principal)
            .expect("subscribe scope");
        auth.authorize_publish(&principal).expect("publish scope");

        let mut expired = claims();
        expired.exp = now().saturating_sub(1);
        assert!(matches!(
            authenticate(&auth, &token(&key, &expired)),
            Err(ApiError::Unauthorized)
        ));

        let mut wrong_issuer = claims();
        wrong_issuer.iss = "https://attacker.example".to_owned();
        assert!(matches!(
            authenticate(&auth, &token(&key, &wrong_issuer)),
            Err(ApiError::Unauthorized)
        ));

        let mut wrong_audience = claims();
        wrong_audience.aud = "other-service".to_owned();
        assert!(matches!(
            authenticate(&auth, &token(&key, &wrong_audience)),
            Err(ApiError::Unauthorized)
        ));

        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("key-1".to_owned());
        let confused = encode(
            &header,
            &claims(),
            &EncodingKey::from_secret(b"not-the-public-key"),
        )
        .expect("confusion token");
        assert!(matches!(
            authenticate(&auth, &confused),
            Err(ApiError::Unauthorized)
        ));
        assert!(matches!(
            authenticate(&auth, "malformed"),
            Err(ApiError::Unauthorized)
        ));
        tokio::fs::remove_file(path)
            .await
            .expect("remove temporary JWKS");
    }

    #[tokio::test]
    async fn jwks_reload_rotates_keys_and_scopes_are_explicit() {
        let key_1 = key_1();
        let key_2 = key_2();
        let path = std::env::temp_dir().join(format!("router-jwks-{}.json", Uuid::new_v4()));
        tokio::fs::write(&path, &key_1.jwks)
            .await
            .expect("write first JWKS");
        let auth = Authenticator::new(jwt_config(path.clone()));
        auth.reload_jwks().await.expect("load first JWKS");

        let first = token(&key_1, &claims());
        authenticate(&auth, &first).expect("first key accepted");

        tokio::fs::write(&path, &key_2.jwks)
            .await
            .expect("write rotated JWKS");
        auth.reload_jwks().await.expect("reload rotated JWKS");
        assert!(matches!(
            authenticate(&auth, &first),
            Err(ApiError::Unauthorized)
        ));

        let mut subscribe_only = claims();
        subscribe_only.scope = "router.subscribe".to_owned();
        let second = token(&key_2, &subscribe_only);
        let principal = authenticate(&auth, &second).expect("rotated key accepted");
        auth.authorize_subscribe(&principal)
            .expect("subscribe scope");
        assert!(matches!(
            auth.authorize_publish(&principal),
            Err(ApiError::Forbidden)
        ));

        tokio::fs::remove_file(path)
            .await
            .expect("remove temporary JWKS");
    }
}
