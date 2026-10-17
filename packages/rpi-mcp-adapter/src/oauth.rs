//! Provider-owned OAuth for Streamable HTTP MCP servers.
//!
//! One sign-in, end to end: probe the MCP endpoint for a `WWW-Authenticate:
//! Bearer` challenge, walk RFC 9728 protected-resource metadata to the RFC 8414
//! authorization-server metadata, register a client (RFC 7591) when the server
//! offers no pre-registered one, then run an authorization-code + PKCE (S256)
//! exchange against a loopback redirect.
//!
//! Tokens land in the host's credential store (`$RPI_CODING_AGENT_DIR/auth.json`,
//! default `~/.rpi/agent/auth.json`) under the provider id `mcp:<server>`, so
//! `rpi auth refresh|logout --provider mcp:<server>` keeps working and the
//! adapter can attach the same bearer token to MCP requests. Nothing in this
//! module logs a token.
//!
//! All network and callback I/O runs on a dedicated worker thread. Command
//! handlers run synchronously on the TUI thread, and the host's `rpi auth login`
//! path calls us from inside a Tokio runtime, so doing the blocking work inline
//! would either freeze the UI or panic on a nested runtime.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use url::Url;

/// Provider ids are namespaced so an MCP server named `anthropic` cannot shadow
/// the model provider of the same name in `auth.json`.
pub const PROVIDER_PREFIX: &str = "mcp:";
const CALLBACK_PATH: &str = "/callback";
const FLOW_TIMEOUT: Duration = Duration::from_secs(180);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_BODY: usize = 1 << 20;
/// Preferred loopback port. Reusing one port keeps the dynamically registered
/// `redirect_uri` stable across logins; we fall back to an ephemeral port when
/// it is taken and re-register in that case.
const DEFAULT_LOOPBACK_PORT: u16 = 33418;

pub fn provider_id(server: &str) -> String {
    format!("{PROVIDER_PREFIX}{server}")
}

pub fn server_name(provider: &str) -> Option<&str> {
    provider.strip_prefix(PROVIDER_PREFIX)
}

/// The non-secret provider manifest the host shows in `rpi auth` listings.
pub fn manifest(server: &str) -> String {
    json!({
        "id": provider_id(server),
        "displayName": format!("MCP server: {server}"),
        "supportsPkce": true,
        "capabilities": { "refresh": true, "revoke": true },
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Credential store (host-compatible auth.json)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Token {
    pub access: String,
    pub refresh: String,
    /// Unix epoch seconds; `0` means "no known expiry", matching the host.
    pub expires: i64,
}

impl Token {
    pub fn is_expired(&self, now: i64) -> bool {
        self.expires != 0 && self.expires <= now
    }
}

pub fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

/// The host's agent directory. Mirrors `rpi-cli`'s `agent_dir()` so the adapter
/// and the host agree on where credentials live.
pub fn store_dir() -> Result<PathBuf, String> {
    if let Some(value) = std::env::var_os("RPI_CODING_AGENT_DIR") {
        let path = PathBuf::from(value);
        if !path.is_absolute() {
            return Err("RPI_CODING_AGENT_DIR must be an absolute path".into());
        }
        return Ok(path);
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or("cannot locate the home directory for the credential store")?;
    Ok(PathBuf::from(home).join(".rpi").join("agent"))
}

fn auth_path(dir: &Path) -> PathBuf {
    dir.join("auth.json")
}

fn cache_path(dir: &Path) -> PathBuf {
    dir.join("mcp-oauth.json")
}

/// Read a JSON object, tolerating a missing file. The map is kept as raw
/// [`Value`]s so credentials owned by other providers round-trip untouched —
/// we only ever replace our own `mcp:<server>` key.
fn read_object(path: &Path) -> Result<BTreeMap<String, Value>, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => match serde_json::from_str::<Value>(&text) {
            Ok(Value::Object(map)) => Ok(map.into_iter().collect()),
            Ok(_) => Err(format!("{} must contain a JSON object", path.display())),
            Err(error) => Err(format!("{} is not valid JSON: {error}", path.display())),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(error) => Err(format!("cannot read {}: {error}", path.display())),
    }
}

fn write_secure_json(path: &Path, map: &BTreeMap<String, Value>) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
    }
    let text = serde_json::to_string_pretty(&Value::Object(map.clone().into_iter().collect()))
        .map_err(|error| error.to_string())?;
    // Write a sibling temp file, lock it down, then rename over the target so a
    // reader never observes a half-written credential store.
    let temp = path.with_extension(format!("tmp{}", std::process::id()));
    std::fs::write(&temp, text.as_bytes())
        .map_err(|error| format!("cannot write {}: {error}", temp.display()))?;
    set_owner_only(&temp)?;
    std::fs::rename(&temp, path)
        .map_err(|error| format!("cannot replace {}: {error}", path.display()))?;
    Ok(())
}

fn set_owner_only(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("cannot restrict {}: {error}", path.display()))?;
    }
    Ok(())
}

pub fn read_token_in(dir: &Path, server: &str) -> Result<Option<Token>, String> {
    let map = read_object(&auth_path(dir))?;
    let Some(value) = map.get(&provider_id(server)) else {
        return Ok(None);
    };
    if value.get("type").and_then(Value::as_str) != Some("oauth") {
        return Ok(None);
    }
    let access = value
        .get("access")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if access.is_empty() {
        return Ok(None);
    }
    Ok(Some(Token {
        access,
        refresh: value
            .get("refresh")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        expires: value.get("expires").and_then(Value::as_i64).unwrap_or(0),
    }))
}

pub fn write_token_in(dir: &Path, server: &str, token: &Token) -> Result<(), String> {
    let path = auth_path(dir);
    let mut map = read_object(&path)?;
    map.insert(
        provider_id(server),
        json!({
            "type": "oauth",
            "access": token.access,
            "refresh": token.refresh,
            "expires": token.expires,
        }),
    );
    write_secure_json(&path, &map)
}

pub fn delete_token_in(dir: &Path, server: &str) -> Result<bool, String> {
    let path = auth_path(dir);
    let mut map = read_object(&path)?;
    if map.remove(&provider_id(server)).is_none() {
        return Ok(false);
    }
    write_secure_json(&path, &map)?;
    Ok(true)
}

/// Discovered endpoints plus the dynamically registered client. Cached per
/// server because `refresh` and `revoke` receive only a credential from the
/// host and must not have to re-run discovery to find the token endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerAuth {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub issuer: String,
    #[serde(default)]
    pub authorization_endpoint: String,
    #[serde(default)]
    pub token_endpoint: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub revocation_endpoint: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub registration_endpoint: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_secret: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
}

impl ServerAuth {
    fn redirect_uri(&self) -> String {
        format!("http://127.0.0.1:{}{CALLBACK_PATH}", self.port)
    }
}

fn read_server_auth_in(dir: &Path, server: &str) -> Result<ServerAuth, String> {
    let map = read_object(&cache_path(dir))?;
    match map.get(&provider_id(server)) {
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|error| format!("cached MCP OAuth state is invalid: {error}")),
        None => Ok(ServerAuth::default()),
    }
}

fn write_server_auth_in(dir: &Path, server: &str, auth: &ServerAuth) -> Result<(), String> {
    let path = cache_path(dir);
    let mut map = read_object(&path)?;
    map.insert(
        provider_id(server),
        serde_json::to_value(auth).map_err(|error| error.to_string())?,
    );
    write_secure_json(&path, &map)
}

// ---------------------------------------------------------------------------
// PKCE
// ---------------------------------------------------------------------------

/// A URL-safe random string. Used for the PKCE verifier and the `state` value.
pub(crate) fn random_token(bytes: usize) -> String {
    let mut buffer = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buffer);
    URL_SAFE_NO_PAD.encode(buffer)
}

/// PKCE S256: `base64url(sha256(verifier))` without padding (RFC 7636 §4.2).
pub(crate) fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

/// Extract the `resource_metadata` parameter from a `Bearer` challenge.
pub(crate) fn resource_metadata_param(header: &str) -> Option<String> {
    let lower = header.to_ascii_lowercase();
    let index = lower.find("resource_metadata")?;
    // `to_ascii_lowercase` preserves byte offsets, so `index` is valid in both.
    let rest = header[index + "resource_metadata".len()..].trim_start();
    let rest = rest.strip_prefix('=')?.trim_start();
    if let Some(quoted) = rest.strip_prefix('"') {
        return quoted.find('"').map(|end| quoted[..end].to_string());
    }
    let end = rest
        .find(|character: char| character == ',' || character.is_whitespace())
        .unwrap_or(rest.len());
    (end > 0).then(|| rest[..end].to_string())
}

/// RFC 9728 / RFC 8414 well-known URL: the well-known segment is inserted
/// between the authority and the existing path.
pub(crate) fn well_known_url(base: &Url, name: &str) -> Result<Url, String> {
    let mut url = base.clone();
    let path = base.path().trim_end_matches('/');
    url.set_path(&format!("/.well-known/{name}{path}"));
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

struct HttpOutcome {
    status: u16,
    body: String,
}

fn http_client() -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("rpi-mcp-adapter/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|error| format!("cannot create the OAuth HTTP client: {error}"))
}

fn read_body(response: reqwest::blocking::Response) -> Result<HttpOutcome, String> {
    let status = response.status().as_u16();
    let text = response
        .text()
        .map_err(|error| format!("cannot read the OAuth response body: {error}"))?;
    if text.len() > MAX_BODY {
        return Err("OAuth response exceeded the 1 MiB limit".into());
    }
    Ok(HttpOutcome { status, body: text })
}

fn fetch_json(client: &reqwest::blocking::Client, url: &Url) -> Result<Value, String> {
    let response = client
        .get(url.clone())
        .header("Accept", "application/json")
        .send()
        .map_err(|error| format!("cannot reach {url}: {error}"))?;
    if !response.status().is_success() {
        return Err(format!("{url} returned HTTP {}", response.status()));
    }
    let outcome = read_body(response)?;
    serde_json::from_str(&outcome.body).map_err(|error| format!("{url} is not valid JSON: {error}"))
}

fn post_form(
    client: &reqwest::blocking::Client,
    url: &str,
    form: &[(String, String)],
) -> Result<HttpOutcome, String> {
    let response = client
        .post(url)
        .header("Accept", "application/json")
        .form(form)
        .send()
        .map_err(|error| format!("cannot reach {url}: {error}"))?;
    read_body(response)
}

fn post_json(
    client: &reqwest::blocking::Client,
    url: &str,
    body: &Value,
) -> Result<HttpOutcome, String> {
    let response = client
        .post(url)
        .header("Accept", "application/json")
        .json(body)
        .send()
        .map_err(|error| format!("cannot reach {url}: {error}"))?;
    read_body(response)
}

/// Send an unauthenticated MCP `initialize` and return the `WWW-Authenticate`
/// challenge. This is the only way the spec offers to learn where the
/// protected-resource metadata lives.
fn probe_challenge(client: &reqwest::blocking::Client, server_url: &str) -> Result<String, String> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": { "name": "rpi-mcp-adapter", "version": env!("CARGO_PKG_VERSION") },
        },
    });
    let response = client
        .post(server_url)
        .header("Accept", "application/json, text/event-stream")
        .json(&body)
        .send()
        .map_err(|error| format!("cannot reach {server_url}: {error}"))?;
    if response.status().is_success() {
        return Err(
            "the MCP server answered without an authorization challenge; it does not require OAuth"
                .into(),
        );
    }
    if let Some(value) = response.headers().get(reqwest::header::WWW_AUTHENTICATE) {
        let header = value
            .to_str()
            .map_err(|_| "the WWW-Authenticate header is not valid UTF-8".to_string())?;
        if header.to_ascii_lowercase().contains("bearer") {
            return Ok(header.to_string());
        }
        return Err(format!("the MCP server requested unsupported auth: {header}"));
    }
    Err(format!(
        "the MCP server returned HTTP {} without a WWW-Authenticate challenge",
        response.status()
    ))
}

fn metadata_string(document: &Value, key: &str) -> Option<String> {
    document
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn metadata_list(document: &Value, key: &str) -> Vec<String> {
    document
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve the authorization-server metadata for one MCP endpoint.
fn discover(
    client: &reqwest::blocking::Client,
    server_url: &str,
    progress: &dyn Progress,
) -> Result<ServerAuth, String> {
    let endpoint = Url::parse(server_url).map_err(|error| format!("invalid MCP URL: {error}"))?;

    progress.stage("contacting the MCP server for an authorization challenge");
    let challenge = probe_challenge(client, server_url)?;

    progress.stage("reading protected-resource metadata");
    let resource_metadata = match resource_metadata_param(&challenge) {
        Some(value) => Url::parse(&value).map_err(|error| {
            format!("the resource_metadata challenge parameter is not a URL: {error}")
        })?,
        None => well_known_url(&endpoint, "oauth-protected-resource")?,
    };
    let resource_document = fetch_json(client, &resource_metadata).map_err(|error| {
        format!(
            "cannot read protected-resource metadata at {resource_metadata}: {error}. \
             The server advertised a challenge but publishes no resource metadata."
        )
    })?;
    let issuer = metadata_list(&resource_document, "authorization_servers")
        .into_iter()
        .next()
        .or_else(|| metadata_string(&resource_document, "authorization_server"))
        .ok_or(
            "the protected-resource metadata lists no authorization_servers; \
             configure the server with an explicit token instead",
        )?;
    let issuer_url = Url::parse(&issuer)
        .map_err(|error| format!("the advertised authorization server is not a URL: {error}"))?;

    progress.stage("reading authorization-server metadata");
    let mut candidates = vec![well_known_url(&issuer_url, "oauth-authorization-server")?];
    candidates.push(well_known_url(&issuer_url, "openid-configuration")?);
    // A handful of servers publish the metadata document at the issuer itself.
    candidates.push(issuer_url.clone());
    let mut last_error = String::new();
    let mut document = None;
    for candidate in candidates {
        match fetch_json(client, &candidate) {
            Ok(value) => {
                document = Some(value);
                break;
            }
            Err(error) => last_error = error,
        }
    }
    let document = document.ok_or_else(|| {
        format!("cannot read authorization-server metadata for {issuer}: {last_error}")
    })?;

    let methods = metadata_list(&document, "code_challenge_methods_supported");
    if !methods.is_empty() && !methods.iter().any(|method| method == "S256") {
        return Err(format!(
            "{issuer} does not offer PKCE S256 (it advertises {}); refusing to continue without PKCE",
            methods.join(", ")
        ));
    }

    Ok(ServerAuth {
        issuer,
        authorization_endpoint: metadata_string(&document, "authorization_endpoint")
            .ok_or("authorization-server metadata has no authorization_endpoint")?,
        token_endpoint: metadata_string(&document, "token_endpoint")
            .ok_or("authorization-server metadata has no token_endpoint")?,
        revocation_endpoint: metadata_string(&document, "revocation_endpoint").unwrap_or_default(),
        registration_endpoint: metadata_string(&document, "registration_endpoint")
            .unwrap_or_default(),
        client_id: String::new(),
        client_secret: String::new(),
        port: 0,
        // Prefer the resource's own scopes; the authorization server's full
        // catalogue is usually far broader than this one resource needs.
        scopes: metadata_list(&resource_document, "scopes_supported"),
    })
}

/// RFC 7591 dynamic client registration. Public client, PKCE, no secret.
fn register_client(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    redirect_uri: &str,
) -> Result<(String, String), String> {
    let body = json!({
        "client_name": "rpi-mcp-adapter",
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
        "application_type": "native",
    });
    let outcome = post_json(client, endpoint, &body)?;
    if !(200..300).contains(&outcome.status) {
        return Err(format!(
            "dynamic client registration at {endpoint} failed with HTTP {}: {}",
            outcome.status,
            snippet(&outcome.body)
        ));
    }
    let document: Value = serde_json::from_str(&outcome.body)
        .map_err(|error| format!("client registration returned invalid JSON: {error}"))?;
    let client_id = metadata_string(&document, "client_id")
        .ok_or("client registration returned no client_id")?;
    Ok((
        client_id,
        metadata_string(&document, "client_secret").unwrap_or_default(),
    ))
}

fn snippet(body: &str) -> String {
    let trimmed = body.trim();
    let mut text: String = trimmed.chars().take(200).collect();
    if trimmed.chars().count() > 200 {
        text.push('…');
    }
    if text.is_empty() {
        "<empty response>".to_string()
    } else {
        text
    }
}

// ---------------------------------------------------------------------------
// Token endpoints
// ---------------------------------------------------------------------------

fn token_from_document(document: &Value, fallback_refresh: &str) -> Result<Token, String> {
    let access = metadata_string(document, "access_token")
        .ok_or("the token endpoint returned no access_token")?;
    let expires_in = document
        .get("expires_in")
        .and_then(Value::as_i64)
        .filter(|seconds| *seconds > 0)
        .unwrap_or(0);
    let refresh = metadata_string(document, "refresh_token").unwrap_or_else(|| {
        // Servers commonly omit refresh_token on a refresh grant; keep the one
        // we already hold instead of silently losing the ability to refresh.
        fallback_refresh.to_string()
    });
    Ok(Token {
        access,
        refresh,
        expires: if expires_in == 0 {
            0
        } else {
            now_epoch() + expires_in
        },
    })
}

fn describe_failure(outcome: &HttpOutcome) -> String {
    let error = serde_json::from_str::<Value>(&outcome.body)
        .ok()
        .and_then(|document| {
            let code = metadata_string(&document, "error")?;
            Some(match metadata_string(&document, "error_description") {
                Some(description) => format!("{code}: {description}"),
                None => code,
            })
        })
        .unwrap_or_else(|| snippet(&outcome.body));
    format!("HTTP {} — {error}", outcome.status)
}

fn client_auth_fields(auth: &ServerAuth) -> Vec<(String, String)> {
    let mut fields = vec![("client_id".to_string(), auth.client_id.clone())];
    if !auth.client_secret.is_empty() {
        fields.push(("client_secret".to_string(), auth.client_secret.clone()));
    }
    fields
}

fn exchange_code_in(
    client: &reqwest::blocking::Client,
    auth: &ServerAuth,
    code: &str,
    verifier: &str,
) -> Result<Token, String> {
    let mut form = vec![
        ("grant_type".to_string(), "authorization_code".to_string()),
        ("code".to_string(), code.to_string()),
        ("redirect_uri".to_string(), auth.redirect_uri()),
        ("code_verifier".to_string(), verifier.to_string()),
    ];
    form.extend(client_auth_fields(auth));
    let outcome = post_form(client, &auth.token_endpoint, &form)?;
    if !(200..300).contains(&outcome.status) {
        return Err(format!(
            "the token endpoint rejected the authorization code ({})",
            describe_failure(&outcome)
        ));
    }
    let document: Value = serde_json::from_str(&outcome.body)
        .map_err(|error| format!("the token endpoint returned invalid JSON: {error}"))?;
    token_from_document(&document, "")
}

fn refresh_in(
    client: &reqwest::blocking::Client,
    auth: &ServerAuth,
    refresh: &str,
) -> Result<Token, String> {
    let mut form = vec![
        ("grant_type".to_string(), "refresh_token".to_string()),
        ("refresh_token".to_string(), refresh.to_string()),
    ];
    form.extend(client_auth_fields(auth));
    let outcome = post_form(client, &auth.token_endpoint, &form)?;
    if !(200..300).contains(&outcome.status) {
        return Err(format!(
            "the token endpoint rejected the refresh token ({})",
            describe_failure(&outcome)
        ));
    }
    let document: Value = serde_json::from_str(&outcome.body)
        .map_err(|error| format!("the token endpoint returned invalid JSON: {error}"))?;
    token_from_document(&document, refresh)
}

fn revoke_in(
    client: &reqwest::blocking::Client,
    auth: &ServerAuth,
    token: &Token,
) -> Result<(), String> {
    if auth.revocation_endpoint.is_empty() {
        // RFC 7009 is optional; without a revocation endpoint the caller still
        // drops the local credential.
        return Ok(());
    }
    for (value, hint) in [
        (&token.refresh, "refresh_token"),
        (&token.access, "access_token"),
    ] {
        if value.is_empty() {
            continue;
        }
        let mut form = vec![
            ("token".to_string(), value.clone()),
            ("token_type_hint".to_string(), hint.to_string()),
        ];
        form.extend(client_auth_fields(auth));
        let outcome = post_form(client, &auth.revocation_endpoint, &form)?;
        if !(200..300).contains(&outcome.status) {
            return Err(format!(
                "the revocation endpoint rejected the token ({})",
                describe_failure(&outcome)
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Loopback callback
// ---------------------------------------------------------------------------

fn bind_listener(preferred: u16) -> Result<(TcpListener, u16), String> {
    if preferred != 0 {
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", preferred)) {
            return Ok((listener, preferred));
        }
    }
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("cannot bind the loopback callback: {error}"))?;
    let port = listener
        .local_addr()
        .map_err(|error| format!("cannot read the loopback port: {error}"))?
        .port();
    Ok((listener, port))
}

/// Percent-decode a single `application/x-www-form-urlencoded` component.
pub(crate) fn decode_component(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let digits = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("");
                match u8::from_str_radix(digits, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        index += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        index += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Extract the query string from an HTTP request line when it targets the
/// callback path. Returns `None` for any other request (favicon, probes).
pub(crate) fn callback_query(request_line: &str) -> Option<String> {
    let mut parts = request_line.split_whitespace();
    let _method = parts.next()?;
    let target = parts.next()?;
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target, ""),
    };
    (path == CALLBACK_PATH).then(|| query.to_string())
}

pub(crate) fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (decode_component(key) == name).then(|| decode_component(value))
    })
}

fn read_request_line(stream: &mut std::net::TcpStream) -> Option<String> {
    let mut buffer = [0u8; 8192];
    let read = stream.read(&mut buffer).ok()?;
    let text = String::from_utf8_lossy(&buffer[..read]);
    text.lines().next().map(str::to_string)
}

fn respond(stream: &mut std::net::TcpStream, status: u16, title: &str, detail: &str) {
    let reason = if status == 200 { "OK" } else { "Bad Request" };
    let body = format!(
        "<!doctype html><meta charset=\"utf-8\"><title>{title}</title>\
         <body style=\"font-family:-apple-system,system-ui,sans-serif;padding:3rem;text-align:center\">\
         <h1>{title}</h1><p>{detail}</p>"
    );
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/html; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn wait_for_callback(
    listener: &TcpListener,
    expected_state: &str,
    cancel: &(dyn Fn() -> bool + Send + Sync),
) -> Result<String, String> {
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("cannot configure the loopback callback: {error}"))?;
    let deadline = Instant::now() + FLOW_TIMEOUT;
    loop {
        if cancel() {
            return Err("sign-in was cancelled".into());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out after {}s waiting for the browser redirect",
                FLOW_TIMEOUT.as_secs()
            ));
        }
        match listener.accept() {
            Ok((mut stream, _)) => {
                // Accepted sockets inherit the listener's non-blocking flag on
                // some platforms; normalise before doing a bounded read.
                let _ = stream.set_nonblocking(false);
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let line = read_request_line(&mut stream);
                let Some(query) = line.as_deref().and_then(callback_query) else {
                    respond(
                        &mut stream,
                        404,
                        "Not found",
                        "This endpoint only handles the OAuth callback.",
                    );
                    continue;
                };
                if let Some(error) = query_param(&query, "error") {
                    let detail = query_param(&query, "error_description").unwrap_or_default();
                    respond(&mut stream, 400, "Authorization failed", &error);
                    return Err(format!("the authorization server returned `{error}` {detail}").trim_end().to_string());
                }
                if query_param(&query, "state").unwrap_or_default() != expected_state {
                    respond(
                        &mut stream,
                        400,
                        "Authorization failed",
                        "The state value did not match; the response was rejected.",
                    );
                    continue;
                }
                let Some(code) = query_param(&query, "code").filter(|code| !code.is_empty()) else {
                    respond(
                        &mut stream,
                        400,
                        "Authorization failed",
                        "The redirect carried no authorization code.",
                    );
                    continue;
                };
                respond(
                    &mut stream,
                    200,
                    "Authorization complete",
                    "You can close this tab and return to rpi.",
                );
                return Ok(code);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(format!("the loopback callback failed: {error}")),
        }
    }
}

/// Hand the authorization URL to the platform opener with no inherited stdio —
/// a TUI child that writes to stdout would corrupt the terminal.
fn open_browser(url: &str) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    let (program, args): (&str, Vec<&str>) = ("open", vec![url]);
    #[cfg(target_os = "windows")]
    let (program, args): (&str, Vec<&str>) = ("cmd", vec!["/c", "start", "", url]);
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let (program, args): (&str, Vec<&str>) = ("xdg-open", vec![url]);
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("cannot open a browser with `{program}`: {error}"))
}

// ---------------------------------------------------------------------------
// Worker thread
// ---------------------------------------------------------------------------

/// Run blocking OAuth work on a dedicated thread. Command handlers run on the
/// TUI thread and `rpi auth *_oauth` runs inside a Tokio runtime, so neither
/// may host a nested `block_on`.
fn on_worker<T: Send>(body: impl FnOnce() -> T + Send) -> Result<T, String> {
    std::thread::scope(|scope| {
        scope
            .spawn(body)
            .join()
            .map_err(|_| "the MCP OAuth worker panicked".to_string())
    })
}

// ---------------------------------------------------------------------------
// Progress reporting and live flow state
// ---------------------------------------------------------------------------

/// Where a sign-in currently is. Implemented by the live flow registry and by
/// a silent no-op in tests.
pub(crate) trait Progress: Send + Sync {
    fn stage(&self, label: &str);
    fn url(&self, url: &str);
}

pub(crate) struct Silent;

impl Progress for Silent {
    fn stage(&self, _label: &str) {}
    fn url(&self, _url: &str) {}
}

#[derive(Debug, Clone, Default)]
pub struct FlowView {
    pub stage: String,
    /// Set while the flow is waiting for the browser, so `/mcp-oauth status` can
    /// show the URL again if the browser never opened.
    pub url: Option<String>,
    /// Terminal message: success or failure. Cleared when a new flow starts.
    pub result: Option<String>,
    pub failed: bool,
    pub pending_input: bool,
}

struct Flow {
    stage: String,
    url: Option<String>,
    result: Option<String>,
    failed: bool,
    cancel: bool,
    updated: Instant,
}

/// Results stay readable for a while so `/mcp-oauth status` can report how the
/// last attempt ended, then age out so the registry cannot grow without bound.
const RESULT_TTL: Duration = Duration::from_secs(600);

static FLOWS: Mutex<BTreeMap<String, Flow>> = Mutex::new(BTreeMap::new());
/// PKCE material parked when the loopback redirect timed out, so a code the
/// user pastes afterwards can still finish the exchange (the host's
/// `begin` -> `interaction` -> `exchange` contract).
static PENDING: Mutex<BTreeMap<String, PendingExchange>> = Mutex::new(BTreeMap::new());

struct PendingExchange {
    auth: ServerAuth,
    url: String,
    verifier: String,
    state: String,
    created: Instant,
}

fn with_flows<T>(body: impl FnOnce(&mut BTreeMap<String, Flow>) -> T) -> T {
    let mut flows = FLOWS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    body(&mut flows)
}

fn with_pending<T>(body: impl FnOnce(&mut BTreeMap<String, PendingExchange>) -> T) -> T {
    let mut pending = PENDING
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    body(&mut pending)
}

struct LiveProgress {
    server: String,
}

impl Progress for LiveProgress {
    fn stage(&self, label: &str) {
        with_flows(|flows| {
            if let Some(flow) = flows.get_mut(&self.server) {
                flow.stage = label.to_string();
            }
        });
    }
    fn url(&self, url: &str) {
        with_flows(|flows| {
            if let Some(flow) = flows.get_mut(&self.server) {
                flow.url = Some(url.to_string());
                flow.stage = "waiting for you to finish signing in in the browser".to_string();
            }
        });
    }
}

/// Whether a flow is still running (as opposed to finished with a result).
fn is_running(flow: &Flow) -> bool {
    flow.result.is_none()
}

pub fn status(server: &str) -> Option<FlowView> {
    let pending = with_pending(|pending| pending.contains_key(server));
    with_flows(|flows| {
        flows.get(server).map(|flow| FlowView {
            stage: flow.stage.clone(),
            url: flow.url.clone(),
            result: flow.result.clone(),
            failed: flow.failed,
            pending_input: pending,
        })
    })
}

pub fn statuses() -> Vec<(String, FlowView)> {
    let pending: Vec<String> = with_pending(|pending| pending.keys().cloned().collect());
    with_flows(|flows| {
        flows
            .iter()
            .map(|(server, flow)| {
                (
                    server.clone(),
                    FlowView {
                        stage: flow.stage.clone(),
                        url: flow.url.clone(),
                        result: flow.result.clone(),
                        failed: flow.failed,
                        pending_input: pending.iter().any(|name| name == server),
                    },
                )
            })
            .collect()
    })
}

/// Forget finished flows whose result has been on screen long enough.
fn prune_finished_flows() {
    with_flows(|flows| {
        flows.retain(|_, flow| is_running(flow) || flow.updated.elapsed() < RESULT_TTL);
    });
    with_pending(|pending| {
        pending.retain(|_, entry| entry.created.elapsed() < RESULT_TTL);
    });
}

pub fn cancel(server: &str) -> bool {
    with_flows(|flows| match flows.get_mut(server) {
        Some(flow) if is_running(flow) => {
            flow.cancel = true;
            flow.stage = "cancelling".to_string();
            true
        }
        _ => false,
    })
}

fn finished(server: &str, result: Result<String, String>) {
    with_flows(|flows| {
        if let Some(flow) = flows.get_mut(server) {
            match result {
                Ok(message) => {
                    flow.stage = "done".to_string();
                    flow.result = Some(message);
                    flow.failed = false;
                }
                Err(error) => {
                    flow.stage = "failed".to_string();
                    flow.result = Some(error);
                    flow.failed = true;
                }
            }
            flow.url = None;
            flow.updated = Instant::now();
        }
    });
}

// ---------------------------------------------------------------------------
// The flow
// ---------------------------------------------------------------------------

fn authorize_url(auth: &ServerAuth, challenge: &str, state: &str) -> Result<String, String> {
    let mut url = Url::parse(&auth.authorization_endpoint)
        .map_err(|error| format!("invalid authorization_endpoint: {error}"))?;
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("response_type", "code")
            .append_pair("client_id", &auth.client_id)
            .append_pair("redirect_uri", &auth.redirect_uri())
            .append_pair("code_challenge", challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", state);
        if !auth.scopes.is_empty() {
            query.append_pair("scope", &auth.scopes.join(" "));
        }
    }
    Ok(url.to_string())
}

/// Run one sign-in. Split out from [`start`] so tests can drive the loopback
/// callback themselves and suppress the browser.
fn run_flow(
    dir: &Path,
    server: &str,
    server_url: &str,
    open_browser_ui: bool,
    progress: &dyn Progress,
    cancel: &(dyn Fn() -> bool + Send + Sync),
) -> Result<String, String> {
    // Everything below performs blocking I/O, so it runs on a worker thread.
    on_worker(|| -> Result<String, String> {
        let client = http_client()?;
        let mut auth = discover(&client, server_url, progress)?;

        progress.stage("preparing the loopback callback");
        let cached = read_server_auth_in(dir, server).unwrap_or_default();
        // Prefer the stable port so the dynamically registered `redirect_uri`
        // stays reusable across sign-ins; an ephemeral port is the fallback.
        let preferred = if cached.port == 0 {
            DEFAULT_LOOPBACK_PORT
        } else {
            cached.port
        };
        let (listener, port) = bind_listener(preferred)?;
        auth.port = port;
        if !cached.client_id.is_empty() && cached.port == port {
            // Reuse the registration only when the redirect_uri we registered
            // is byte-identical to the one we are about to use.
            auth.client_id = cached.client_id.clone();
            auth.client_secret = cached.client_secret.clone();
        }
        if auth.client_id.is_empty() {
            if auth.registration_endpoint.is_empty() {
                return Err(format!(
                    "{} requires OAuth but offers no dynamic client registration; \
                     it needs a pre-registered client id, which the adapter cannot supply",
                    auth.issuer
                ));
            }
            progress.stage("registering this client with the authorization server");
            let redirect_uri = auth.redirect_uri();
            let (client_id, client_secret) =
                register_client(&client, &auth.registration_endpoint, &redirect_uri)?;
            auth.client_id = client_id;
            auth.client_secret = client_secret;
        }
        write_server_auth_in(dir, server, &auth)?;

        let verifier = random_token(32);
        let state = random_token(16);
        let url = authorize_url(&auth, &pkce_challenge(&verifier), &state)?;
        progress.url(&url);

        if open_browser_ui {
            if let Err(error) = open_browser(&url) {
                // Keep waiting: the user can still open the URL by hand, and
                // `/mcp-oauth status` prints it.
                progress.stage(&format!("{error}; open the URL from /mcp-oauth status"));
            }
        }

        let code = match wait_for_callback(&listener, &state, cancel) {
            Ok(code) => code,
            Err(error) if error.contains("timed out") => {
                // Park the PKCE material so a pasted code can still finish the
                // exchange later (the host's `begin` -> `exchange` contract).
                with_pending(|pending| {
                    pending.insert(
                        server.to_string(),
                        PendingExchange {
                            auth: auth.clone(),
                            url: url.clone(),
                            verifier: verifier.clone(),
                            state: state.clone(),
                            created: Instant::now(),
                        },
                    );
                });
                return Err(error);
            }
            Err(error) => return Err(error),
        };

        progress.stage("exchanging the authorization code for a token");
        let token = exchange_code_in(&client, &auth, &code, &verifier)?;
        write_token_in(dir, server, &token)?;
        Ok(format!("Signed in to {server}."))
    })?
}

/// Start a sign-in in the background. Returns once the flow is registered.
/// `config` names the MCP config to resolve the server URL from; `None` uses
/// the discovered config (the host ABI path, which carries no config argument).
pub fn start(config: Option<&Path>, server: &str, open_browser_ui: bool) -> Result<(), String> {
    let url = crate::commands::server_url_in(config, server)?;
    let dir = store_dir()?;
    prune_finished_flows();
    let started = with_flows(|flows| {
        if flows.get(server).is_some_and(is_running) {
            return false;
        }
        flows.insert(
            server.to_string(),
            Flow {
                stage: "starting".to_string(),
                url: None,
                result: None,
                failed: false,
                cancel: false,
                updated: Instant::now(),
            },
        );
        true
    });
    if !started {
        return Err(format!(
            "a sign-in for {server} is already in progress; watch it with `/mcp-oauth status` or stop it with `/mcp-oauth cancel {server}`"
        ));
    }
    let owned = server.to_string();
    std::thread::spawn(move || {
        let progress = LiveProgress {
            server: owned.clone(),
        };
        let cancel_server = owned.clone();
        let cancel = move || {
            with_flows(|flows| {
                flows
                    .get(&cancel_server)
                    .map(|flow| flow.cancel)
                    .unwrap_or(false)
            })
        };
        let outcome = run_flow(&dir, &owned, &url, open_browser_ui, &progress, &cancel);
        let outcome = outcome.map(|_| {
            with_pending(|pending| pending.remove(&owned));
            format!("Signed in to {owned}. Tokens stored in the host credential store.")
        });
        finished(&owned, outcome);
    });
    Ok(())
}

/// Finish a flow from a code the user pasted. Used by the host's
/// `begin` -> `interaction` -> `exchange` contract.
fn complete_with_code(dir: &Path, server: &str, value: &str) -> Result<Token, String> {
    let code = extract_code(value);
    if code.is_empty() {
        return Err("no authorization code was supplied".into());
    }
    let pending = with_pending(|pending| pending.remove(server)).ok_or(
        "no sign-in is waiting for a code; run `/mcp-oauth login <server>` again",
    )?;
    if pending.created.elapsed() > FLOW_TIMEOUT {
        return Err("the pending sign-in expired; start it again".into());
    }
    // A pasted redirect URL carries the `state` we generated: reject a mismatch
    // so a code from another flow cannot be exchanged into this one. A bare
    // code has no state to compare and is accepted as-is.
    if let Some((_, query)) = value.split_once('?') {
        let query = query.split('#').next().unwrap_or(query);
        if let Some(state) = query_param(query, "state") {
            if state != pending.state {
                return Err(
                    "the pasted redirect does not belong to this sign-in (state mismatch); start it again"
                        .into(),
                );
            }
        }
    }
    on_worker(|| -> Result<Token, String> {
        let client = http_client()?;
        let token = exchange_code_in(&client, &pending.auth, &code, &pending.verifier)?;
        write_token_in(dir, server, &token)?;
        Ok(token)
    })?
}

/// Accept either a bare code or the full redirect URL the browser landed on.
pub(crate) fn extract_code(value: &str) -> String {
    let trimmed = value.trim();
    if let Some((_, query)) = trimmed.split_once('?') {
        if let Some(code) = query_param(query, "code") {
            return code.trim().to_string();
        }
    }
    let stripped = trimmed.strip_prefix("code=").unwrap_or(trimmed);
    decode_component(stripped.split('&').next().unwrap_or(stripped))
        .trim()
        .to_string()
}

/// Revoke at the authorization server (best effort) and drop the credential.
pub fn logout(server: &str) -> Result<String, String> {
    let dir = store_dir()?;
    let token = read_token_in(&dir, server)?;
    let auth = read_server_auth_in(&dir, server).unwrap_or_default();
    let mut revoked = false;
    if let Some(token) = &token {
        if !auth.revocation_endpoint.is_empty() {
            on_worker(|| {
                let client = http_client()?;
                revoke_in(&client, &auth, token)
            })??;
            revoked = true;
        }
    }
    let removed = delete_token_in(&dir, server)?;
    Ok(match (removed, revoked) {
        (true, true) => format!("Revoked and removed stored credentials for {server}."),
        (true, false) => format!(
            "Removed stored credentials for {server} (the server exposes no revocation endpoint)."
        ),
        (false, true) => format!("Revoked the token for {server}; nothing was stored locally."),
        (false, false) => format!("No stored credentials for {server}."),
    })
}

/// Attach a valid bearer token to MCP requests, refreshing a stored token that
/// has expired. Returns `None` when the server has no stored credential.
pub fn bearer(server: &str) -> Option<String> {
    let dir = store_dir().ok()?;
    let token = read_token_in(&dir, server).ok().flatten()?;
    if !token.is_expired(now_epoch()) || token.refresh.is_empty() {
        return Some(token.access);
    }
    let auth = read_server_auth_in(&dir, server).unwrap_or_default();
    if auth.token_endpoint.is_empty() {
        return Some(token.access);
    }
    on_worker(|| {
        let client = http_client()?;
        let refreshed = refresh_in(&client, &auth, &token.refresh)?;
        write_token_in(&dir, server, &refreshed)?;
        Ok::<Token, String>(refreshed)
    })
    .ok()
    .and_then(Result::ok)
    .map(|refreshed| refreshed.access)
    .or(Some(token.access))
}

/// Report the stored credential without exposing the token itself.
pub fn credential_state(server: &str) -> (bool, Option<i64>) {
    match store_dir().ok().and_then(|dir| read_token_in(&dir, server).ok().flatten()) {
        Some(token) => (true, Some(token.expires)),
        None => (false, None),
    }
}

// ---------------------------------------------------------------------------
// Host ABI: begin / exchange / refresh / revoke
// ---------------------------------------------------------------------------

fn response(kind: &str, payload: Value) -> Value {
    json!({ "version": 1, "kind": kind, "payload": payload })
}

rpi_plugin_sdk::string_handler! {
/// Host entrypoint for one provider-owned OAuth action (`rpi auth` / the
/// programmatic credential API). Envelope and response shapes are documented
/// on [`handle_request`].
pub(crate) fn provider_request(raw, out, _data) {
    let request: Value = serde_json::from_str(raw).unwrap_or(Value::Null);
    let answer = handle_request(&request);
    if !out.is_available() {
        return 1;
    }
    out.write(answer.to_string());
    0
}
}

fn error_response(message: &str) -> Value {
    response("error", json!({ "message": message }))
}

fn credential_response(token: &Token) -> Value {
    response(
        "credential",
        json!({
            "access": token.access,
            "refresh": token.refresh,
            "expires_at": token.expires,
        }),
    )
}

/// Dispatch one provider-owned OAuth action from the host.
pub fn handle_request(request: &Value) -> Value {
    let action = request
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let provider = request
        .get("providerId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let input = request.get("input").cloned().unwrap_or(Value::Null);
    let Some(server) = server_name(provider) else {
        return error_response(&format!("{provider:?} is not an MCP OAuth provider"));
    };
    match action {
        "begin" => begin_action(server),
        "exchange" => exchange_action(server, &input),
        "refresh" => refresh_action(server, &input),
        "revoke" => revoke_action(server, &input),
        other => error_response(&format!("unsupported OAuth action {other:?}")),
    }
}

fn begin_action(server: &str) -> Value {
    let url = match crate::commands::server_url(server) {
        Ok(url) => url,
        Err(error) => return error_response(&error),
    };
    let dir = match store_dir() {
        Ok(dir) => dir,
        Err(error) => return error_response(&error),
    };
    // A headless caller (`rpi auth login`) gets synchronous semantics: drive the
    // whole flow and return the credential directly. The browser still opens.
    let cancel = || false;
    let cancel: &(dyn Fn() -> bool + Send + Sync) = &cancel;
    let outcome = run_flow(&dir, server, &url, true, &Silent, cancel);
    match outcome {
        Ok(_) => match read_token_in(&dir, server) {
            Ok(Some(token)) => credential_response(&token),
            Ok(None) => error_response("the token exchange produced no stored credential"),
            Err(error) => error_response(&error),
        },
        Err(error) if error.contains("timed out") => {
            // Fall back to the host's paste-a-code contract. The PKCE material
            // is parked by `run_flow`, so `exchange` can finish the job.
            let url = with_pending(|pending| pending.get(server).map(|entry| entry.url.clone()))
                .unwrap_or_default();
            response(
                "interaction",
                json!({
                    "display": {
                        "title": format!("Sign in to the MCP server {server}"),
                        "url": url,
                        "instructions": [
                            "Complete the sign-in in your browser, then paste the authorization code (or the full redirect URL)."
                        ]
                    }
                }),
            )
        }
        Err(error) => error_response(&error),
    }
}

fn exchange_action(server: &str, input: &Value) -> Value {
    let Some(value) = input.get("value").and_then(Value::as_str) else {
        return error_response("exchange requires a `value` with the authorization code");
    };
    let dir = match store_dir() {
        Ok(dir) => dir,
        Err(error) => return error_response(&error),
    };
    match complete_with_code(&dir, server, value) {
        Ok(token) => credential_response(&token),
        Err(error) => error_response(&error),
    }
}

fn refresh_action(server: &str, input: &Value) -> Value {
    let credential = input.get("credential").cloned().unwrap_or(Value::Null);
    let refresh = credential
        .get("refresh")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if refresh.is_empty() {
        return error_response("refresh requires a refresh token");
    }
    let dir = match store_dir() {
        Ok(dir) => dir,
        Err(error) => return error_response(&error),
    };
    let auth = match read_server_auth_in(&dir, server) {
        Ok(auth) if !auth.token_endpoint.is_empty() => auth,
        Ok(_) => {
            return error_response(
                "no cached authorization-server metadata for this MCP server; run a fresh sign-in",
            )
        }
        Err(error) => return error_response(&error),
    };
    let outcome = on_worker(|| -> Result<Token, String> {
        let client = http_client()?;
        let token = refresh_in(&client, &auth, &refresh)?;
        write_token_in(&dir, server, &token)?;
        Ok(token)
    });
    match outcome {
        Ok(Ok(token)) => credential_response(&token),
        Ok(Err(error)) => error_response(&error),
        Err(error) => error_response(&error),
    }
}

fn revoke_action(server: &str, input: &Value) -> Value {
    let credential = input.get("credential").cloned().unwrap_or(Value::Null);
    let token = Token {
        access: credential
            .get("access")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        refresh: credential
            .get("refresh")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        expires: 0,
    };
    let Ok(dir) = store_dir() else {
        return error_response("cannot locate the credential store");
    };
    let auth = read_server_auth_in(&dir, server).unwrap_or_default();
    match on_worker(|| {
        let client = http_client()?;
        revoke_in(&client, &auth, &token)
    }) {
        Ok(Ok(())) => {
            let _ = delete_token_in(&dir, server);
            response("success", json!({}))
        }
        Ok(Err(error)) => error_response(&error),
        Err(error) => error_response(&error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rpi-mcp-oauth-{}-{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn provider_ids_round_trip_and_ignore_foreign_prefixes() {
        assert_eq!(provider_id("docs"), "mcp:docs");
        assert_eq!(server_name("mcp:docs"), Some("docs"));
        assert_eq!(server_name("docs"), None);
        assert_eq!(server_name("mcp"), None);
    }

    #[test]
    fn manifest_declares_pkce_and_credential_capabilities() {
        let value: Value = serde_json::from_str(&manifest("docs")).unwrap();
        assert_eq!(value["id"], "mcp:docs");
        assert_eq!(value["displayName"], "MCP server: docs");
        assert_eq!(value["supportsPkce"], true);
        assert_eq!(value["capabilities"]["refresh"], true);
        assert_eq!(value["capabilities"]["revoke"], true);
        // A manifest travels to the host and must never carry secret material.
        let text = manifest("docs");
        assert!(!text.contains("access"), "{text}");
        assert!(!text.contains("verifier"), "{text}");
    }

    #[test]
    fn handle_request_rejects_unknown_actions_and_foreign_providers() {
        let unknown = handle_request(&json!({"action": "nope", "providerId": "mcp:docs"}));
        assert_eq!(unknown["version"], 1);
        assert_eq!(unknown["kind"], "error");
        assert!(unknown["payload"]["message"]
            .as_str()
            .unwrap()
            .contains("unsupported OAuth action"));

        let foreign = handle_request(&json!({"action": "begin", "providerId": "openai"}));
        assert_eq!(foreign["kind"], "error");
        assert!(foreign["payload"]["message"]
            .as_str()
            .unwrap()
            .contains("not an MCP OAuth provider"));
    }

    #[test]
    fn an_actionless_request_is_a_clean_error() {
        let response = handle_request(&Value::Null);
        assert_eq!(response["version"], 1);
        assert_eq!(response["kind"], "error");
        assert!(response["payload"]["message"].is_string());
    }

    #[test]
    fn credential_responses_use_the_host_field_names() {
        let token = Token {
            access: "a".into(),
            refresh: "r".into(),
            expires: 42,
        };
        let value = credential_response(&token);
        assert_eq!(value["version"], 1);
        assert_eq!(value["kind"], "credential");
        assert_eq!(value["payload"]["access"], "a");
        assert_eq!(value["payload"]["refresh"], "r");
        // The host reads `expires_at`; `expires` is the on-disk name.
        assert_eq!(value["payload"]["expires_at"], 42);
        assert!(value["payload"].get("expires").is_none());
    }

    #[test]
    fn token_store_round_trips_in_the_host_shape() {
        let dir = temp_dir("token");
        assert_eq!(read_token_in(&dir, "docs").unwrap(), None);

        let token = Token {
            access: "acc".into(),
            refresh: "ref".into(),
            expires: 100,
        };
        write_token_in(&dir, "docs", &token).unwrap();
        assert_eq!(read_token_in(&dir, "docs").unwrap(), Some(token));

        let stored =
            serde_json::from_str::<Value>(&std::fs::read_to_string(auth_path(&dir)).unwrap())
                .unwrap();
        assert_eq!(stored["mcp:docs"]["type"], "oauth");
        assert_eq!(stored["mcp:docs"]["access"], "acc");
        assert_eq!(stored["mcp:docs"]["expires"], 100);

        // Another server's credential is untouched by writes to this one.
        write_token_in(
            &dir,
            "other",
            &Token {
                access: "o".into(),
                refresh: String::new(),
                expires: 0,
            },
        )
        .unwrap();
        assert_eq!(read_token_in(&dir, "docs").unwrap().unwrap().access, "acc");

        assert!(delete_token_in(&dir, "docs").unwrap());
        assert_eq!(read_token_in(&dir, "docs").unwrap(), None);
        // Deleting twice reports that nothing was removed.
        assert!(!delete_token_in(&dir, "docs").unwrap());
        // The unrelated credential survived.
        assert_eq!(read_token_in(&dir, "other").unwrap().unwrap().access, "o");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_non_oauth_entry_is_not_a_credential() {
        let dir = temp_dir("api-key");
        let mut map = BTreeMap::new();
        map.insert(
            "mcp:docs".to_string(),
            json!({"type": "api_key", "key": "secret"}),
        );
        write_secure_json(&auth_path(&dir), &map).unwrap();
        assert_eq!(read_token_in(&dir, "docs").unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn token_expiry_treats_zero_as_unknown() {
        let token = |expires| Token {
            access: "a".into(),
            refresh: String::new(),
            expires,
        };
        assert!(!token(0).is_expired(1_000_000));
        assert!(token(10).is_expired(10));
        assert!(!token(11).is_expired(10));
    }

    #[test]
    fn server_auth_cache_round_trips_and_defaults_to_empty() {
        let dir = temp_dir("cache");
        assert_eq!(
            read_server_auth_in(&dir, "docs").unwrap(),
            ServerAuth::default()
        );
        let auth = ServerAuth {
            issuer: "https://auth.example.com".into(),
            authorization_endpoint: "https://auth.example.com/authorize".into(),
            token_endpoint: "https://auth.example.com/token".into(),
            client_id: "client-1".into(),
            port: 33418,
            scopes: vec!["read".into()],
            ..ServerAuth::default()
        };
        write_server_auth_in(&dir, "docs", &auth).unwrap();
        assert_eq!(read_server_auth_in(&dir, "docs").unwrap(), auth);
        assert_eq!(
            auth.redirect_uri(),
            "http://127.0.0.1:33418/callback"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pkce_challenge_matches_the_rfc7636_test_vector() {
        // RFC 7636 Appendix B.
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        assert_eq!(
            pkce_challenge(verifier),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        // base64url without padding, as the spec requires.
        assert!(!pkce_challenge(verifier).contains('='));
    }

    #[test]
    fn random_tokens_are_url_safe_entropy() {
        let first = random_token(32);
        assert_ne!(first, random_token(32));
        assert!(first
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        // 32 bytes is 43 base64url characters once padding is dropped.
        assert_eq!(first.len(), 43);
        assert!(!random_token(16).contains('='));
    }

    #[test]
    fn resource_metadata_param_handles_quoted_and_bare_forms() {
        assert_eq!(
            resource_metadata_param(
                "Bearer resource_metadata=\"https://x/.well-known/oauth-protected-resource\""
            ),
            Some("https://x/.well-known/oauth-protected-resource".to_string())
        );
        assert_eq!(
            resource_metadata_param("Bearer error=\"x\", resource_metadata=https://y/md"),
            Some("https://y/md".to_string())
        );
        assert_eq!(resource_metadata_param("Bearer realm=\"x\""), None);
        assert_eq!(resource_metadata_param("resource_metadata="), None);
    }

    #[test]
    fn well_known_urls_insert_the_segment_before_the_path() {
        let url = |value: &str, name: &str| well_known_url(&Url::parse(value).unwrap(), name)
            .unwrap()
            .to_string();
        assert_eq!(
            url("https://api.example.com/mcp/v1", "oauth-protected-resource"),
            "https://api.example.com/.well-known/oauth-protected-resource/mcp/v1"
        );
        assert_eq!(
            url("https://api.example.com", "oauth-authorization-server"),
            "https://api.example.com/.well-known/oauth-authorization-server"
        );
        // A query or fragment on the resource URL never leaks into the metadata URL.
        assert_eq!(
            url("https://api.example.com/mcp?token=1#frag", "oauth-protected-resource"),
            "https://api.example.com/.well-known/oauth-protected-resource/mcp"
        );
    }

    #[test]
    fn callback_query_only_matches_the_callback_path() {
        assert_eq!(
            callback_query("GET /callback?code=a&state=b HTTP/1.1"),
            Some("code=a&state=b".to_string())
        );
        assert_eq!(
            callback_query("GET /callback HTTP/1.1"),
            Some(String::new())
        );
        assert_eq!(callback_query("GET /other?code=a HTTP/1.1"), None);
        assert_eq!(callback_query("GET"), None);
    }

    #[test]
    fn query_params_and_components_are_percent_decoded() {
        assert_eq!(
            query_param("code=a%2Fb%20c&state=x+y", "code"),
            Some("a/b c".to_string())
        );
        assert_eq!(query_param("state=x+y", "state"), Some("x y".to_string()));
        assert_eq!(query_param("code=a", "state"), None);
        assert_eq!(decode_component("a%2Fb%2Bc"), "a/b+c");
        // A malformed escape is preserved rather than dropped.
        assert_eq!(decode_component("%zz"), "%zz");
        assert_eq!(decode_component("100%"), "100%");
    }

    #[test]
    fn extract_code_accepts_a_bare_code_or_a_redirect_url() {
        assert_eq!(extract_code("  abc123  "), "abc123");
        assert_eq!(extract_code("code=abc123&state=s"), "abc123");
        assert_eq!(
            extract_code("http://127.0.0.1:33418/callback?code=abc%2F1&state=s"),
            "abc/1"
        );
        assert_eq!(extract_code(""), "");
    }
}
