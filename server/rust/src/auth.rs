use crate::error::AuthError;
use axum::http::{HeaderMap, Uri};
use std::collections::HashMap;

/// Helper function to extract a standard HTTP Bearer token from headers.
///
/// # Errors
/// Returns `AuthError::MissingHeader` if the `Authorization` header is not present,
/// or `AuthError::InvalidFormat` if it does not begin with `Bearer `.
pub fn extract_bearer_token(headers: &HeaderMap) -> Result<&str, AuthError> {
    let header_val = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(AuthError::MissingHeader)?
        .to_str()
        .map_err(|_| AuthError::InvalidFormat)?;

    if let Some(token) = header_val.strip_prefix("Bearer ") {
        Ok(token.trim())
    } else {
        Err(AuthError::InvalidFormat)
    }
}

/// Helper function to extract a query parameter by key from a URI.
#[must_use]
pub fn extract_query_param<'a>(uri: &'a Uri, key: &str) -> Option<&'a str> {
    let query = uri.query()?;
    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
            if k == key {
                return Some(v);
            }
        }
    }
    None
}

/// Built-in authenticator extracting standard Bearer tokens into claims.
#[derive(Clone, Copy, Debug, Default)]
pub struct BearerAuth;

impl BearerAuth {
    /// Validates and extracts the Bearer token into a `HashMap` claim with key `"token"`.
    ///
    /// # Errors
    /// Returns `AuthError` if extraction fails.
    pub fn authenticate(
        headers: &HeaderMap,
        _uri: &Uri,
    ) -> Result<HashMap<String, String>, AuthError> {
        let token = extract_bearer_token(headers)?;
        let mut claims = HashMap::with_capacity(1);
        claims.insert("token".to_string(), token.to_string());
        Ok(claims)
    }
}

/// Built-in authenticator extracting tokens from URI query string (`?token=...`).
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryAuth;

impl QueryAuth {
    /// Validates and extracts a URL query parameter into a `HashMap` claim with key `"token"`.
    ///
    /// # Errors
    /// Returns `AuthError` if the query parameter is not found.
    pub fn authenticate(
        _headers: &HeaderMap,
        uri: &Uri,
    ) -> Result<HashMap<String, String>, AuthError> {
        let token = extract_query_param(uri, "token")
            .ok_or_else(|| AuthError::Unauthorized("Missing 'token' query parameter".into()))?;
        let mut claims = HashMap::with_capacity(1);
        claims.insert("token".to_string(), token.to_string());
        Ok(claims)
    }
}
