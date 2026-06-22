//! Embedding computation port — **`StubEmbedder`** (deterministic, offline) and optional **`ApiEmbedder`** (HTTP).

use std::sync::Arc;

use thiserror::Error;

/// Default stub dimension (deterministic hashed bag-of-tokens).
pub const STUB_EMBED_DIM: usize = 128;

#[derive(Debug, Error)]
pub enum EmbedError {
    #[error("embedding API error: {0}")]
    Api(String),
    #[error("empty batch")]
    EmptyBatch,
    #[cfg(feature = "api-embeddings")]
    #[error("HTTP transport: {0}")]
    Http(String),
}

/// Compute dense vectors for code chunks / queries.
pub trait Embedder: Send + Sync {
    fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError>;
    fn dim(&self) -> usize;
    fn model_id(&self) -> &str;
}

/// Deterministic, dependency-free embedder for tests and offline degraded mode.
#[derive(Debug, Clone, Default)]
pub struct StubEmbedder {
    dim: usize,
    model_id: String,
}

impl StubEmbedder {
    pub fn new() -> Self {
        Self {
            dim: STUB_EMBED_DIM,
            model_id: "stub/hashed-bow".into(),
        }
    }

    pub fn with_dim(dim: usize) -> Self {
        Self {
            dim: dim.max(8),
            model_id: "stub/hashed-bow".into(),
        }
    }

    fn hash_token(token: &str) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in token.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }

    fn embed_one(&self, text: &str) -> Vec<f32> {
        let mut vec = vec![0f32; self.dim];
        for token in text.split(|c: char| !c.is_alphanumeric() && c != '_') {
            if token.is_empty() {
                continue;
            }
            let h = Self::hash_token(&token.to_lowercase());
            let idx = (h as usize) % self.dim;
            vec[idx] += 1.0;
        }
        l2_normalize(&mut vec);
        vec
    }
}

impl Embedder for StubEmbedder {
    fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Err(EmbedError::EmptyBatch);
        }
        Ok(texts.iter().map(|t| self.embed_one(t)).collect())
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }
}

#[cfg(feature = "api-embeddings")]
#[derive(Debug, Clone)]
pub struct ApiEmbedderConfig {
    pub api_url: String,
    pub model: String,
    pub api_key: Option<String>,
    pub dim: usize,
    pub batch_size: usize,
    pub timeout_ms: u64,
}

#[cfg(feature = "api-embeddings")]
impl ApiEmbedderConfig {
    pub fn from_env() -> Option<Self> {
        let api_url = std::env::var("CIS_EMBED_API_URL").ok()?;
        if api_url.is_empty() {
            return None;
        }
        Some(Self {
            api_url,
            model: std::env::var("CIS_EMBED_MODEL")
                .unwrap_or_else(|_| "text-embedding-3-small".into()),
            api_key: std::env::var("CIS_EMBED_API_KEY")
                .ok()
                .filter(|k| !k.is_empty()),
            dim: std::env::var("CIS_EMBED_DIM")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1536),
            batch_size: std::env::var("CIS_EMBED_BATCH")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(16)
                .max(1),
            timeout_ms: std::env::var("CIS_EMBED_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30_000)
                .max(1_000),
        })
    }
}

#[cfg(feature = "api-embeddings")]
#[derive(Debug)]
pub struct ApiEmbedder {
    config: ApiEmbedderConfig,
}

#[cfg(feature = "api-embeddings")]
impl ApiEmbedder {
    pub fn new(config: ApiEmbedderConfig) -> Self {
        Self { config }
    }

    /// One embedding request — **`input` is a JSON string** per this server's API contract.
    fn post_one(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let url = embeddings_endpoint_url(&self.config.api_url);
        let payload = serde_json::json!({
            "model": self.config.model,
            "input": text,
        });
        let body = serde_json::to_string(&payload).map_err(|e| EmbedError::Api(e.to_string()))?;

        let mut req = minreq::post(&url)
            .with_header("Content-Type", "application/json")
            .with_timeout(self.config.timeout_ms);
        if let Some(ref key) = self.config.api_key {
            req = req.with_header("Authorization", &format!("Bearer {}", key));
        }

        let resp = req
            .with_body(body)
            .send()
            .map_err(|e| EmbedError::Http(e.to_string()))?;
        let status = resp.status_code;
        let text = resp.as_str().map_err(|e| EmbedError::Http(e.to_string()))?;
        if status < 200 || status >= 300 {
            return Err(EmbedError::Api(format!("HTTP {}: {}", status, text)));
        }
        parse_embedding_response(text)
            .and_then(|mut v| v.pop().ok_or_else(|| EmbedError::Api("empty embedding response".into())))
    }
}

#[cfg(feature = "api-embeddings")]
fn parse_embedding_response(body: &str) -> Result<Vec<Vec<f32>>, EmbedError> {
    #[derive(serde::Deserialize)]
    struct OpenAiItem {
        embedding: Vec<f32>,
    }
    #[derive(serde::Deserialize)]
    struct OpenAiList {
        data: Vec<OpenAiItem>,
    }
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum Resp {
        OpenAi(OpenAiList),
        Single {
            embedding: Vec<f32>,
        },
        List {
            embeddings: Vec<Vec<f32>>,
        },
    }

    match serde_json::from_str::<Resp>(body) {
        Ok(Resp::OpenAi(r)) => Ok(r.data.into_iter().map(|d| d.embedding).collect()),
        Ok(Resp::Single { embedding }) => Ok(vec![embedding]),
        Ok(Resp::List { embeddings }) => Ok(embeddings),
        Err(e) => Err(EmbedError::Api(format!("unrecognized embedding JSON: {e}; body={body}"))),
    }
}

#[cfg(feature = "api-embeddings")]
impl Embedder for ApiEmbedder {
    fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Err(EmbedError::EmptyBatch);
        }
        let mut out = Vec::with_capacity(texts.len());
        // This HF-space API documents `input` as a string; one POST per text (batch_size = worker chunking only).
        for text in texts {
            out.push(self.post_one(text)?);
        }
        Ok(out)
    }

    fn dim(&self) -> usize {
        self.config.dim
    }

    fn model_id(&self) -> &str {
        &self.config.model
    }
}

/// Resolve OpenAI-compatible embeddings URL from `CIS_EMBED_API_URL`.
///
/// - `https://host` → `https://host/v1/embeddings`
/// - `https://host/v1` → `https://host/v1/embeddings` (no double `/v1`)
/// - `https://host/v1/embeddings` → unchanged
pub fn embeddings_endpoint_url(api_url: &str) -> String {
    let base = api_url.trim_end_matches('/');
    if base.ends_with("/embeddings") {
        base.to_string()
    } else if base.ends_with("/v1") {
        format!("{}/embeddings", base)
    } else {
        format!("{}/v1/embeddings", base)
    }
}

/// Build the active embedder from environment (API when configured, else stub).
pub fn embedder_from_env() -> Arc<dyn Embedder> {
    #[cfg(feature = "api-embeddings")]
    {
        if let Some(cfg) = ApiEmbedderConfig::from_env() {
            return Arc::new(ApiEmbedder::new(cfg));
        }
    }
    Arc::new(StubEmbedder::new())
}

/// Whether an external API embedder is configured (not stub-only).
pub fn api_embedder_configured() -> bool {
    #[cfg(feature = "api-embeddings")]
    {
        ApiEmbedderConfig::from_env().is_some()
    }
    #[cfg(not(feature = "api-embeddings"))]
    {
        false
    }
}

pub fn l2_normalize(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > f32::EPSILON {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f64 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| (*x as f64) * (*y as f64))
        .sum();
    dot.clamp(-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_is_deterministic() {
        let e = StubEmbedder::new();
        let a = e.embed_batch(&["authenticate user".into()]).unwrap();
        let b = e.embed_batch(&["authenticate user".into()]).unwrap();
        assert_eq!(a[0], b[0]);
    }

    #[test]
    fn stub_different_texts_differ() {
        let e = StubEmbedder::new();
        let a = e.embed_batch(&["authenticate".into()]).unwrap();
        let b = e.embed_batch(&["database pool".into()]).unwrap();
        assert_ne!(a[0], b[0]);
    }

    #[test]
    #[cfg(feature = "api-embeddings")]
    #[test]
    fn parse_openai_embedding_response() {
        let body = r#"{"object":"list","data":[{"object":"embedding","embedding":[1.0,0.0],"index":0}]}"#;
        let v = parse_embedding_response(body).unwrap();
        assert_eq!(v[0], vec![1.0, 0.0]);
    }

    #[cfg(feature = "api-embeddings")]
    #[test]
    fn embedding_request_uses_string_input() {
        let payload = serde_json::json!({
            "model": "multilingual-e5-small",
            "input": "Hello world",
        });
        let s = serde_json::to_string(&payload).unwrap();
        assert!(s.contains(r#""input":"Hello world""#));
        assert!(!s.contains(r#""input":["#));
    }

    fn embeddings_endpoint_url_no_double_v1() {
        assert_eq!(
            embeddings_endpoint_url("https://example.hf.space/v1"),
            "https://example.hf.space/v1/embeddings"
        );
        assert_eq!(
            embeddings_endpoint_url("https://api.openai.com"),
            "https://api.openai.com/v1/embeddings"
        );
        assert_eq!(
            embeddings_endpoint_url("https://host/v1/embeddings"),
            "https://host/v1/embeddings"
        );
    }

    #[test]
    fn cosine_identical_unit_vectors() {
        let v = vec![1.0f32, 0.0, 0.0];
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-6);
    }
}
