use super::normalize::{normalize_config_token, normalize_domain};
use crate::memory::{
    MemoryStore, QdrantSemanticConfig, ReflectionParams, RetrievalStrategy, SqliteVecConfig,
};
use crate::orchestrator::MemoryHydrationSettings;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MemoryConfig {
    pub backend: Arc<str>,
    pub path: Option<PathBuf>,
    pub default_domain: Arc<str>,
    pub hydration_enabled: bool,
    pub hydrate_strategy: Arc<str>,
    pub hydrate_max_fragments: usize,
    pub hydrate_max_estimated_tokens: usize,
    pub hydrate_stores: Vec<Arc<str>>,
    pub semantic_backend: Arc<str>,
    pub qdrant: MemoryQdrantConfig,
    pub sqlite_vec: MemorySqliteVecConfig,
    pub episode_recording_enabled: bool,
    pub reflection: MemoryReflectionConfig,
    pub retention: MemoryRetentionConfig,
    pub policy: MemoryPolicyConfig,
    pub shared_domains: Vec<MemorySharedDomainConfig>,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            backend: Arc::from("sqlite"),
            path: None,
            default_domain: Arc::from("general"),
            hydration_enabled: false,
            hydrate_strategy: Arc::from("hybrid"),
            hydrate_max_fragments: 5,
            hydrate_max_estimated_tokens: 1_200,
            hydrate_stores: vec![Arc::from("semantic"), Arc::from("episodic")],
            semantic_backend: Arc::from("none"),
            qdrant: MemoryQdrantConfig::default(),
            sqlite_vec: MemorySqliteVecConfig::default(),
            episode_recording_enabled: false,
            reflection: MemoryReflectionConfig::default(),
            retention: MemoryRetentionConfig::default(),
            policy: MemoryPolicyConfig::default(),
            shared_domains: Vec::new(),
        }
    }
}

/// `[memory.reflection]`: a scheduled whole-memory maintenance sweep that
/// promotes repeated episodes into semantic facts, supersedes contradicted
/// facts, and rebuilds the lexical index. Disabled in the conservative default;
/// the deployment `agent.toml` opts in (mirroring `episode_recording_enabled`).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MemoryReflectionConfig {
    pub enabled: bool,
    /// Cron expression (minute-resolution) for the sweep.
    pub schedule: Arc<str>,
    /// Minimum repeated episodes (by summary) before promotion to a semantic
    /// fact. Floored at 2 by the reflection engine.
    pub min_episode_repetitions: usize,
    pub rebuild_lexical_index: bool,
}

impl Default for MemoryReflectionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            schedule: Arc::from("0 3 * * *"),
            min_episode_repetitions: 2,
            rebuild_lexical_index: true,
        }
    }
}

impl MemoryReflectionConfig {
    pub fn params(&self) -> ReflectionParams {
        ReflectionParams {
            min_episode_repetitions: self.min_episode_repetitions,
            rebuild_lexical_index: self.rebuild_lexical_index,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MemorySqliteVecConfig {
    pub table: Arc<str>,
    pub vector_dimensions: usize,
}

impl Default for MemorySqliteVecConfig {
    fn default() -> Self {
        let defaults = SqliteVecConfig::default();
        Self {
            table: defaults.table,
            vector_dimensions: defaults.vector_dimensions,
        }
    }
}

impl From<&MemorySqliteVecConfig> for SqliteVecConfig {
    fn from(config: &MemorySqliteVecConfig) -> Self {
        Self {
            table: Arc::clone(&config.table),
            vector_dimensions: config.vector_dimensions,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MemoryQdrantConfig {
    pub url: Arc<str>,
    pub collection: Arc<str>,
    pub vector_name: Option<Arc<str>>,
    pub vector_dimensions: usize,
    pub api_key: Option<Arc<str>>,
    pub timeout_ms: u64,
}

impl Default for MemoryQdrantConfig {
    fn default() -> Self {
        let defaults = QdrantSemanticConfig::default();
        Self {
            url: defaults.url,
            collection: defaults.collection,
            vector_name: defaults.vector_name,
            vector_dimensions: defaults.vector_dimensions,
            api_key: defaults.api_key,
            timeout_ms: defaults.timeout_ms,
        }
    }
}

impl From<&MemoryQdrantConfig> for QdrantSemanticConfig {
    fn from(config: &MemoryQdrantConfig) -> Self {
        Self {
            url: Arc::clone(&config.url),
            collection: Arc::clone(&config.collection),
            vector_name: config.vector_name.clone(),
            vector_dimensions: config.vector_dimensions,
            api_key: config.api_key.clone(),
            timeout_ms: config.timeout_ms,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MemoryRetentionConfig {
    pub max_records: Option<usize>,
    pub max_bytes: Option<usize>,
    pub max_age_days: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MemoryPolicyConfig {
    pub writes: Arc<str>,
    pub forgets: Arc<str>,
    pub shared_writes: bool,
}

impl Default for MemoryPolicyConfig {
    fn default() -> Self {
        Self {
            writes: Arc::from("ask_user"),
            forgets: Arc::from("ask_user"),
            shared_writes: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MemorySharedDomainConfig {
    pub name: Arc<str>,
    pub read: bool,
    pub write: bool,
}

impl Default for MemorySharedDomainConfig {
    fn default() -> Self {
        Self {
            name: Arc::from("general"),
            read: true,
            write: false,
        }
    }
}

impl MemoryConfig {
    pub fn validate(&mut self) -> Result<(), String> {
        self.backend = Arc::from(normalize_config_token(&self.backend));
        match self.backend.as_ref() {
            "sqlite" | "memory.sqlite" | "in_memory" | "memory.in_memory" => {}
            other => {
                return Err(format!(
                    "unknown memory backend '{other}'; expected sqlite or in_memory"
                ));
            }
        }
        self.hydrate_strategy = Arc::from(normalize_config_token(&self.hydrate_strategy));
        parse_retrieval_strategy(&self.hydrate_strategy)?;
        self.semantic_backend = Arc::from(normalize_config_token(&self.semantic_backend));
        match self.semantic_backend.as_ref() {
            "none" | "qdrant" | "memory.qdrant" | "sqlite" | "memory.sqlite" | "sqlite_vec"
            | "memory.sqlite_vec" | "vector" => {}
            other => {
                return Err(format!(
                    "unknown memory semantic_backend '{other}'; expected none, sqlite/sqlite_vec, qdrant, or vector"
                ));
            }
        }
        validate_qdrant_config(&self.qdrant)?;
        validate_sqlite_vec_config(&self.sqlite_vec)?;

        self.default_domain = normalize_domain(&self.default_domain, "memory.default_domain")?;
        if self.hydrate_max_fragments == 0 {
            return Err("memory.hydrate_max_fragments must be greater than 0".to_owned());
        }
        if self.hydrate_max_estimated_tokens == 0 {
            return Err("memory.hydrate_max_estimated_tokens must be greater than 0".to_owned());
        }
        if self.hydrate_stores.is_empty() {
            return Err("memory.hydrate_stores must include at least one store".to_owned());
        }
        for store in &self.hydrate_stores {
            parse_memory_store(store)?;
        }
        validate_optional_budget(self.retention.max_records, "memory.retention.max_records")?;
        validate_optional_budget(self.retention.max_bytes, "memory.retention.max_bytes")?;
        if self.retention.max_age_days == Some(0) {
            return Err("memory.retention.max_age_days must be greater than 0".to_owned());
        }
        self.policy.writes = Arc::from(normalize_config_token(&self.policy.writes));
        self.policy.forgets = Arc::from(normalize_config_token(&self.policy.forgets));
        validate_memory_policy(&self.policy.writes, "memory.policy.writes")?;
        validate_memory_policy(&self.policy.forgets, "memory.policy.forgets")?;
        for domain in &mut self.shared_domains {
            domain.name = normalize_domain(&domain.name, "memory.shared_domains.name")?;
        }
        Ok(())
    }

    pub fn backend_is_in_memory(&self) -> bool {
        matches!(self.backend.as_ref(), "in_memory" | "memory.in_memory")
    }

    pub fn semantic_backend_is_qdrant(&self) -> bool {
        matches!(self.semantic_backend.as_ref(), "qdrant" | "memory.qdrant")
    }

    pub fn semantic_backend_is_sqlite_vec(&self) -> bool {
        matches!(
            self.semantic_backend.as_ref(),
            "sqlite" | "memory.sqlite" | "sqlite_vec" | "memory.sqlite_vec"
        )
    }

    pub fn hydration_settings(&self) -> Result<MemoryHydrationSettings, String> {
        Ok(MemoryHydrationSettings {
            enabled: self.hydration_enabled,
            max_fragments: self.hydrate_max_fragments,
            max_estimated_tokens: self.hydrate_max_estimated_tokens,
            stores: self
                .hydrate_stores
                .iter()
                .map(|store| parse_memory_store(store))
                .collect::<Result<Vec<_>, _>>()?,
            strategy: parse_retrieval_strategy(&self.hydrate_strategy)?,
            allowed_shared_domains: self
                .shared_domains
                .iter()
                .filter(|domain| domain.read)
                .map(|domain| Arc::clone(&domain.name))
                .collect(),
        })
    }
}

pub(super) fn parse_memory_store(input: &str) -> Result<MemoryStore, String> {
    match normalize_config_token(input).as_str() {
        "working" => Ok(MemoryStore::Working),
        "episodic" => Ok(MemoryStore::Episodic),
        "semantic" => Ok(MemoryStore::Semantic),
        "procedural" => Ok(MemoryStore::Procedural),
        "audit" => Ok(MemoryStore::Audit),
        other => Err(format!(
            "unknown memory store '{other}'; expected working, episodic, semantic, procedural, or audit"
        )),
    }
}

pub(super) fn parse_retrieval_strategy(input: &str) -> Result<RetrievalStrategy, String> {
    match normalize_config_token(input).as_str() {
        "lexical" => Ok(RetrievalStrategy::Lexical),
        "recency" => Ok(RetrievalStrategy::Recency),
        "hybrid" => Ok(RetrievalStrategy::Hybrid),
        other => Err(format!(
            "unknown memory hydrate_strategy '{other}'; expected lexical, recency, or hybrid"
        )),
    }
}

fn validate_qdrant_config(config: &MemoryQdrantConfig) -> Result<(), String> {
    if config.url.trim().is_empty() {
        return Err("memory.qdrant.url must not be empty".to_owned());
    }
    if !config.url.starts_with("http://") {
        return Err("memory.qdrant.url must use http://".to_owned());
    }
    if config.collection.trim().is_empty() {
        return Err("memory.qdrant.collection must not be empty".to_owned());
    }
    if config.vector_dimensions == 0 {
        return Err("memory.qdrant.vector_dimensions must be greater than 0".to_owned());
    }
    if config.timeout_ms == 0 {
        return Err("memory.qdrant.timeout_ms must be greater than 0".to_owned());
    }
    Ok(())
}

fn validate_sqlite_vec_config(config: &MemorySqliteVecConfig) -> Result<(), String> {
    if config.table.is_empty()
        || !config
            .table
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        || config
            .table
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_digit())
    {
        return Err(
            "memory.sqlite_vec.table must be a non-empty identifier containing only letters, digits, or '_' and must not start with a digit"
                .to_owned(),
        );
    }
    if config.vector_dimensions == 0 {
        return Err("memory.sqlite_vec.vector_dimensions must be greater than 0".to_owned());
    }
    Ok(())
}

fn validate_optional_budget(value: Option<usize>, name: &str) -> Result<(), String> {
    if value == Some(0) {
        Err(format!("{name} must be greater than 0"))
    } else {
        Ok(())
    }
}

fn validate_memory_policy(input: &str, name: &str) -> Result<(), String> {
    match normalize_config_token(input).as_str() {
        "allow" | "deny" | "ask_user" => Ok(()),
        other => Err(format!(
            "{name} has unknown value '{other}'; expected allow, deny, or ask_user"
        )),
    }
}
