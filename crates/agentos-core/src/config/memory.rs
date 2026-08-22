use super::normalize::{normalize_config_token, normalize_domain};
use crate::memory::{
    MemoryStore, QdrantSemanticConfig, ReflectionParams, RetentionRequest, RetrievalStrategy,
    SharedDomainGrants, SqliteVecConfig,
};
use crate::orchestrator::MemoryHydrationSettings;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
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
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
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

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
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

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
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

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryRetentionConfig {
    /// Ceiling on active memory records. Unset keeps everything.
    pub max_records: Option<usize>,
    /// Ceiling on the total stored size of active records, in bytes.
    pub max_bytes: Option<usize>,
    /// Ceiling on a record's age, in days.
    pub max_age_days: Option<u64>,
}

impl MemoryRetentionConfig {
    /// The budgets a reflection sweep applies.
    pub fn request(&self) -> RetentionRequest {
        RetentionRequest {
            store_budgets: Vec::new(),
            max_records: self.max_records,
            max_bytes: self.max_bytes,
            max_age_days: self.max_age_days,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryPolicyConfig {
    /// What happens when the model reads memory: `allow`, `ask_user`, or
    /// `deny`.
    pub reads: Arc<str>,
    /// What happens when the model writes memory.
    pub writes: Arc<str>,
    /// What happens when the model forgets a record.
    pub forgets: Arc<str>,
    /// Whether *any* write to a shared domain is permitted in this deployment.
    ///
    /// The first of three gates, and the coarsest. A write into
    /// `[[memory.shared_domains]]` also needs that domain's own `write = true`
    /// and a caller holding it — see `memory::authorize`. Off by default,
    /// because shared memory is the one scope where one conversation's writes
    /// are another's reads.
    pub shared_writes: bool,
}

impl Default for MemoryPolicyConfig {
    fn default() -> Self {
        Self {
            reads: Arc::from("allow"),
            writes: Arc::from("ask_user"),
            forgets: Arc::from("ask_user"),
            shared_writes: false,
        }
    }
}

impl MemoryPolicyConfig {
    /// The three verbs, paired with the `memory` operation each governs.
    ///
    /// Returned as a list rather than read field by field so a new operation
    /// cannot be added to the tool without something here failing to name it.
    pub fn operations(&self) -> [(&'static str, &str); 3] {
        [
            ("read", self.reads.as_ref()),
            ("write", self.writes.as_ref()),
            ("forget", self.forgets.as_ref()),
        ]
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
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
        self.policy.reads = Arc::from(normalize_config_token(&self.policy.reads));
        self.policy.writes = Arc::from(normalize_config_token(&self.policy.writes));
        self.policy.forgets = Arc::from(normalize_config_token(&self.policy.forgets));
        validate_memory_policy(&self.policy.reads, "memory.policy.reads")?;
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
            shared_domains: self.shared_domain_grants(),
            default_domain: Arc::clone(&self.default_domain),
        })
    }

    /// What a scheduled maintenance sweep does, from `[memory.reflection]`
    /// *and* `[memory.retention]`.
    ///
    /// One method over both sections rather than a `params()` on the
    /// reflection struct alone: the sweep is the only thing that applies
    /// retention, and the previous split is why `[memory.retention]` ended up
    /// unreachable — nothing that could see those budgets was on the path that
    /// needed them.
    pub fn reflection_params(&self) -> ReflectionParams {
        ReflectionParams {
            min_episode_repetitions: self.reflection.min_episode_repetitions,
            rebuild_lexical_index: self.reflection.rebuild_lexical_index,
            retention: self.retention.request(),
        }
    }

    /// What `[[memory.shared_domains]]` permit, with
    /// `[memory.policy].shared_writes` folded in.
    ///
    /// The global switch is applied here rather than at the point of decision
    /// so there is one place where "this deployment allows shared writes at
    /// all" is read. A domain marked `write = true` under
    /// `shared_writes = false` grants nothing, which is the conservative
    /// reading and the one an operator flipping the global switch off expects.
    pub fn shared_domain_grants(&self) -> SharedDomainGrants {
        SharedDomainGrants {
            readable: self
                .shared_domains
                .iter()
                .filter(|domain| domain.read)
                .map(|domain| Arc::clone(&domain.name))
                .collect(),
            writable: if self.policy.shared_writes {
                self.shared_domains
                    .iter()
                    .filter(|domain| domain.write)
                    .map(|domain| Arc::clone(&domain.name))
                    .collect()
            } else {
                Vec::new()
            },
        }
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
