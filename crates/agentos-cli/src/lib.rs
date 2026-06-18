//! Library surface shared by the agentos-cli binaries (TUI + gateway).
//!
//! Currently exposes the slash-command parser and renderers so the TUI and the
//! Telegram/Feishu gateway can speak the same `/help`, `/skills`, `/crons`,
//! `/tools`, `/memory`, `/orchestrator`, `/model`, `/clear` vocabulary.

pub mod slash;

use agentos_interfaces::SemanticIndex;
use std::sync::Arc;

/// Resolve a `[memory].semantic_backend` config string to a first-party
/// extension's semantic index. This is the CLI-side extension registration arm
/// passed to `AgentRuntime::build_with`: `agentos-core` never names the
/// `agentos-memory-vector` crate — only the CLI (and this function) depends on
/// it. Unknown backends return `None`, so core falls back to its built-in
/// `sqlite_vec` / `qdrant` / `none` selection.
pub fn semantic_index_factory(backend: &str) -> Option<Arc<dyn SemanticIndex>> {
    match backend {
        "vector" => Some(Arc::new(
            agentos_memory_vector::VectorSemanticIndex::default(),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_resolves_only_the_vector_extension() {
        assert!(semantic_index_factory("vector").is_some());
        // Built-in backends fall through to core's own selection.
        assert!(semantic_index_factory("sqlite_vec").is_none());
        assert!(semantic_index_factory("qdrant").is_none());
        assert!(semantic_index_factory("none").is_none());
    }
}
