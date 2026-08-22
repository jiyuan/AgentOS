//! `[[mcp_servers]]` and `[[mcp_tools]]` — the Model Context Protocol servers
//! this deployment connects to, and the canned tools it declares inline.
//!
//! Split out of `config/mod.rs` when it grew past the module ceiling. The two
//! sections belong together: a `[[mcp_tools]]` entry names a `[[mcp_servers]]`
//! entry by id, and whether that server is a live process or a table in this
//! file is decided by its `endpoint`.

use agentos_interfaces::tool::SandboxMode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct McpServerConfig {
    /// The name this server is referred to by, in `[resources.mcp].enabled`
    /// and in each `[[mcp_tools]].server_id`.
    pub id: Arc<str>,
    /// Where the server is: an `http(s)://` URL, or a command line for a
    /// stdio server. An address written here is an operator's decision, so it
    /// is not subject to the egress policy that judges model-chosen URLs.
    pub endpoint: Arc<str>,
    /// How long one call to this server may take. Unset uses
    /// `[limits].tool_timeout_ms`.
    pub timeout_ms: Option<u64>,
    /// What this server's process may do to the filesystem
    /// (M8 / `MCP-001`, deliverable 7).
    ///
    /// Applied to the *server child*, which is the process that touches the
    /// filesystem, and stamped onto every tool it offers. Before M8 an MCP
    /// call was routed to the shell-only isolation worker, which could not run
    /// it, so the declared mode restricted nothing — `full_access` is
    /// therefore the default, because it is an honest statement of what was
    /// already happening rather than a new grant. A server that only reads
    /// should say `read_only`, and on a host with no sandbox backend it will
    /// then refuse to start rather than run unrestricted.
    pub sandbox: SandboxMode,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct McpToolConfig {
    /// Which `[[mcp_servers]]` entry serves this tool, by its `id`.
    pub server_id: Arc<str>,
    /// The tool name the model calls. Shares one namespace with the built-in
    /// tools, so it must not collide with one.
    pub name: Arc<str>,
    /// What the tool does, as the model reads it. This is the whole of what
    /// the model knows about it when choosing.
    pub description: Arc<str>,
    /// A canned response, for a statically declared tool that needs no live
    /// server — the reference `remote_echo` is one. Ignored by tools whose
    /// server answers for itself.
    pub response: Arc<str>,
    /// What this MCP-backed tool may do to the filesystem (roadmap X2).
    ///
    /// Defaults to `full_access`, which is exactly what the old
    /// `requires_isolation = false` meant: the call is made in-process by the
    /// MCP client, so there is no child process for a sandbox to restrict.
    #[serde(default)]
    pub sandbox: SandboxMode,
}

impl Default for McpToolConfig {
    fn default() -> Self {
        Self {
            server_id: Arc::from("static-mcp"),
            name: Arc::from("remote_echo"),
            description: Arc::from("Static MCP-backed tool"),
            response: Arc::from("static MCP response"),
            sandbox: SandboxMode::FullAccess,
        }
    }
}
