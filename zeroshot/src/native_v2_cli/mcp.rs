//! MCP server over Streamable HTTP.
//!
//! Tools build the same command values as the CLI. The process entrypoint runs those values
//! through the existing executors. Run events are the durable stream `watch` already reads.

#[path = "mcp/args.rs"]
mod args;
#[path = "mcp/events.rs"]
pub(crate) mod events;
#[path = "mcp/host.rs"]
mod host;
#[path = "mcp/http.rs"]
mod http;
#[path = "mcp/serve.rs"]
mod serve;
#[path = "mcp/tools.rs"]
mod tools;

#[cfg(test)]
#[path = "mcp/tests.rs"]
mod tests;

pub use host::{McpFailure, McpHost, WatchStream};
pub use http::{McpServer, serve_http, serve_http_listener};
