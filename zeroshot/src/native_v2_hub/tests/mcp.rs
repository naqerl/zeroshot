//! The hub daemon's listener also serves the MCP endpoint.

use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

use super::super::api::{ApiState, EXIT_CODE_HEADER, router_with_mcp};
use super::super::service::HubService;
use super::super::store::HubStore;
use super::FakeDriver;
use crate::native_v2_target::FileTargetRegistry;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn daemon_router_requires_auth_and_serves_mcp() {
    let directory = tempfile::tempdir().expect("tempdir");
    let service = Arc::new(
        HubService::open(
            HubStore::new(directory.path().join("hub")),
            Arc::new(FakeDriver::default()),
            FileTargetRegistry::new(directory.path().join("targets.json")),
        )
        .expect("service"),
    );
    let state = Arc::new(ApiState {
        service,
        token: TOKEN.to_owned(),
        heartbeat: Arc::new(Mutex::new(json!({"schema": "zeroshot.hub.daemon/v1"}))),
        executable: None,
    });
    let (mcp_router, mcp) = crate::mcp_host::spawn_production_mcp().expect("MCP worker");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(async move {
        axum::serve(listener, router_with_mcp(state, Some(mcp_router))).await
    });
    let http = reqwest::Client::new();
    let initialize = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0"}
        }
    });
    let url = format!("http://{address}/mcp");

    let unauthorized = http
        .post(&url)
        .json(&initialize)
        .send()
        .await
        .expect("unauthenticated MCP request");
    assert_eq!(unauthorized.status(), 401);
    assert_eq!(
        unauthorized
            .headers()
            .get(EXIT_CODE_HEADER)
            .map(|value| value.as_bytes()),
        Some(&b"1"[..])
    );
    let body: Value = unauthorized.json().await.expect("unauthorized JSON");
    assert_eq!(body["code"], json!("hub.unauthorized"));

    let authorized = http
        .post(&url)
        .bearer_auth(TOKEN)
        .json(&initialize)
        .send()
        .await
        .expect("authenticated MCP request");
    assert_eq!(authorized.status(), 200);
    assert!(
        authorized
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| !value.is_empty()),
        "initialize returns a non-empty session id"
    );
    let body: Value = authorized.json().await.expect("initialize JSON");
    assert_eq!(body["result"]["serverInfo"]["name"], json!("zeroshot"));

    mcp.shutdown().await;
    server.abort();
}
