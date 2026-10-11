use super::tests::{cleanup_test_entry, insert_test_entry};
use super::*;
use crate::mcp::{client::McpClient, rmcp_stdio_transport::RmcpStdioTransport};
use serde_json::json;
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

struct StdioFixture {
    directory: PathBuf,
    marker: PathBuf,
}

impl StdioFixture {
    fn new(exits_after_first_initialize: bool) -> Self {
        let id = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("mcphub-rmcp-health-{}-{id}", std::process::id()));
        fs::create_dir_all(&directory).expect("create fixture directory");
        let marker = directory.join("initialized");
        if !exits_after_first_initialize {
            fs::write(&marker, b"keep alive").expect("prepare persistent fixture");
        }
        Self { directory, marker }
    }

    fn transport(&self) -> RmcpStdioTransport {
        let script = r#"
import json, os, sys, time
marker = sys.argv[1]
for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    if method == "initialize":
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "serverInfo": {"name": "local-fixture", "version": "1"}
            }
        }), flush=True)
        if not os.path.exists(marker):
            open(marker, "w").close()
            time.sleep(0.1)
            sys.exit(0)
    elif method == "tools/call":
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": request["id"],
            "error": {"code": -32602, "message": "fixture validation error"}
        }), flush=True)
"#;
        RmcpStdioTransport::new(
            "rmcp-health-fixture",
            "python3".to_string(),
            vec![
                "-u".to_string(),
                "-c".to_string(),
                script.to_string(),
                self.marker.display().to_string(),
            ],
            HashMap::new(),
        )
    }
}

impl Drop for StdioFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

#[tokio::test]
async fn exited_stdio_child_is_unhealthy_and_a_reconnect_spawns_a_live_client() {
    let fixture = StdioFixture::new(true);
    let name = format!(
        "rmcp-health-exited-child-{}-{}",
        std::process::id(),
        FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
    );
    let client = Arc::new(Mutex::new(McpClient::new(
        name.clone(),
        Box::new(fixture.transport()),
    )));
    client
        .lock()
        .await
        .connect()
        .await
        .expect("initial MCP handshake");

    tokio::time::timeout(Duration::from_secs(5), async {
        while client.lock().await.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("closed stdio service should become unhealthy");

    let idle_ms = 60_000;
    insert_test_entry(&name, client.clone(), idle_ms, 0, Instant::now()).await;
    let result = run_call(&name, &client, "after-exit", json!({}), idle_ms).await;
    assert!(
        result.is_err(),
        "the closed service should reject its next call"
    );
    assert!(
        !store().read().await.contains_key(&name),
        "the dead on-demand entry should be evicted"
    );

    client
        .lock()
        .await
        .connect()
        .await
        .expect("reconnect after child exit");
    assert!(
        client.lock().await.is_connected(),
        "the rebuilt child should be healthy"
    );
    client
        .lock()
        .await
        .disconnect()
        .await
        .expect("disconnect fixture");
}

#[tokio::test]
async fn healthy_stdio_application_error_keeps_the_on_demand_client() {
    let fixture = StdioFixture::new(false);
    let name = format!(
        "rmcp-health-application-error-{}-{}",
        std::process::id(),
        FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
    );
    let client = Arc::new(Mutex::new(McpClient::new(
        name.clone(),
        Box::new(fixture.transport()),
    )));
    client
        .lock()
        .await
        .connect()
        .await
        .expect("fixture MCP handshake");
    let idle_ms = 60_000;
    insert_test_entry(&name, client.clone(), idle_ms, 0, Instant::now()).await;

    let result = run_call(&name, &client, "invalid-arguments", json!({}), idle_ms).await;
    assert!(result.is_err(), "fixture should return a JSON-RPC error");
    {
        let map = store().read().await;
        let entry = map
            .get(&name)
            .expect("healthy application errors must preserve the entry");
        assert!(Arc::ptr_eq(&entry.client, &client));
    }
    assert!(
        client.lock().await.is_connected(),
        "a JSON-RPC error must not mark the transport dead"
    );

    cleanup_test_entry(&name, &client).await;
}
