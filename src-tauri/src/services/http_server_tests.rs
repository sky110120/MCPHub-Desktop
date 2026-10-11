use super::*;
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use futures_util::{FutureExt, Stream, StreamExt};
use serde_json::{json, Value};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row,
};
use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);

struct TestRuntimeDirs {
    root: PathBuf,
    previous_home: Option<std::ffi::OsString>,
    previous_xdg_data: Option<std::ffi::OsString>,
    previous_xdg_cache: Option<std::ffi::OsString>,
    previous_appdata: Option<std::ffi::OsString>,
}

impl TestRuntimeDirs {
    fn new() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "mcphub-http-protocol-{}-{stamp}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("create isolated runtime directory");

        let previous_home = std::env::var_os("HOME");
        let previous_xdg_data = std::env::var_os("XDG_DATA_HOME");
        let previous_xdg_cache = std::env::var_os("XDG_CACHE_HOME");
        let previous_appdata = std::env::var_os("APPDATA");
        std::env::set_var("HOME", &root);
        std::env::set_var("XDG_DATA_HOME", root.join("data"));
        std::env::set_var("XDG_CACHE_HOME", root.join("cache"));
        std::env::set_var("APPDATA", root.join("appdata"));

        Self {
            root,
            previous_home,
            previous_xdg_data,
            previous_xdg_cache,
            previous_appdata,
        }
    }

    fn root(&self) -> &std::path::Path {
        &self.root
    }
}

impl Drop for TestRuntimeDirs {
    fn drop(&mut self) {
        restore_env("HOME", self.previous_home.take());
        restore_env("XDG_DATA_HOME", self.previous_xdg_data.take());
        restore_env("XDG_CACHE_HOME", self.previous_xdg_cache.take());
        restore_env("APPDATA", self.previous_appdata.take());
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn restore_env(name: &str, value: Option<std::ffi::OsString>) {
    if let Some(value) = value {
        std::env::set_var(name, value);
    } else {
        std::env::remove_var(name);
    }
}

async fn oneshot(router: &Router, request: Request<Body>) -> axum::response::Response {
    tokio::time::timeout(REQUEST_TIMEOUT, router.clone().oneshot(request))
        .await
        .expect("router request timed out")
        .expect("infallible router service")
}

fn rpc_request(path: &str, value: Value, bearer: Option<&str>) -> Request<Body> {
    let mut builder = Request::post(path)
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header("mcp-protocol-version", "2025-03-26");
    if let Some(token) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    builder
        .body(Body::from(
            serde_json::to_vec(&value).expect("serialize RPC request"),
        ))
        .expect("build RPC request")
}

async fn response_json(response: axum::response::Response) -> Value {
    let status = response.status();
    let is_sse = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("text/event-stream"));
    let bytes = tokio::time::timeout(REQUEST_TIMEOUT, to_bytes(response.into_body(), 1024 * 1024))
        .await
        .expect("response body timed out")
        .expect("read response body");
    if is_sse {
        return legacy_sse::sse_data_frames(&bytes)
            .into_iter()
            .filter_map(|message| serde_json::from_str::<Value>(&message).ok())
            .find(|message| message.get("result").is_some() || message.get("error").is_some())
            .unwrap_or_else(|| {
                panic!(
                    "HTTP {status} SSE response has no JSON-RPC reply: {}",
                    String::from_utf8_lossy(&bytes)
                )
            });
    }
    serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!(
            "HTTP {status} response is not JSON ({error}): {}",
            String::from_utf8_lossy(&bytes)
        )
    })
}

async fn assert_corrupt_config_fails_closed(router: &Router, raw_config: &str) {
    sqlx::query("UPDATE system_config SET config_json=? WHERE id=1")
        .bind(raw_config)
        .execute(crate::db::pool())
        .await
        .expect("store deliberately corrupt config");

    let response = oneshot(
        router,
        rpc_request(
            "/mcp",
            json!({
                "jsonrpc": "2.0",
                "id": 900,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "corrupt-config-test", "version": "0.1"}
                }
            }),
            None,
        ),
    )
    .await;
    let status = response.status();
    let update_result =
        crate::services::config_service::update(&json!({"routing":{"enableBearerAuth":true}}))
            .await;
    let stored: String = sqlx::query("SELECT config_json FROM system_config WHERE id=1")
        .fetch_one(crate::db::pool())
        .await
        .expect("read stored config")
        .try_get("config_json")
        .expect("config_json is non-null");
    assert!(
        status == StatusCode::UNAUTHORIZED && update_result.is_err() && stored == raw_config,
        "expected fail-closed 401, rejected config update, and unchanged raw config; status={status}, update_result={update_result:?}, stored={stored:?}, original={raw_config:?}"
    );
}

async fn initialize_http_session(
    router: &Router,
    path: &str,
    bearer: Option<&str>,
    id: u64,
) -> (String, Value) {
    let response = oneshot(
        router,
        rpc_request(
            path,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "http-protocol-test", "version": "0.1"}
                }
            }),
            bearer,
        ),
    )
    .await;
    let status = response.status();
    if status != StatusCode::OK {
        let bytes =
            tokio::time::timeout(REQUEST_TIMEOUT, to_bytes(response.into_body(), 1024 * 1024))
                .await
                .expect("initialize error body timed out")
                .expect("read initialize error body");
        panic!(
            "initialize {path} returned {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    let session_id = response
        .headers()
        .get("mcp-session-id")
        .expect("initialize returns MCP session id")
        .to_str()
        .expect("session id is text")
        .to_owned();
    let body = response_json(response).await;
    assert_eq!(body["id"], id);
    assert!(body["result"]["serverInfo"]["name"].is_string());
    (session_id, body)
}

async fn session_rpc(
    router: &Router,
    path: &str,
    session_id: &str,
    bearer: Option<&str>,
    value: Value,
) -> axum::response::Response {
    let mut request = rpc_request(path, value, bearer);
    request.headers_mut().insert(
        "mcp-session-id",
        session_id.parse().expect("valid session id header"),
    );
    oneshot(router, request).await
}

async fn read_sse_event(
    stream: &mut (impl Stream<Item = Result<axum::body::Bytes, axum::Error>> + Unpin),
) -> (String, String) {
    let frame = tokio::time::timeout(REQUEST_TIMEOUT, async {
        let mut pending = Vec::new();
        loop {
            let chunk = stream
                .next()
                .await
                .expect("SSE stream ended before event")
                .expect("SSE body error");
            pending.extend_from_slice(&chunk);
            if let Some(end) = pending.windows(2).position(|w| w == b"\n\n") {
                return pending[..end].to_vec();
            }
        }
    })
    .await
    .expect("SSE event timed out");
    let text = String::from_utf8(frame).expect("SSE frame is UTF-8");
    let mut event = String::new();
    let mut data = Vec::new();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            event = value.trim().to_string();
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start().to_string());
        }
    }
    (event, data.join("\n"))
}

async fn wait_stream_end<S>(stream: &mut S) -> bool
where
    S: Stream<Item = Result<axum::body::Bytes, axum::Error>> + Unpin,
{
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        while stream.next().await.is_some() {}
    })
    .await
    .is_ok()
}

async fn legacy_post(
    router: &Router,
    endpoint: &str,
    message: Value,
    bearer: Option<&str>,
) -> axum::response::Response {
    let mut builder = Request::post(endpoint)
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .header("mcp-protocol-version", "2024-11-05");
    if let Some(token) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let request = builder
        .body(Body::from(
            serde_json::to_vec(&message).expect("serialize legacy message"),
        ))
        .expect("build legacy POST");
    oneshot(router, request).await
}

async fn assert_mcp_auth_rejects(router: &Router, path: &str) {
    let initialize = json!({
        "jsonrpc": "2.0", "id": 901, "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "auth-test", "version": "0.1"}
        }
    });
    let missing = oneshot(router, rpc_request(path, initialize.clone(), None)).await;
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED, "missing bearer");
    let invalid = oneshot(
        router,
        rpc_request(path, initialize, Some("not-a-valid-token")),
    )
    .await;
    assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED, "invalid bearer");
}

fn legacy_get_request(path: &str, bearer: Option<&str>) -> Request<Body> {
    let mut builder = Request::get(path)
        .header(header::HOST, "127.0.0.1")
        .header(header::ACCEPT, "text/event-stream")
        .header(header::USER_AGENT, "http-protocol-test");
    if let Some(token) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    builder.body(Body::empty()).expect("build legacy SSE GET")
}

async fn run_protocol_flow(
    runtime: &TestRuntimeDirs,
    router: &Router,
    sdk_cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let fixture = runtime.root().join("time_mcp_fixture.py");
    std::fs::write(
        &fixture,
        r#"import json, os, sys, time
release_file = os.path.join(os.path.dirname(__file__), "release-slow-tool")
started_file = os.path.join(os.path.dirname(__file__), "slow-tool-started")
for line in sys.stdin:
    try:
        request = json.loads(line)
    except Exception:
        continue
    method = request.get("method")
    if method == "notifications/initialized":
        continue
    if "id" not in request:
        continue
    result = {}
    if method == "initialize":
        result = {
            "protocolVersion": request.get("params", {}).get("protocolVersion", "2025-03-26"),
            "capabilities": {"tools": {"listChanged": True}},
            "serverInfo": {"name": "local-time-fixture", "version": "1.0"}
        }
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        result = {"tools": [{"name": "time", "description": "Return fixture time", "inputSchema": {"type": "object", "properties": {"waitForRelease": {"type": "boolean"}}}}]}
    elif method == "tools/call":
        if request.get("params", {}).get("arguments", {}).get("waitForRelease"):
            open(started_file, "w").close()
            deadline = time.monotonic() + 20
            while not os.path.exists(release_file) and time.monotonic() < deadline:
                time.sleep(0.01)
        result = {"content": [{"type": "text", "text": "12:34 UTC"}], "isError": False}
    else:
        print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "error": {"code": -32601, "message": "Method not found"}}), flush=True)
        continue
    print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}), flush=True)
"#,
    )?;

    let mut fixture_config = crate::models::server::ServerConfig {
        id: String::new(),
        name: "protocol-time-fixture".to_string(),
        server_type: crate::models::server::ServerType::Stdio,
        description: Some("temporary local MCP fixture".to_string()),
        command: Some("python3".to_string()),
        args: Some(vec![fixture.to_string_lossy().into_owned()]),
        env: None,
        url: None,
        headers: None,
        options: None,
        openapi: None,
        per_session_client: Some(false),
        start_on_demand: Some(false),
        idle_timeout_ms: None,
        proxy: None,
        enable_keep_alive: None,
        keep_alive_interval: None,
        passthrough_headers: None,
        enabled: true,
    };
    let fixture_status = tokio::time::timeout(
        REQUEST_TIMEOUT,
        crate::mcp::pool::connect_server(&fixture_config),
    )
    .await
    .map_err(|_| anyhow::anyhow!("fixture MCP connect timed out"))?;
    anyhow::ensure!(
        fixture_status.connected,
        "fixture MCP server failed to connect: {:?}",
        fixture_status.error
    );
    anyhow::ensure!(
        fixture_status.tool_count == 1,
        "fixture should expose exactly one tool"
    );
    fixture_config.name = "protocol-denied-time-fixture".to_string();
    let denied_fixture_status = tokio::time::timeout(
        REQUEST_TIMEOUT,
        crate::mcp::pool::connect_server(&fixture_config),
    )
    .await
    .map_err(|_| anyhow::anyhow!("denied fixture MCP connect timed out"))?;
    anyhow::ensure!(
        denied_fixture_status.connected && denied_fixture_status.tool_count == 1,
        "denied fixture must be live for the permission check: {:?}",
        denied_fixture_status.error
    );

    // Malformed JSON and valid-but-wrong top-level shapes must all
    // remain fail-closed for bearer auth, without mutating their raw DB value.
    for raw in ["{broken-json", "[]", "true", "null", "42", "\"invalid\""] {
        assert_corrupt_config_fails_closed(router, raw).await;
    }
    sqlx::query("UPDATE system_config SET config_json=? WHERE id=1")
        .bind(r#"{"routing":{"enableBearerAuth":false,"enableGlobalRoute":true,"enableGroupNameRoute":true}}"#)
        .execute(crate::db::pool())
        .await?;

    // Auth disabled: the production router accepts initialize without a token.
    let (open_session, _) =
        initialize_http_session(router, "/mcp/protocol-time-fixture", None, 1).await;
    let initialized = session_rpc(
        router,
        "/mcp/protocol-time-fixture",
        &open_session,
        None,
        json!({"jsonrpc":"2.0", "method":"notifications/initialized"}),
    )
    .await;
    anyhow::ensure!(
        initialized.status().is_success(),
        "initialized notification should succeed"
    );
    let _ = to_bytes(initialized.into_body(), 1024).await?;

    sqlx::query("INSERT INTO groups (id,name,description,servers) VALUES (?,?,?,?)")
        .bind("protocol-allowed-group-id")
        .bind("protocol-allowed-group")
        .bind("allowed fixture group")
        .bind(r#"[{"name":"protocol-time-fixture","tools":["time"]}]"#)
        .execute(crate::db::pool())
        .await?;
    sqlx::query("INSERT INTO groups (id,name,description,servers) VALUES (?,?,?,?)")
        .bind("protocol-denied-group-id")
        .bind("protocol-denied-group")
        .bind("separate live fixture, bearer not allowed")
        .bind(r#"[{"name":"protocol-denied-time-fixture","tools":["time"]}]"#)
        .execute(crate::db::pool())
        .await?;

    let bearer =
        crate::services::bearer_key_service::create(&crate::models::bearer_key::BearerKeyPayload {
            name: "protocol fixture key".to_string(),
            enabled: true,
            access_type: "groups".to_string(),
            allowed_groups: vec!["protocol-allowed-group".to_string()],
            allowed_servers: Vec::new(),
        })
        .await?;
    crate::services::config_service::update(&json!({"routing":{"enableBearerAuth":true}})).await?;
    assert_mcp_auth_rejects(router, "/mcp/protocol-time-fixture").await;

    let (session, initialized_body) =
        initialize_http_session(router, "/mcp/protocol-time-fixture", Some(&bearer.token), 2).await;
    anyhow::ensure!(
        initialized_body["result"]["capabilities"]["tools"].is_object(),
        "initialize must advertise tools"
    );
    let unknown_session_id = "not-a-real-mcp-session";
    let unknown_response = session_rpc(
        router,
        "/mcp/protocol-time-fixture",
        unknown_session_id,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "id":200, "method":"ping"}),
    )
    .await;
    anyhow::ensure!(
        !unknown_response.status().is_success(),
        "unknown session request unexpectedly succeeded"
    );
    anyhow::ensure!(
        !session_last_seen().lock().await.contains_key(unknown_session_id),
        "unknown session id was added to last-seen tracking"
    );

    let initialized = session_rpc(
        router,
        "/mcp/protocol-time-fixture",
        &session,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "method":"notifications/initialized"}),
    )
    .await;
    anyhow::ensure!(
        initialized.status().is_success(),
        "initialized notification failed"
    );
    let _ = to_bytes(initialized.into_body(), 1024).await?;
    anyhow::ensure!(
        session_last_seen()
            .lock()
            .await
            .get(&session)
            .expect("initialized SDK session is tracked")
            .activity
            .lock()
            .expect("session activity lock is healthy")
            .in_flight
            == 0,
        "completed notification response retained an activity lease"
    );
    {
        let last_seen = session_last_seen().lock().await;
        let tracked = last_seen
            .get(&session)
            .expect("initialized SDK session is tracked");
        let mut activity = tracked
            .activity
            .lock()
            .expect("session activity lock is healthy");
        activity.last_seen =
            std::time::Instant::now() - Duration::from_secs(SESSION_IDLE_REAP_SECS + 1);
    }
    let touched = session_rpc(
        router,
        "/mcp/protocol-time-fixture",
        &session,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "id":201, "method":"ping"}),
    )
    .await;
    anyhow::ensure!(touched.status().is_success(), "valid session touch failed");
    let _ = response_json(touched).await;
    let touched_at = session_last_seen()
        .lock()
        .await
        .get(&session)
        .expect("valid session remains tracked")
        .activity
        .lock()
        .expect("session activity lock is healthy")
        .last_seen;
    anyhow::ensure!(
        std::time::Instant::now().duration_since(touched_at) < Duration::from_secs(1),
        "valid session request did not refresh its last-seen timestamp"
    );
    reap_idle_sessions_at(std::time::Instant::now(), Duration::from_secs(SESSION_IDLE_REAP_SECS))
        .await;
    anyhow::ensure!(
        shared_session_manager()
            .sessions
            .read()
            .await
            .contains_key(session.as_str()),
        "recently touched session was reaped"
    );

    let (stale_session, _) =
        initialize_http_session(router, "/mcp/protocol-time-fixture", Some(&bearer.token), 202)
            .await;
    {
        let last_seen = session_last_seen().lock().await;
        let mut activity = last_seen
            .get(&stale_session)
            .expect("new SDK session is tracked")
            .activity
            .lock()
            .expect("session activity lock is healthy");
        activity.last_seen =
            std::time::Instant::now() - Duration::from_secs(SESSION_IDLE_REAP_SECS + 1);
    }
    reap_idle_sessions_at(std::time::Instant::now(), Duration::from_secs(SESSION_IDLE_REAP_SECS))
        .await;
    anyhow::ensure!(
        !shared_session_manager()
            .sessions
            .read()
            .await
            .contains_key(stale_session.as_str()),
        "stale SDK session remained in the manager after reaping"
    );
    anyhow::ensure!(
        !session_last_seen().lock().await.contains_key(&stale_session),
        "stale SDK session remained in last-seen tracking after reaping"
    );

    let (deleted_session, _) =
        initialize_http_session(router, "/mcp/protocol-time-fixture", Some(&bearer.token), 204)
            .await;
    let deleted = oneshot(
        router,
        Request::delete("/mcp/protocol-time-fixture")
            .header(header::HOST, "127.0.0.1")
            .header("mcp-session-id", &deleted_session)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", bearer.token),
            )
            .body(Body::empty())?,
    )
    .await;
    anyhow::ensure!(
        deleted.status().is_success(),
        "valid session DELETE failed: {}",
        deleted.status()
    );
    let _ = to_bytes(deleted.into_body(), 1024).await?;
    anyhow::ensure!(
        !session_last_seen().lock().await.contains_key(&deleted_session),
        "deleted SDK session remained in last-seen tracking"
    );
    anyhow::ensure!(
        !shared_session_manager()
            .sessions
            .read()
            .await
            .contains_key(deleted_session.as_str()),
        "deleted SDK session remained in the manager"
    );

    let available_permits = session_tracking_capacity().available_permits();
    anyhow::ensure!(
        available_permits > 0,
        "session tracking capacity unexpectedly exhausted before cap test"
    );
    let capacity_hold = session_tracking_capacity()
        .clone()
        .acquire_many_owned(available_permits as u32)
        .await?;
    let manager_count_before_rejection =
        shared_session_manager().sessions.read().await.len();
    let tracker_count_before_rejection = session_last_seen().lock().await.len();
    let rejected_initialize = oneshot(
        router,
        rpc_request(
            "/mcp/protocol-time-fixture",
            json!({
                "jsonrpc":"2.0", "id":205, "method":"initialize",
                "params":{
                    "protocolVersion":"2025-03-26", "capabilities":{},
                    "clientInfo":{"name":"capacity-test", "version":"0.1"}
                }
            }),
            Some(&bearer.token),
        ),
    )
    .await;
    anyhow::ensure!(
        rejected_initialize.status() == StatusCode::SERVICE_UNAVAILABLE,
        "session initialization was not rejected at tracker capacity"
    );
    let _ = to_bytes(rejected_initialize.into_body(), 1024).await?;
    anyhow::ensure!(
        shared_session_manager().sessions.read().await.len() == manager_count_before_rejection,
        "capacity-rejected SDK session remained in the manager"
    );
    anyhow::ensure!(
        session_last_seen().lock().await.len() == tracker_count_before_rejection,
        "capacity-rejected SDK session remained in the tracker"
    );
    drop(capacity_hold);

    let active_release_file = runtime.root().join("release-slow-tool");
    let active_started_file = runtime.root().join("slow-tool-started");
    let _ = std::fs::remove_file(&active_release_file);
    let _ = std::fs::remove_file(&active_started_file);
    anyhow::ensure!(
        session_last_seen()
            .lock()
            .await
            .get(&session)
            .expect("active SDK session is tracked")
            .activity
            .lock()
            .expect("session activity lock is healthy")
            .in_flight
            == 0,
        "session had an outstanding response lease before active-call test"
    );
    let active_router = router.clone();
    let active_session = session.clone();
    let active_token = bearer.token.clone();
    let active_call = tokio::spawn(async move {
        let mut request = rpc_request(
            "/mcp/protocol-time-fixture",
            json!({
                "jsonrpc":"2.0", "id":203, "method":"tools/call",
                "params":{"name":"time", "arguments":{"waitForRelease":true}}
            }),
            Some(&active_token),
        );
        request.headers_mut().insert(
            "mcp-session-id",
            active_session.parse().expect("valid session id"),
        );
        oneshot(&active_router, request).await
    });
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        while !active_started_file.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("active fixture call did not start");
    anyhow::ensure!(
        session_last_seen()
            .lock()
            .await
            .get(&session)
            .expect("active SDK session remains tracked")
            .activity
            .lock()
            .expect("session activity lock is healthy")
            .in_flight
            == 1,
        "active request did not retain exactly one activity lease"
    );
    {
        let last_seen = session_last_seen().lock().await;
        let mut activity = last_seen
            .get(&session)
            .expect("active SDK session remains tracked")
            .activity
            .lock()
            .expect("session activity lock is healthy");
        activity.last_seen =
            std::time::Instant::now() - Duration::from_secs(SESSION_IDLE_REAP_SECS + 1);
    }
    reap_idle_sessions_at(std::time::Instant::now(), Duration::from_secs(SESSION_IDLE_REAP_SECS))
        .await;
    anyhow::ensure!(
        shared_session_manager()
            .sessions
            .read()
            .await
            .contains_key(session.as_str()),
        "session with an in-flight tool call was reaped"
    );
    std::fs::write(&active_release_file, "release")?;
    let active_response = tokio::time::timeout(REQUEST_TIMEOUT, active_call)
        .await
        .expect("active tool call did not finish after release")
        .expect("active tool task panicked");
    let active_body = response_json(active_response).await;
    anyhow::ensure!(
        active_body["result"]["content"][0]["text"] == "12:34 UTC",
        "active tool call failed or lost its response after reaper pass"
    );
    anyhow::ensure!(
        session_last_seen()
            .lock()
            .await
            .get(&session)
            .expect("completed SDK session remains tracked")
            .activity
            .lock()
            .expect("session activity lock is healthy")
            .in_flight
            == 0,
        "completed request retained an activity lease"
    );
    let completed_at = session_last_seen()
        .lock()
        .await
        .get(&session)
        .expect("completed SDK session remains tracked")
        .activity
        .lock()
        .expect("session activity lock is healthy")
        .last_seen;
    anyhow::ensure!(
        std::time::Instant::now().duration_since(completed_at) < Duration::from_secs(1),
        "completing a long request did not refresh session activity"
    );
    reap_idle_sessions_at(
        std::time::Instant::now(),
        Duration::from_secs(SESSION_IDLE_REAP_SECS),
    )
    .await;
    anyhow::ensure!(
        shared_session_manager()
            .sessions
            .read()
            .await
            .contains_key(session.as_str()),
        "session was reaped immediately after an active request completed"
    );

    let listed = session_rpc(
        router,
        "/mcp/protocol-time-fixture",
        &session,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "id":3, "method":"tools/list", "params":{}}),
    )
    .await;
    let list_body = response_json(listed).await;
    anyhow::ensure!(
        list_body["result"]["tools"]
            .as_array()
            .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "time")),
        "tools/list omitted fixture time tool"
    );

    let called = session_rpc(
        router,
        "/mcp/protocol-time-fixture",
        &session,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "id":4, "method":"tools/call", "params":{"name":"time", "arguments":{}}}),
    ).await;
    let call_body = response_json(called).await;
    anyhow::ensure!(
        call_body["result"]["content"][0]["text"] == "12:34 UTC",
        "fixture tool result did not traverse the hub"
    );

    let allowed_group = initialize_http_session(
        router,
        "/mcp/protocol-allowed-group",
        Some(&bearer.token),
        5,
    )
    .await
    .0;
    let initialized = session_rpc(
        router,
        "/mcp/protocol-allowed-group",
        &allowed_group,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "method":"notifications/initialized"}),
    )
    .await;
    anyhow::ensure!(
        initialized.status().is_success(),
        "allowed group handshake failed"
    );
    let _ = to_bytes(initialized.into_body(), 1024).await?;
    let allowed_list = session_rpc(
        router,
        "/mcp/protocol-allowed-group",
        &allowed_group,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "id":6, "method":"tools/list", "params":{}}),
    )
    .await;
    let allowed_list = response_json(allowed_list).await;
    anyhow::ensure!(
        allowed_list["result"]["tools"]
            .as_array()
            .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "time")),
        "allowed group did not expose its selected tool"
    );

    let denied_group =
        initialize_http_session(router, "/mcp/protocol-denied-group", Some(&bearer.token), 7)
            .await
            .0;
    let initialized = session_rpc(
        router,
        "/mcp/protocol-denied-group",
        &denied_group,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "method":"notifications/initialized"}),
    )
    .await;
    anyhow::ensure!(
        initialized.status().is_success(),
        "denied group handshake failed"
    );
    let _ = to_bytes(initialized.into_body(), 1024).await?;
    let denied_list = session_rpc(
        router,
        "/mcp/protocol-denied-group",
        &denied_group,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "id":8, "method":"tools/list", "params":{}}),
    )
    .await;
    let denied_list = response_json(denied_list).await;
    anyhow::ensure!(
        denied_list["result"]["tools"]
            .as_array()
            .is_some_and(|tools| tools.is_empty()),
        "group restriction exposed a denied group tool"
    );

    crate::services::server_tool_config_service::upsert(
        &crate::models::server_tool_config::ServerToolConfigPayload {
            server_name: "protocol-time-fixture".to_string(),
            item_type: "tool".to_string(),
            item_name: "time".to_string(),
            enabled: false,
            description: None,
            pinned: None,
        },
    )
    .await?;
    let disabled_list = session_rpc(
        router,
        "/mcp/protocol-time-fixture",
        &session,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "id":9, "method":"tools/list", "params":{}}),
    )
    .await;
    let disabled_list = response_json(disabled_list).await;
    anyhow::ensure!(
        disabled_list["result"]["tools"]
            .as_array()
            .is_some_and(|tools| tools.iter().all(|tool| tool["name"] != "time")),
        "disabled tool remained visible"
    );
    let disabled_call = session_rpc(
        router,
        "/mcp/protocol-time-fixture",
        &session,
        Some(&bearer.token),
        json!({"jsonrpc":"2.0", "id":10, "method":"tools/call", "params":{"name":"time", "arguments":{}}}),
    ).await;
    let disabled_call = response_json(disabled_call).await;
    anyhow::ensure!(
        disabled_call["error"].is_object() || disabled_call["result"]["isError"] == true,
        "disabled tool call was not rejected"
    );
    for (path, expected_status) in [
        ("/rest/protocol-time-fixture/call", StatusCode::FORBIDDEN),
        (
            "/rest/group/protocol-allowed-group/call",
            StatusCode::FORBIDDEN,
        ),
        (
            "/rest/protocol-denied-time-fixture/call",
            StatusCode::FORBIDDEN,
        ),
        (
            "/rest/group/protocol-denied-group/call",
            StatusCode::NOT_FOUND,
        ),
    ] {
        let response = oneshot(
            router,
            rpc_request(
                path,
                json!({"tool":"time", "arguments":{}}),
                Some(&bearer.token),
            ),
        )
        .await;
        anyhow::ensure!(
            response.status() == expected_status,
            "REST disabled-tool or bearer gate failed for {path}: {}",
            response.status()
        );
    }
    crate::services::server_tool_config_service::upsert(
        &crate::models::server_tool_config::ServerToolConfigPayload {
            server_name: "protocol-time-fixture".to_string(),
            item_type: "tool".to_string(),
            item_name: "time".to_string(),
            enabled: true,
            description: None,
            pinned: None,
        },
    )
    .await?;

    // Exercise the legacy endpoint-event protocol through its real adapter.
    // Initialization is a request/response and must survive the Streamable
    // JSON-response mode instead of disappearing during SSE frame conversion.
    let missing_legacy_auth = oneshot(
        router,
        legacy_get_request("/mcp/protocol-time-fixture", None),
    )
    .await;
    anyhow::ensure!(
        missing_legacy_auth.status() == StatusCode::UNAUTHORIZED,
        "legacy SSE GET without bearer was not rejected"
    );
    let invalid_legacy_auth = oneshot(
        router,
        legacy_get_request("/mcp/protocol-time-fixture", Some("not-a-valid-token")),
    )
    .await;
    anyhow::ensure!(
        invalid_legacy_auth.status() == StatusCode::UNAUTHORIZED,
        "legacy SSE GET with invalid bearer was not rejected"
    );

    let legacy_stream_response = oneshot(
        router,
        legacy_get_request("/mcp/protocol-time-fixture", Some(&bearer.token)),
    )
    .await;
    anyhow::ensure!(
        legacy_stream_response.status() == StatusCode::OK,
        "legacy SSE GET failed"
    );
    let mut legacy_stream = Body::new(legacy_stream_response.into_body()).into_data_stream();
    let (event, endpoint_data) = read_sse_event(&mut legacy_stream).await;
    anyhow::ensure!(
        event == "endpoint",
        "legacy stream did not begin with endpoint event"
    );
    let endpoint = endpoint_data
        .strip_prefix("/mcp/message")
        .map(|suffix| format!("/mcp/message{suffix}"))
        .ok_or_else(|| anyhow::anyhow!("unexpected legacy endpoint URL: {endpoint_data}"))?;

    let legacy_init = legacy_post(
        router,
        &endpoint,
        json!({
            "jsonrpc":"2.0", "id":20, "method":"initialize",
            "params":{"protocolVersion":"2024-11-05", "capabilities":{}, "clientInfo":{"name":"legacy-test", "version":"0.1"}}
        }),
        Some(&bearer.token),
    ).await;
    anyhow::ensure!(
        legacy_init.status() == StatusCode::ACCEPTED,
        "legacy initialize POST failed"
    );
    let (event, initialize_data) = read_sse_event(&mut legacy_stream).await;
    anyhow::ensure!(
        event == "message",
        "legacy initialize did not produce message event"
    );
    let initialize_response: Value = serde_json::from_str(&initialize_data)?;
    anyhow::ensure!(
        initialize_response["id"] == 20 && initialize_response["result"]["serverInfo"].is_object(),
        "legacy initialize JSON-RPC response was lost or malformed: {initialize_data}"
    );
    let manager_session_ids = shared_session_manager()
        .sessions
        .read()
        .await
        .keys()
        .map(|sid| sid.to_string())
        .collect::<std::collections::HashSet<_>>();
    let tracked_session_ids = session_last_seen()
        .lock()
        .await
        .keys()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    anyhow::ensure!(
        manager_session_ids == tracked_session_ids,
        "legacy SSE SDK session was not included in bounded session tracking"
    );

    let legacy_initialized = legacy_post(
        router,
        &endpoint,
        json!({"jsonrpc":"2.0", "method":"notifications/initialized"}),
        Some(&bearer.token),
    )
    .await;
    anyhow::ensure!(
        legacy_initialized.status() == StatusCode::ACCEPTED,
        "legacy initialized POST failed"
    );
    let legacy_ping = legacy_post(
        router,
        &endpoint,
        json!({"jsonrpc":"2.0", "id":21, "method":"ping"}),
        Some(&bearer.token),
    )
    .await;
    anyhow::ensure!(
        legacy_ping.status() == StatusCode::ACCEPTED,
        "legacy ping POST failed"
    );
    let (event, ping_data) = read_sse_event(&mut legacy_stream).await;
    anyhow::ensure!(
        event == "message",
        "legacy ping did not produce message event"
    );
    let ping_response: Value = serde_json::from_str(&ping_data)?;
    anyhow::ensure!(
        ping_response["id"] == 21 && ping_response["result"].is_object(),
        "legacy ping response did not arrive on GET stream"
    );

    // The legacy POST must return before a long tool call completes; its
    // response is delivered asynchronously over the already-open GET stream.
    let release_file = runtime.root().join("release-slow-tool");
    let started_file = runtime.root().join("slow-tool-started");
    let _ = std::fs::remove_file(&release_file);
    let _ = std::fs::remove_file(&started_file);
    let slow_post_result = tokio::time::timeout(
        Duration::from_secs(1),
        legacy_post(
            router,
            &endpoint,
            json!({
                "jsonrpc":"2.0", "id":22, "method":"tools/call",
                "params":{"name":"time", "arguments":{"waitForRelease":true}}
            }),
            Some(&bearer.token),
        ),
    )
    .await;
    let returned_before_tool_release = matches!(
        &slow_post_result,
        Ok(response) if response.status() == StatusCode::ACCEPTED
    );
    let fixture_started = tokio::time::timeout(Duration::from_secs(1), async {
        while !started_file.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    std::fs::write(&release_file, "release")?;
    anyhow::ensure!(
        returned_before_tool_release && fixture_started,
        "legacy tools/call POST did not return 202 within one second before the fixture was released"
    );
    let slow_post = slow_post_result.expect("checked as an early successful POST");
    drop(slow_post);
    let (event, slow_data) = read_sse_event(&mut legacy_stream).await;
    anyhow::ensure!(event == "message", "slow tool reply was not an SSE message");
    let slow_response: Value = serde_json::from_str(&slow_data)?;
    anyhow::ensure!(
        slow_response["id"] == 22 && slow_response["result"]["content"][0]["text"] == "12:34 UTC",
        "slow tool response did not arrive after fixture release: {slow_data}"
    );

    // Open an SDK-owned Streamable GET stream as well. Cancelling the server's
    // SDK token must close both that stream and the adapter's legacy stream.
    let mut sdk_get = Request::get("/mcp/protocol-time-fixture")
        .header(header::HOST, "127.0.0.1")
        .header(header::ACCEPT, "text/event-stream")
        .header("mcp-session-id", &session)
        .header("mcp-protocol-version", "2025-03-26")
        .header(header::AUTHORIZATION, format!("Bearer {}", bearer.token))
        .body(Body::empty())?;
    sdk_get
        .headers_mut()
        .insert(header::USER_AGENT, "http-protocol-test".parse()?);
    let sdk_stream_response = oneshot(router, sdk_get).await;
    anyhow::ensure!(
        sdk_stream_response.status().is_success(),
        "SDK Streamable GET failed"
    );
    let mut sdk_stream = Body::new(sdk_stream_response.into_body()).into_data_stream();

    sdk_cancel.cancel();
    anyhow::ensure!(
        wait_stream_end(&mut legacy_stream).await,
        "legacy SSE stream did not end after SDK cancellation"
    );
    anyhow::ensure!(
        wait_stream_end(&mut sdk_stream).await,
        "Streamable SDK stream did not end after SDK cancellation"
    );

    let drained_sessions = drain_shared_sdk_sessions().await;
    anyhow::ensure!(drained_sessions > 0, "shutdown drain found no SDK sessions");
    anyhow::ensure!(
        shared_session_manager().sessions.read().await.is_empty(),
        "SDK session manager was not empty after shutdown drain"
    );
    anyhow::ensure!(
        session_last_seen().lock().await.is_empty(),
        "SDK last-seen map was not empty after shutdown drain"
    );

    // Rebuilding the router with a fresh server token models the next HTTP
    // start. A new initialize must work after the old manager entries drain.
    let restart_cancel = CancellationToken::new();
    let restarted_router = build_router(1024 * 1024, restart_cancel.clone());
    let restarted_response = oneshot(
        &restarted_router,
        rpc_request(
            "/mcp/protocol-time-fixture",
            json!({
                "jsonrpc":"2.0", "id":23, "method":"initialize",
                "params":{
                    "protocolVersion":"2025-03-26", "capabilities":{},
                    "clientInfo":{"name":"http-restart-test", "version":"0.1"}
                }
            }),
            Some(&bearer.token),
        ),
    )
    .await;
    let restarted_initialize_succeeded = restarted_response.status() == StatusCode::OK
        && restarted_response.headers().contains_key("mcp-session-id");
    restart_cancel.cancel();
    drain_shared_sdk_sessions().await;
    anyhow::ensure!(
        restarted_initialize_succeeded,
        "fresh router could not initialize after the prior server sessions were drained"
    );
    anyhow::ensure!(
        shared_session_manager().sessions.read().await.is_empty(),
        "SDK session manager was not empty after restart fixture cleanup"
    );

    // The adapter should remove its legacy session, not merely stop forwarding.
    let session_id = endpoint
        .split("sessionId=")
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("legacy endpoint is missing session id"))?;
    let mut cleanup_endpoint = format!("/mcp/message?sessionId={session_id}");
    let mut cleaned = false;
    for _ in 0..20 {
        let response = legacy_post(
            router,
            &cleanup_endpoint,
            json!({"jsonrpc":"2.0", "id":22, "method":"ping"}),
            Some(&bearer.token),
        )
        .await;
        if response.status() == StatusCode::NOT_FOUND {
            cleaned = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        cleanup_endpoint = format!("/mcp/message?sessionId={session_id}");
    }
    anyhow::ensure!(
        cleaned,
        "legacy adapter session remained registered after cancellation"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_http_mcp_protocol_and_legacy_sse_lifecycle() {
    let runtime = TestRuntimeDirs::new();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .in_memory(true)
                .statement_cache_capacity(0),
        )
        .await
        .expect("open in-memory SQLite");
    tokio::time::timeout(
        Duration::from_secs(30),
        crate::db::migration::run_pending(&pool),
    )
    .await
    .expect("in-memory migrations timed out")
    .expect("run current schema migrations in memory");
    crate::db::initialize_test_pool(pool)
        .await
        .expect("install isolated in-memory process pool");

    let sdk_cancel = CancellationToken::new();
    let router = build_router(1024 * 1024, sdk_cancel.clone());
    let result = tokio::time::timeout(
        Duration::from_secs(120),
        std::panic::AssertUnwindSafe(run_protocol_flow(&runtime, &router, &sdk_cancel))
            .catch_unwind(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("protocol test flow timed out"))
    .and_then(|result| {
        result
            .map_err(|_| anyhow::anyhow!("protocol test flow panicked; see assertion above"))
            .and_then(|result| result)
    });
    sdk_cancel.cancel();
    for name in ["protocol-time-fixture", "protocol-denied-time-fixture"] {
        let cleanup = tokio::time::timeout(
            Duration::from_secs(20),
            crate::mcp::pool::disconnect_server(name),
        )
        .await;
        assert!(
            cleanup.is_ok(),
            "fixture {name} disconnect exceeded cleanup timeout"
        );
    }
    assert!(
        result.is_ok(),
        "isolated HTTP MCP protocol test failed: {result:?}"
    );
}
