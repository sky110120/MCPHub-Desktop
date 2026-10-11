//! Compatibility adapter for the 2024-11-05 endpoint-event + message POST transport.
//!
//! JSON-RPC parsing and dispatch remain owned by rmcp's StreamableHttpService;
//! this module only translates the old HTTP shape and SSE framing.

use axum::{
    body::Body,
    http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
};
use futures_util::{stream, StreamExt};
use serde_json::Value;
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const MAX_LEGACY_SESSIONS: usize = 128;
const LEGACY_SESSION_IDLE: Duration = Duration::from_secs(10 * 60);
const LEGACY_EVENT_CAPACITY: usize = 32;
const MAX_LEGACY_BODY_BYTES: usize = 64 * 1024 * 1024;

/// The bearer key identity, not its secret token. A token is checked again on
/// every POST and is never retained in legacy session state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthBinding(pub(crate) Option<String>);

struct LegacySession {
    scope_path: String,
    principal: AuthBinding,
    initial_headers: HeaderMap,
    protocol_version: String,
    sdk_session_id: Option<String>,
    last_seen: Instant,
    closed: bool,
    sender: mpsc::Sender<String>,
    server_cancel: CancellationToken,
}

#[derive(Clone)]
struct LegacySessionEntry {
    state: Arc<Mutex<LegacySession>>,
    cancel: CancellationToken,
    post_lock: Arc<Mutex<()>>,
}

type SessionMap = HashMap<String, LegacySessionEntry>;

fn sessions() -> &'static Mutex<SessionMap> {
    static SESSIONS: OnceLock<Mutex<SessionMap>> = OnceLock::new();
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn is_legacy_get(method: &Method, path: &str, headers: &HeaderMap) -> bool {
    *method == Method::GET
        && (path == "/mcp" || path.starts_with("/mcp/"))
        && path != "/mcp/message"
        && headers
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .is_none()
        && headers
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.to_ascii_lowercase().contains("text/event-stream"))
}

fn endpoint_scope(path: &str) -> String {
    path.strip_prefix("/mcp").unwrap_or("").to_string()
}

fn safe_initial_headers(headers: &HeaderMap) -> HeaderMap {
    let mut safe = HeaderMap::new();
    for name in [header::HOST, header::ORIGIN, header::USER_AGENT] {
        if let Some(value) = headers.get(&name) {
            safe.insert(name, value.clone());
        }
    }
    if let Some(value) = headers.get("mcp-protocol-version") {
        safe.insert("mcp-protocol-version", value.clone());
    }
    safe
}

fn sdk_uri(scope_path: &str) -> Uri {
    let path = if scope_path.is_empty() {
        "/mcp".to_string()
    } else {
        format!("/mcp{scope_path}")
    };
    path.parse().expect("scope path came from a valid HTTP URI")
}

pub(crate) async fn open(
    request: Request<Body>,
    principal: AuthBinding,
    server_cancel: CancellationToken,
) -> Response {
    let path = request.uri().path().to_string();
    let scope_path = endpoint_scope(&path);
    let initial_headers = safe_initial_headers(request.headers());
    let protocol_version = initial_headers
        .get("mcp-protocol-version")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("2024-11-05")
        .to_string();

    let id = uuid::Uuid::new_v4().to_string();
    let (sender, receiver) = mpsc::channel(LEGACY_EVENT_CAPACITY);
    let session_cancel = server_cancel.child_token();
    if sender
        .try_send(format!("/mcp/message?sessionId={id}"))
        .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    let session = Arc::new(Mutex::new(LegacySession {
        scope_path,
        principal,
        initial_headers,
        protocol_version,
        sdk_session_id: None,
        last_seen: Instant::now(),
        closed: false,
        sender,
        server_cancel: server_cancel.clone(),
    }));
    {
        let mut map = sessions().lock().await;
        if map.len() >= MAX_LEGACY_SESSIONS {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Legacy SSE session limit reached",
            )
                .into_response();
        }
        map.insert(
            id.clone(),
            LegacySessionEntry {
                state: session.clone(),
                cancel: session_cancel.clone(),
                post_lock: Arc::new(Mutex::new(())),
            },
        );
    }
    spawn_reaper();
    let monitor_cancel = session_cancel.clone();
    spawn_close_monitor(id.clone(), session, monitor_cancel);

    let events = stream::unfold(
        (receiver, session_cancel.clone()),
        |(mut receiver, cancel)| async move {
            tokio::select! {
                _ = cancel.cancelled() => None,
                data = receiver.recv() => data.map(|data| (data, (receiver, cancel))),
            }
        },
    )
    .map(|data| {
        let event = if data.starts_with("/mcp/message?") {
            Event::default().event("endpoint").data(data)
        } else {
            Event::default().event("message").data(data)
        };
        Ok::<Event, Infallible>(event)
    });
    Sse::new(events)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

fn spawn_close_monitor(
    id: String,
    session: Arc<Mutex<LegacySession>>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let sender = session.lock().await.sender.clone();
        tokio::select! {
            _ = sender.closed() => close_session(&id).await,
            _ = cancel.cancelled() => close_session(&id).await,
        }
    })
}

#[derive(Debug)]
struct MessageQuery {
    session_id: String,
}

fn message_session_id(uri: &Uri) -> Option<MessageQuery> {
    let query = uri.query()?;
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        let key = percent_encoding::percent_decode_str(key)
            .decode_utf8()
            .ok()?;
        if key != "sessionId" {
            return None;
        }
        let value = percent_encoding::percent_decode_str(value)
            .decode_utf8()
            .ok()?;
        (!value.is_empty()).then(|| MessageQuery {
            session_id: value.into_owned(),
        })
    })
}

pub(crate) async fn post(
    principal: axum::extract::Extension<AuthBinding>,
    server_cancel: axum::extract::Extension<CancellationToken>,
    request: Request<Body>,
) -> Response {
    let Some(query) = message_session_id(request.uri()) else {
        return (StatusCode::BAD_REQUEST, "Missing sessionId").into_response();
    };
    let Some(entry) = sessions().lock().await.get(&query.session_id).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let (parts, body) = request.into_parts();
    let body_limit = super::http_server::BODY_LIMIT_SNAPSHOT
        .load(std::sync::atomic::Ordering::Relaxed)
        .max(1024 * 1024)
        .min(MAX_LEGACY_BODY_BYTES);
    let body = tokio::select! {
        _ = entry.cancel.cancelled() => return StatusCode::GONE.into_response(),
        _ = server_cancel.0.cancelled() => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        result = axum::body::to_bytes(body, body_limit) => match result {
            Ok(body) => body,
            Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        },
    };
    let _: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let post_guard = tokio::select! {
        _ = entry.cancel.cancelled() => return StatusCode::GONE.into_response(),
        _ = server_cancel.0.cancelled() => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        guard = entry.post_lock.clone().lock_owned() => guard,
    };
    let (scope_path, initial_headers, protocol_version, sdk_session_id, sender, was_uninitialized) = {
        let mut session = entry.state.lock().await;
        if session.principal != principal.0 {
            return StatusCode::FORBIDDEN.into_response();
        }
        if session.closed || entry.cancel.is_cancelled() {
            return StatusCode::GONE.into_response();
        }
        session.last_seen = Instant::now();
        (
            session.scope_path.clone(),
            session.initial_headers.clone(),
            session.protocol_version.clone(),
            session.sdk_session_id.clone(),
            session.sender.clone(),
            session.sdk_session_id.is_none(),
        )
    };
    if let Some(response) = super::http_server::legacy_scope_route_gate(&scope_path).await {
        return response;
    }

    let mut headers = initial_headers.clone();
    for (name, value) in &parts.headers {
        headers.insert(name, value.clone());
    }
    headers.remove("mcp-session-id");
    headers.insert(
        header::ACCEPT,
        HeaderValue::from_static("application/json, text/event-stream"),
    );
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if let Some(sdk_id) = sdk_session_id.as_deref() {
        headers.insert(
            "mcp-protocol-version",
            HeaderValue::from_str(&protocol_version)
                .unwrap_or_else(|_| HeaderValue::from_static("2024-11-05")),
        );
        let Ok(value) = HeaderValue::from_str(sdk_id) else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        headers.insert("mcp-session-id", value);
    }

    let uri = sdk_uri(&scope_path);
    let sdk_request = match Request::builder()
        .method(Method::POST)
        .uri(uri)
        .body(Body::from(body.clone()))
    {
        Ok(mut request) => {
            *request.headers_mut() = headers;
            request
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let sdk_response = match tokio::select! {
        _ = entry.cancel.cancelled() => return StatusCode::GONE.into_response(),
        _ = server_cancel.0.cancelled() => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        response = super::http_server::rmcp_service_with_json_response(
            entry.cancel.child_token(),
            false,
        )
        .oneshot(sdk_request) => response,
    } {
        Ok(response) => response,
        Err(never) => match never {},
    };
    let status = sdk_response.status();
    if !status.is_success() {
        return status.into_response();
    }
    let response_sdk_id = sdk_response
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let response_is_json = sdk_response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("application/json"));
    {
        let mut session = entry.state.lock().await;
        if session.closed || entry.cancel.is_cancelled() {
            return StatusCode::GONE.into_response();
        }
        if let Some(sdk_id) = response_sdk_id.as_ref() {
            session.sdk_session_id = Some(sdk_id.clone());
        }
        session.last_seen = Instant::now();
    }
    let sdk_body = Body::new(sdk_response.into_body());
    let sdk_session = entry.clone();
    let legacy_id = query.session_id;
    let session_cancel = entry.cancel.clone();
    let shutdown_cancel = server_cancel.0.clone();
    if was_uninitialized {
        tokio::spawn(forward_sdk_post_response(
            legacy_id,
            sdk_body,
            response_is_json,
            sender,
            sdk_session,
            session_cancel,
            shutdown_cancel,
            response_sdk_id,
            Some(post_guard),
        ));
    } else {
        drop(post_guard);
        tokio::spawn(forward_sdk_post_response(
            legacy_id,
            sdk_body,
            response_is_json,
            sender,
            sdk_session,
            session_cancel,
            shutdown_cancel,
            None,
            None,
        ));
    }
    (StatusCode::ACCEPTED, "").into_response()
}

const MAX_PENDING_SSE_FRAME_BYTES: usize = MAX_LEGACY_BODY_BYTES;

async fn forward_sdk_post_response(
    legacy_id: String,
    body: Body,
    is_json: bool,
    sender: mpsc::Sender<String>,
    session: LegacySessionEntry,
    session_cancel: CancellationToken,
    server_cancel: CancellationToken,
    sdk_session_id: Option<String>,
    mut initialize_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
) {
    let mut body = body.into_data_stream();
    if is_json {
        let mut pending = Vec::new();
        loop {
            let chunk = tokio::select! {
                _ = session_cancel.cancelled() => return,
                _ = server_cancel.cancelled() => return,
                chunk = body.next() => chunk,
            };
            match chunk {
                Some(Ok(bytes)) => {
                    if pending.len().saturating_add(bytes.len()) > MAX_LEGACY_BODY_BYTES {
                        log::warn!("legacy SSE SDK JSON response exceeded the body limit");
                        return;
                    }
                    pending.extend_from_slice(&bytes);
                }
                Some(Err(error)) => {
                    log::warn!("legacy SSE SDK JSON response body failed: {error}");
                    return;
                }
                None => break,
            }
        }
        for message in translate_sdk_response(&pending, true) {
            if !forward_legacy_post_message(
                &legacy_id,
                message,
                &sender,
                &session,
                &session_cancel,
                &server_cancel,
                sdk_session_id.as_deref(),
                &mut initialize_guard,
            )
            .await
            {
                return;
            }
        }
        return;
    }

    let mut decoder = SseFrameDecoder::default();
    loop {
        let chunk = tokio::select! {
            _ = session_cancel.cancelled() => return,
            _ = server_cancel.cancelled() => return,
            chunk = body.next() => chunk,
        };
        let data = match chunk {
            Some(Ok(data)) => data,
            Some(Err(error)) => {
                log::warn!("legacy SSE SDK response body failed: {error}");
                return;
            }
            None => return,
        };

        for byte in data.iter().copied() {
            let message = match decoder.push_byte(byte) {
                Ok(message) => message,
                Err(()) => {
                    log::warn!("legacy SSE SDK response frame exceeded the frame limit");
                    return;
                }
            };
            if let Some(message) = message {
                if !forward_legacy_post_message(
                    &legacy_id,
                    message,
                    &sender,
                    &session,
                    &session_cancel,
                    &server_cancel,
                    sdk_session_id.as_deref(),
                    &mut initialize_guard,
                )
                .await
                {
                    return;
                }
            }
        }
    }
}

async fn forward_legacy_post_message(
    legacy_id: &str,
    message: String,
    sender: &mpsc::Sender<String>,
    session: &LegacySessionEntry,
    session_cancel: &CancellationToken,
    server_cancel: &CancellationToken,
    sdk_session_id: Option<&str>,
    initialize_guard: &mut Option<tokio::sync::OwnedMutexGuard<()>>,
) -> bool {
    if message.is_empty() {
        return true;
    }
    let initialize_event_stream = if initialize_guard.is_some() {
        let response_version = serde_json::from_str::<Value>(&message)
            .ok()
            .and_then(|message| {
                message
                    .pointer("/result/protocolVersion")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
        let (scope_path, initial_headers, protocol_version) = {
            let mut state = tokio::select! {
                _ = session_cancel.cancelled() => return false,
                _ = server_cancel.cancelled() => return false,
                state = session.state.lock() => state,
            };
            if state.closed {
                return false;
            }
            if let Some(version) = response_version {
                state.protocol_version = version;
            }
            state.last_seen = Instant::now();
            (
                state.scope_path.clone(),
                state.initial_headers.clone(),
                state.protocol_version.clone(),
            )
        };
        sdk_session_id.map(|sdk_id| {
            (
                scope_path,
                initial_headers,
                protocol_version,
                sdk_id.to_string(),
            )
        })
    } else {
        None
    };
    let sent = tokio::select! {
        _ = session_cancel.cancelled() => false,
        _ = server_cancel.cancelled() => false,
        result = sender.send(message) => result.is_ok(),
    };
    if !sent {
        return false;
    }
    if initialize_guard.is_some() {
        drop(initialize_guard.take());
    }
    if let Some((scope_path, initial_headers, protocol_version, sdk_id)) = initialize_event_stream {
        start_sdk_event_stream(
            legacy_id.to_string(),
            scope_path,
            initial_headers,
            protocol_version,
            sdk_id,
            session_cancel.clone(),
            server_cancel.clone(),
        );
    }
    true
}

pub(super) fn sse_data_frames(bytes: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(bytes).replace("\r\n", "\n");
    text.split("\n\n")
        .filter_map(|frame| {
            let data = frame
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(str::trim_start)
                .collect::<Vec<_>>()
                .join("\n");
            (!data.is_empty()).then_some(data)
        })
        .collect()
}

#[derive(Default)]
struct SseFrameDecoder {
    pending: Vec<u8>,
}

impl SseFrameDecoder {
    fn push_byte(&mut self, byte: u8) -> Result<Option<String>, ()> {
        self.pending.push(byte);
        if self.pending.ends_with(b"\n\n") || self.pending.ends_with(b"\r\n\r\n") {
            let Some((boundary, separator_len)) = find_sse_boundary(&self.pending) else {
                return Ok(None);
            };
            if boundary + separator_len != self.pending.len()
                || self.pending.len() > MAX_PENDING_SSE_FRAME_BYTES
            {
                return Err(());
            }
            let frame = std::mem::take(&mut self.pending);
            return Ok(sse_data_frames(&frame).into_iter().next());
        }
        if self.pending.len() > MAX_PENDING_SSE_FRAME_BYTES {
            return Err(());
        }
        Ok(None)
    }
}

fn translate_sdk_response(body: &[u8], is_json: bool) -> Vec<String> {
    if is_json {
        std::str::from_utf8(body)
            .ok()
            .map(|value| vec![value.to_string()])
            .unwrap_or_default()
    } else {
        sse_data_frames(body)
    }
}

fn start_sdk_event_stream(
    legacy_id: String,
    scope_path: String,
    initial_headers: HeaderMap,
    protocol_version: String,
    sdk_session_id: String,
    session_cancel: CancellationToken,
    server_cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let Some(session) = sessions().lock().await.get(&legacy_id).cloned() else {
            return;
        };
        let sender = session.state.lock().await.sender.clone();
        let mut headers = initial_headers;
        let Ok(sdk_id) = HeaderValue::from_str(&sdk_session_id) else {
            close_session(&legacy_id).await;
            return;
        };
        let Ok(version) = HeaderValue::from_str(&protocol_version) else {
            close_session(&legacy_id).await;
            return;
        };
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("text/event-stream"),
        );
        headers.insert("mcp-session-id", sdk_id);
        headers.insert("mcp-protocol-version", version);
        let request = match Request::builder()
            .method(Method::GET)
            .uri(sdk_uri(&scope_path))
            .body(Body::empty())
        {
            Ok(mut request) => {
                *request.headers_mut() = headers;
                request
            }
            Err(_) => {
                close_session(&legacy_id).await;
                return;
            }
        };
        let response = match tokio::select! {
            _ = session_cancel.cancelled() => return,
            _ = server_cancel.cancelled() => return,
            response = super::http_server::rmcp_service_with_json_response(
                session_cancel.child_token(),
                false,
            )
            .oneshot(request) => response,
        } {
            Ok(response) if response.status().is_success() => response,
            _ => {
                close_session(&legacy_id).await;
                return;
            }
        };
        let mut body = Body::new(response.into_body()).into_data_stream();
        let mut decoder = SseFrameDecoder::default();
        'stream: loop {
            let chunk = tokio::select! {
                _ = session_cancel.cancelled() => break,
                _ = server_cancel.cancelled() => break,
                chunk = body.next() => chunk,
            };
            let Some(Ok(data)) = chunk else { break };
            for byte in data.iter().copied() {
                let message = match decoder.push_byte(byte) {
                    Ok(message) => message,
                    Err(()) => {
                        log::warn!("legacy SSE SDK event frame exceeded the frame limit");
                        break 'stream;
                    }
                };
                if let Some(message) = message {
                    let sent = tokio::select! {
                        _ = session_cancel.cancelled() => false,
                        _ = server_cancel.cancelled() => false,
                        result = sender.send(message) => result.is_ok(),
                    };
                    if !sent {
                        break 'stream;
                    }
                }
            }
        }
        close_session(&legacy_id).await;
    });
}

fn find_sse_boundary(bytes: &[u8]) -> Option<(usize, usize)> {
    let crlf = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| (index, 4));
    let lf = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| (index, 2));
    match (crlf, lf) {
        (Some(crlf), Some(lf)) => Some(if crlf.0 <= lf.0 { crlf } else { lf }),
        (Some(boundary), None) | (None, Some(boundary)) => Some(boundary),
        (None, None) => None,
    }
}

async fn close_session(id: &str) {
    let entry = sessions().lock().await.get(id).cloned();
    let Some(entry) = entry else { return };
    entry.cancel.cancel();
    let mut session = entry.state.lock().await;
    if session.closed {
        return;
    }
    session.closed = true;
    let sdk_id = session.sdk_session_id.clone();
    let scope_path = session.scope_path.clone();
    let headers = session.initial_headers.clone();
    let protocol_version = session.protocol_version.clone();
    let server_cancel = session.server_cancel.clone();
    drop(session);
    sessions().lock().await.remove(id);
    cleanup_sdk_session(sdk_id, scope_path, headers, protocol_version, server_cancel).await;
}

async fn cleanup_sdk_session(
    sdk_id: Option<String>,
    scope_path: String,
    mut headers: HeaderMap,
    protocol_version: String,
    server_cancel: CancellationToken,
) {
    let Some(sdk_id) = sdk_id else { return };
    let Ok(session_id) = HeaderValue::from_str(&sdk_id) else {
        return;
    };
    let Ok(version) = HeaderValue::from_str(&protocol_version) else {
        return;
    };
    headers.insert("mcp-session-id", session_id);
    headers.insert("mcp-protocol-version", version);
    let request = match Request::builder()
        .method(Method::DELETE)
        .uri(sdk_uri(&scope_path))
        .body(Body::empty())
    {
        Ok(mut request) => {
            *request.headers_mut() = headers;
            request
        }
        Err(_) => return,
    };
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = server_cancel.cancelled() => {},
            _ = super::http_server::rmcp_service(server_cancel.clone()).oneshot(request) => {},
        }
    })
    .await;
    let handle = super::http_server::shared_session_manager()
        .sessions
        .write()
        .await
        .remove(sdk_id.as_str());
    if let Some(handle) = handle {
        let _ = tokio::time::timeout(Duration::from_secs(2), handle.close()).await;
    }
    crate::mcp::session_pool::cleanup_session(&sdk_id).await;
}

async fn reap_if_idle(id: &str, now: Instant) {
    let entry = sessions().lock().await.get(id).cloned();
    let Some(entry) = entry else { return };
    let mut session = entry.state.lock().await;
    if session.closed || now.duration_since(session.last_seen) < LEGACY_SESSION_IDLE {
        return;
    }
    session.closed = true;
    entry.cancel.cancel();
    let sdk_id = session.sdk_session_id.clone();
    let scope_path = session.scope_path.clone();
    let headers = session.initial_headers.clone();
    let protocol_version = session.protocol_version.clone();
    let server_cancel = session.server_cancel.clone();
    drop(session);
    sessions().lock().await.remove(id);
    cleanup_sdk_session(sdk_id, scope_path, headers, protocol_version, server_cancel).await;
}

fn spawn_reaper() {
    static STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if STARTED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let now = Instant::now();
            let candidates = {
                let map = sessions().lock().await;
                map.keys().cloned().collect::<Vec<_>>()
            };
            for id in candidates {
                reap_if_idle(&id, now).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::{
        transport::streamable_http_server::{
            session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
        },
        ServerHandler,
    };

    struct NoopServer;
    impl ServerHandler for NoopServer {}

    #[test]
    fn legacy_get_does_not_capture_streamable_session_get() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, "text/event-stream".parse().unwrap());
        assert!(is_legacy_get(&Method::GET, "/mcp/team-a", &headers));
        headers.insert("mcp-session-id", "sdk-session".parse().unwrap());
        assert!(!is_legacy_get(&Method::GET, "/mcp/team-a", &headers));
    }

    #[test]
    fn legacy_post_uri_decodes_only_session_id_parameter() {
        let uri: Uri = "/mcp/message?sessionId=abc%2Fdef&other=ignored"
            .parse()
            .unwrap();
        assert_eq!(message_session_id(&uri).unwrap().session_id, "abc/def");
    }

    #[test]
    fn sdk_sse_message_frames_are_translated_without_legacy_body_rewriting() {
        let body = axum::body::Bytes::from_static(
            b"id: 0\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n\n",
        );
        assert_eq!(
            sse_data_frames(&body),
            vec!["{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}"]
        );
    }

    #[test]
    fn application_json_responses_are_forwarded_as_legacy_message_payloads() {
        let body = br#"{"jsonrpc":"2.0","id":8,"result":{"protocolVersion":"2025-03-26"}}"#;
        let payloads = translate_sdk_response(body, true);
        assert_eq!(payloads, vec![String::from_utf8(body.to_vec()).unwrap()]);
    }

    #[test]
    fn sse_decoder_handles_split_crlf_terminator_and_split_utf8() {
        let frame = "data: {\"text\":\"你好\"}\r\n\r\n".as_bytes();
        let split_utf8 = frame.iter().position(|byte| *byte >= 0x80).unwrap() + 1;
        let split_terminator = frame.len() - 1;
        let mut decoder = SseFrameDecoder::default();
        let mut messages = Vec::new();
        for chunk in [
            &frame[..split_utf8],
            &frame[split_utf8..split_terminator],
            &frame[split_terminator..],
        ] {
            for byte in chunk.iter().copied() {
                if let Some(message) = decoder.push_byte(byte).unwrap() {
                    messages.push(message);
                }
            }
        }
        assert_eq!(messages, vec!["{\"text\":\"你好\"}"]);
        assert!(decoder.pending.is_empty());
    }

    #[test]
    fn sse_decoder_emits_multiple_frames_from_one_chunk() {
        let chunk = b"data: first\n\ndata: second\n\ndata: third\n\n";
        let mut decoder = SseFrameDecoder::default();
        let messages = chunk
            .iter()
            .filter_map(|byte| decoder.push_byte(*byte).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            messages,
            vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string()
            ]
        );
    }

    #[test]
    fn sse_decoder_preserves_large_tool_response_support() {
        let payload = "x".repeat(9 * 1024 * 1024);
        let mut decoder = SseFrameDecoder::default();
        let mut message = None;
        for byte in b"data: ".iter().chain(payload.as_bytes()).chain(b"\n\n") {
            if let Some(data) = decoder.push_byte(*byte).expect("reply below 64 MiB") {
                message = Some(data);
            }
        }
        assert_eq!(message.as_deref(), Some(payload.as_str()));
    }

    #[test]
    fn sse_boundary_uses_the_earliest_crlf_or_lf_terminator() {
        assert_eq!(find_sse_boundary(b"prefix\r\n\r\nsuffix\n\n"), Some((6, 4)));
        assert_eq!(find_sse_boundary(b"prefix\n\nsuffix\r\n\r\n"), Some((6, 2)));
    }

    #[tokio::test]
    async fn sdk_initialize_initialized_ping_round_trip_translates_responses() {
        let manager = Arc::new(LocalSessionManager::default());
        let cancel = CancellationToken::new();
        let service: StreamableHttpService<NoopServer, LocalSessionManager> =
            StreamableHttpService::new(
                || Ok(NoopServer),
                manager.clone(),
                StreamableHttpServerConfig::default()
                    .with_sse_keep_alive(None)
                    .with_cancellation_token(cancel.clone()),
            );

        let initialize = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "legacy-fixture", "version": "1"}
            }
        });
        let init_response = service
            .clone()
            .oneshot(sdk_post_request(initialize, None))
            .await
            .unwrap();
        assert!(init_response.status().is_success());
        let sdk_session_id = init_response
            .headers()
            .get("mcp-session-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let init_is_json = init_response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("application/json"));
        let init_body = axum::body::to_bytes(Body::new(init_response.into_body()), 1024 * 1024)
            .await
            .unwrap();
        let init_messages = translate_sdk_response(&init_body, init_is_json);
        assert!(init_messages.iter().any(|message| {
            serde_json::from_str::<Value>(message)
                .ok()
                .and_then(|message| message.pointer("/result/protocolVersion").cloned())
                .is_some()
        }));

        let initialized = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        });
        let initialized_response = service
            .clone()
            .oneshot(sdk_post_request(initialized, Some(&sdk_session_id)))
            .await
            .unwrap();
        assert_eq!(initialized_response.status(), StatusCode::ACCEPTED);

        let ping = serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "ping"});
        let ping_response = service
            .clone()
            .oneshot(sdk_post_request(ping, Some(&sdk_session_id)))
            .await
            .unwrap();
        assert!(ping_response.status().is_success());
        let ping_is_json = ping_response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("application/json"));
        let ping_body = axum::body::to_bytes(Body::new(ping_response.into_body()), 1024 * 1024)
            .await
            .unwrap();
        let ping_messages = translate_sdk_response(&ping_body, ping_is_json);
        assert!(ping_messages.iter().any(|message| {
            serde_json::from_str::<Value>(message)
                .ok()
                .and_then(|message| message.get("id").cloned())
                == Some(serde_json::json!(2))
        }));

        let delete = Request::builder()
            .method(Method::DELETE)
            .uri("/mcp")
            .header(header::HOST, "localhost")
            .header("mcp-session-id", sdk_session_id)
            .header("mcp-protocol-version", "2025-03-26")
            .body(Body::empty())
            .unwrap();
        let _ = service.oneshot(delete).await.unwrap();
        assert!(manager.sessions.read().await.is_empty());
        cancel.cancel();
    }

    #[tokio::test]
    async fn idle_close_cancels_sse_stream_and_monitor_within_bound() {
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = mpsc::channel(LEGACY_EVENT_CAPACITY);
        let _ = sender.try_send(format!("/mcp/message?sessionId={id}"));
        let cancel = CancellationToken::new();
        let state = Arc::new(Mutex::new(LegacySession {
            scope_path: String::new(),
            principal: AuthBinding(None),
            initial_headers: HeaderMap::new(),
            protocol_version: "2024-11-05".into(),
            sdk_session_id: None,
            last_seen: Instant::now() - LEGACY_SESSION_IDLE,
            closed: false,
            sender,
            server_cancel: CancellationToken::new(),
        }));
        sessions().lock().await.insert(
            id.clone(),
            LegacySessionEntry {
                state: state.clone(),
                cancel: cancel.clone(),
                post_lock: Arc::new(Mutex::new(())),
            },
        );
        let monitor = spawn_close_monitor(id.clone(), state, cancel.clone());
        let events = stream::unfold(
            (receiver, cancel.clone()),
            |(mut receiver, cancel)| async move {
                tokio::select! {
                    _ = cancel.cancelled() => None,
                    data = receiver.recv() => data.map(|data| (data, (receiver, cancel))),
                }
            },
        );
        futures_util::pin_mut!(events);
        assert!(events.next().await.is_some());

        reap_if_idle(&id, Instant::now()).await;

        assert!(
            tokio::time::timeout(Duration::from_millis(250), events.next())
                .await
                .expect("SSE receiver must observe cancellation")
                .is_none()
        );
        tokio::time::timeout(Duration::from_millis(250), monitor)
            .await
            .expect("close monitor must not remain parked on sender.closed")
            .expect("monitor task must complete");
        assert!(!sessions().lock().await.contains_key(&id));
    }

    fn sdk_post_request(body: Value, session_id: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri("/mcp")
            .header(header::HOST, "localhost")
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header(header::CONTENT_TYPE, "application/json")
            .header("mcp-protocol-version", "2024-11-05");
        if let Some(session_id) = session_id {
            builder = builder.header("mcp-session-id", session_id);
        }
        builder.body(Body::from(body.to_string())).unwrap()
    }

    #[test]
    fn initial_headers_never_retain_auth_or_cookie_values() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "localhost".parse().unwrap());
        headers.insert(header::ORIGIN, "https://client.invalid".parse().unwrap());
        headers.insert(header::AUTHORIZATION, "Bearer secret".parse().unwrap());
        headers.insert(header::COOKIE, "session=secret".parse().unwrap());
        let safe = safe_initial_headers(&headers);
        assert_eq!(safe.get(header::HOST).unwrap(), "localhost");
        assert_eq!(safe.get(header::ORIGIN).unwrap(), "https://client.invalid");
        assert!(!safe.contains_key(header::AUTHORIZATION));
        assert!(!safe.contains_key(header::COOKIE));
    }
}
