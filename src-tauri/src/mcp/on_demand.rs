//! On-demand stdio server spawning - Rust mirror of origin PR #1012.
//!
//! When a stdio server config has `startOnDemand: true`, the shared pool
//! **does not** connect it at startup. Instead it sits "sleeping" (a pool
//! placeholder with `client: None`, `connected: false`, `start_on_demand: true`)
//! until a tool call arrives. The first tool call lazily builds + connects the
//! client here, caches it, runs the call, and arms an idle-shutdown timer. After
//! `idleTimeoutMs` (default 5 min) with no further calls the process is torn down
//! but the cached tool list is preserved in the pool placeholder so the server
//! stays discoverable and re-wakes on the next call.
//!
//! Scope (matches origin):
//! - Only stdio servers benefit (HTTP/SSE servers have no heavy process to keep
//!   alive). `pool::connect_server` gates the sleeping placeholder on
//!   `ServerType::Stdio`.
//! - Applies to the shared-pool call path (Tauri `call_tool` command + HTTP
//!   non-isolated `tools/call`). `perSessionClient` + `startOnDemand` is
//!   rejected at the service layer (mutually exclusive).
//!
//! Storage: a process-global `RwLock<HashMap<server_name, OnDemandEntry>>`. The
//! live client lives here (not in `PoolEntry.client`, which stays `None` for
//! on-demand servers); the pool entry is a "shadow" carrying status + cached
//! tools + the `start_on_demand` flag. A per-server creation lock mirrors
//! `session_pool::CREATE_LOCKS` so concurrent first-calls don't double-spawn.

use super::client::McpClient;
use super::pool;
use crate::models::server::{Tool, ToolCallResult};
use crate::services::{app_logger, server_service};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
    time::Instant,
};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};

/// Connect timeout for a freshly spawned on-demand client. Matches the shared
/// pool's 120s budget (npx/uvx first-run package downloads can be slow).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
/// Cap on the post-connect tools/list sweep (rmcp peer requests have no
/// default timeout); creation lock is held until this completes.
const TOOLS_LIST_TIMEOUT: Duration = Duration::from_secs(60);

/// Default idle-shutdown delay when `idle_timeout_ms` is unset (5 min).
const DEFAULT_IDLE_MS: u64 = 300_000;

struct OnDemandEntry {
    client: Arc<Mutex<McpClient>>,
    /// Timestamp of the last successful tool call. Captured into idle-timer
    /// tasks as a "generation" so a stale timer can detect a newer call arrived.
    last_used: Instant,
    /// Cached idle-shutdown delay (ms) from the server config.
    idle_ms: u64,
    /// Handle to the pending idle-shutdown task. Aborted + replaced on every
    /// successful call to push the shutdown out.
    idle_handle: Mutex<Option<JoinHandle<()>>>,
    /// Number of tool calls currently in flight (COUNTER, not a bool). The
    /// idle timer must not tear the client down while this is > 0 — a call can
    /// legally run longer than `idle_ms` (pool upstream calls may take up to
    /// 600s), and the pre-call `last_used` bump otherwise keeps the timer's
    /// generation snapshot matching for the entire call duration, causing a
    /// deterministic teardown right after the call returns. A bool was wrong
    /// for CONCURRENT calls: the first finisher cleared the flag while the
    /// second was still running, letting the timer disconnect a live call.
    in_flight: u32,
}

type Store = Arc<RwLock<HashMap<String, OnDemandEntry>>>;

static ON_DEMAND_CLIENTS: OnceLock<Store> = OnceLock::new();
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

pub(crate) fn begin_shutdown() {
    SHUTTING_DOWN.store(true, Ordering::Release);
}

fn is_shutting_down() -> bool {
    SHUTTING_DOWN.load(Ordering::Acquire)
}

fn store() -> &'static Store {
    ON_DEMAND_CLIENTS.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
}

/// Serializes lifecycle invalidation with the final publish of a cold-started
/// client. This prevents disable/reload from racing the store insertion.
type LifecycleLock = Arc<Mutex<()>>;

static LIFECYCLE_LOCK: OnceLock<LifecycleLock> = OnceLock::new();

fn lifecycle_lock() -> &'static LifecycleLock {
    LIFECYCLE_LOCK.get_or_init(|| Arc::new(Mutex::new(())))
}

/// A teardown increments the generation. A cold start checks it again before
/// publishing its client, so stale clients self-cancel after lifecycle changes.
type LifecycleGenerations = Arc<Mutex<HashMap<String, u64>>>;

static LIFECYCLE_GENERATIONS: OnceLock<LifecycleGenerations> = OnceLock::new();

fn lifecycle_generations() -> &'static LifecycleGenerations {
    LIFECYCLE_GENERATIONS.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

async fn lifecycle_generation(server_name: &str) -> u64 {
    let map = lifecycle_generations().lock().await;
    map.get(server_name).copied().unwrap_or(0)
}

async fn bump_lifecycle_generation(server_name: &str) {
    let mut map = lifecycle_generations().lock().await;
    let generation = map.entry(server_name.to_string()).or_insert(0);
    *generation = generation.saturating_add(1);
}

/// Per-server creation locks - prevents concurrent duplicate spawns for the
/// same server (mirrors `session_pool::CREATE_LOCKS`).
type CreateLocks = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;

static CREATE_LOCKS: OnceLock<CreateLocks> = OnceLock::new();

fn create_locks() -> &'static CreateLocks {
    CREATE_LOCKS.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

/// Get-or-spawn the on-demand client for `server_name` and call `tool` with
/// `arguments`. On a connection-class failure the entry is evicted so the next
/// call rebuilds it. After a successful call the idle-shutdown timer is reset.
pub async fn call_tool_on_demand(
    server_name: &str,
    tool: &str,
    arguments: Value,
) -> Result<ToolCallResult> {
    if is_shutting_down() {
        return Err(anyhow!("on-demand server manager is shutting down"));
    }
    // Fast path: a cached client exists. Clone the Arc + read idle_ms under a
    // read lock, then run the call outside the store lock.
    {
        let map = store().read().await;
        if let Some(entry) = map.get(server_name) {
            let client_arc = entry.client.clone();
            let idle_ms = entry.idle_ms;
            drop(map);
            log::debug!(
                "[on-demand] Reusing spawned client for '{}' (tool '{}')",
                server_name,
                tool
            );
            return run_call(server_name, &client_arc, tool, arguments, idle_ms).await;
        }
    }

    let start_generation = lifecycle_generation(server_name).await;

    // Slow path: acquire (or reuse) a per-server creation lock so concurrent
    // first-calls serialize instead of each spawning a duplicate process.
    let create_lock = {
        let mut locks = create_locks().lock().await;
        locks
            .entry(server_name.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let _guard = create_lock.lock().await;

    if is_shutting_down() {
        return Err(anyhow!("on-demand server manager is shutting down"));
    }

    // Double-check after acquiring the lock: another holder may have just
    // finished spawning the client.
    {
        let map = store().read().await;
        if let Some(entry) = map.get(server_name) {
            let client_arc = entry.client.clone();
            let idle_ms = entry.idle_ms;
            drop(map);
            log::debug!(
                "[on-demand] Spawned client created by a concurrent call for '{}' (tool '{}')",
                server_name,
                tool
            );
            return run_call(server_name, &client_arc, tool, arguments, idle_ms).await;
        }
    }

    log::info!(
        "[on-demand] No cached client for '{}', cold-starting (tool '{}')",
        server_name,
        tool
    );
    app_logger::log_to_db(
        "info",
        &format!(
            "[on-demand] Cold-starting on-demand server '{}' (tool '{}')",
            server_name, tool
        ),
    );

    // Serialize with connect/disconnect on the same server so the cold start
    // cannot interleave with a disable/reload/delete cycle: without this, a
    // `disconnect_server` running concurrently would clean the pool shadow and
    // terminate lifecycle management while we spawn a fresh client — producing
    // an orphaned client that keeps its process alive until the idle timer
    // fires (and the late timer would then stomp on whatever entry replaced
    // the shadow). Holding the connect lock spans config load + spawn +
    // insert; mark_* mutators now also guard on entry identity.
    let _connect_guard = pool::pool_connect_lock(server_name).await;

    // Re-check the store after acquiring the connect lock: a racing cold
    // start that held the lock before us may have already spawned.
    {
        let map = store().read().await;
        if let Some(entry) = map.get(server_name) {
            let client_arc = entry.client.clone();
            let idle_ms = entry.idle_ms;
            drop(map);
            log::debug!(
                "[on-demand] Client for '{}' appeared while waiting for connect lock (tool '{}')",
                server_name,
                tool
            );
            drop(_connect_guard);
            drop(_guard);
            return run_call(server_name, &client_arc, tool, arguments, idle_ms).await;
        }
    }

    // Build + connect a fresh client. The DB read happens only here (once per
    // wake), not on every call.
    let cfg = server_service::get_by_name(server_name)
        .await
        .map_err(|e| {
            let msg = format!(
                "[on-demand] Failed to load config for '{}': {}",
                server_name, e
            );
            log::error!("{}", msg);
            app_logger::log_to_db("error", &msg);
            anyhow!(
                "Failed to load config for on-demand server '{}': {}",
                server_name,
                e
            )
        })?
        .ok_or_else(|| {
            let msg = format!("[on-demand] Server '{}' not found", server_name);
            log::error!("{}", msg);
            app_logger::log_to_db("error", &msg);
            anyhow!("Server '{}' not found for on-demand call", server_name)
        })?;

    if !cfg.enabled || !cfg.start_on_demand.unwrap_or(false) {
        return Err(anyhow!(
            "on-demand server '{}' is disabled or no longer configured for on-demand startup",
            server_name
        ));
    }

    let idle_ms = cfg.idle_timeout_ms.unwrap_or(DEFAULT_IDLE_MS);

    let (client, tools, server_version) = match build_and_connect(&cfg).await {
        Ok(t) => t,
        Err(e) => {
            let msg = format!(
                "[on-demand] Cold-start connect failed for '{}': {}",
                server_name, e
            );
            log::warn!("{}", msg);
            app_logger::log_to_db("warn", &msg);
            // Reflect the failure on the pool placeholder so the frontend can
            // surface it (sleeping + error).
            pool::mark_on_demand_error(server_name, e.to_string()).await;
            return Err(e);
        }
    };

    // Serialize the final publish with lifecycle teardown. The fresh config
    // read catches updates that changed the same server's connection settings
    // while the client was being built.
    let lifecycle_guard = lifecycle_lock().lock().await;
    let current_generation = lifecycle_generation(server_name).await;
    let current_cfg = match server_service::get_by_name(server_name).await {
        Ok(cfg) => cfg,
        Err(e) => {
            drop(lifecycle_guard);
            let mut client = client;
            let _ = client.disconnect().await;
            return Err(anyhow!(
                "failed to validate on-demand server '{}': {}",
                server_name,
                e
            ));
        }
    };
    let still_active = !is_shutting_down()
        && current_generation == start_generation
        && current_cfg
            .as_ref()
            .map(|cfg| cfg.enabled && cfg.start_on_demand.unwrap_or(false))
            .unwrap_or(false);
    if !still_active {
        drop(lifecycle_guard);
        let mut client = client;
        let _ = client.disconnect().await;
        return Err(anyhow!(
            "on-demand startup for '{}' was cancelled by a lifecycle change",
            server_name
        ));
    }

    let client_arc = Arc::new(Mutex::new(client));
    // Mirror session_pool round-8: a disable/delete may land between the
    // shadow check above and connect completion (connect can take minutes for
    // slow npx/uvx installs) — inserting would keep a live process serving a
    // disabled server until the idle timer recycles it.
    match server_service::get_by_name(server_name).await {
        Ok(Some(c)) if c.enabled => {}
        _ => {
            let msg = format!(
                "[on-demand] Server '{}' disabled/removed during cold start; discarding fresh client (tool '{}')",
                server_name, tool
            );
            log::warn!("{}", msg);
            app_logger::log_to_db("warn", &msg);
            let _ = client_arc.lock().await.disconnect().await;
            pool::mark_on_demand_error(server_name, "server is disabled or removed".to_string())
                .await;
            return Err(anyhow!("Server '{}' is disabled or removed", server_name));
        }
    }
    {
        let mut map = store().write().await;
        map.insert(
            server_name.to_string(),
            OnDemandEntry {
                client: client_arc.clone(),
                last_used: Instant::now(),
                idle_ms,
                idle_handle: Mutex::new(None),
                in_flight: 0,
            },
        );
    }
    drop(lifecycle_guard);
    // Mark the pool placeholder awake so status + cached tools reflect reality.
    pool::mark_on_demand_awake(server_name, tools, server_version).await;

    let msg = format!(
        "[on-demand] Server '{}' cold-started, ready for tool '{}'",
        server_name, tool
    );
    log::info!("{}", msg);
    app_logger::log_to_db("info", &msg);

    drop(_connect_guard);
    drop(_guard);
    run_call(server_name, &client_arc, tool, arguments, idle_ms).await
}

/// Wake an on-demand server when a caller needs its tool list before the first
/// tool invocation. The client stays in the on-demand store and is protected by
/// the normal idle timer, so a following `tools/call` reuses this connection.
pub async fn prime_on_demand_server(server_name: &str) -> Result<()> {
    if is_shutting_down() {
        return Err(anyhow!("on-demand server manager is shutting down"));
    }
    let (status, _) = pool::get_entry_info(server_name)
        .await
        .ok_or_else(|| anyhow!("Server '{}' not connected", server_name))?;
    if !status.start_on_demand {
        return Err(anyhow!(
            "Server '{}' is not configured for on-demand startup",
            server_name
        ));
    }

    {
        let map = store().read().await;
        if let Some(entry) = map.get(server_name) {
            let client_arc = entry.client.clone();
            let idle_ms = entry.idle_ms;
            drop(map);
            schedule_idle(server_name, &client_arc, idle_ms).await;
            return Ok(());
        }
    }

    let start_generation = lifecycle_generation(server_name).await;
    let create_lock = {
        let mut locks = create_locks().lock().await;
        locks
            .entry(server_name.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let _guard = create_lock.lock().await;

    if is_shutting_down() {
        return Err(anyhow!("on-demand server manager is shutting down"));
    }

    // A concurrent tool call or discovery request may have populated the live
    // store while this request waited for the creation lock. Cached tools alone
    // are not enough here: the server may have gone idle and still need to be
    // awakened for the next call.
    {
        let map = store().read().await;
        if let Some(entry) = map.get(server_name) {
            let client_arc = entry.client.clone();
            let idle_ms = entry.idle_ms;
            drop(map);
            schedule_idle(server_name, &client_arc, idle_ms).await;
            return Ok(());
        }
    }

    if pool::get_entry_info(server_name).await.is_none() {
        return Err(anyhow!("Server '{}' is no longer connected", server_name));
    }

    let cfg = server_service::get_by_name(server_name)
        .await
        .map_err(|e| anyhow!("failed to load on-demand server '{}': {}", server_name, e))?
        .ok_or_else(|| anyhow!("Server '{}' not found for on-demand discovery", server_name))?;
    if !cfg.enabled || !cfg.start_on_demand.unwrap_or(false) {
        return Err(anyhow!(
            "on-demand server '{}' is disabled or no longer configured for on-demand startup",
            server_name
        ));
    }

    let idle_ms = cfg.idle_timeout_ms.unwrap_or(DEFAULT_IDLE_MS);
    let (client, tools, server_version) = match build_and_connect(&cfg).await {
        Ok(result) => result,
        Err(e) => {
            pool::mark_on_demand_error(server_name, e.to_string()).await;
            return Err(e);
        }
    };

    let lifecycle_guard = lifecycle_lock().lock().await;
    let current_generation = lifecycle_generation(server_name).await;
    let current_cfg = match server_service::get_by_name(server_name).await {
        Ok(cfg) => cfg,
        Err(e) => {
            drop(lifecycle_guard);
            let mut client = client;
            let _ = client.disconnect().await;
            return Err(anyhow!(
                "failed to validate on-demand server '{}': {}",
                server_name,
                e
            ));
        }
    };
    let still_active = !is_shutting_down()
        && current_generation == start_generation
        && current_cfg
            .as_ref()
            .map(|cfg| cfg.enabled && cfg.start_on_demand.unwrap_or(false))
            .unwrap_or(false);
    if !still_active {
        drop(lifecycle_guard);
        let mut client = client;
        let _ = client.disconnect().await;
        return Err(anyhow!(
            "on-demand discovery for '{}' was cancelled by a lifecycle change",
            server_name
        ));
    }

    let client_arc = Arc::new(Mutex::new(client));
    store().write().await.insert(
        server_name.to_string(),
        OnDemandEntry {
            client: client_arc.clone(),
            last_used: Instant::now(),
            idle_ms,
            idle_handle: Mutex::new(None),
            in_flight: 0,
        },
    );
    drop(lifecycle_guard);
    pool::mark_on_demand_awake(server_name, tools, server_version).await;
    schedule_idle(server_name, &client_arc, idle_ms).await;
    log::info!("[on-demand] Server '{}' tool cache primed", server_name);
    Ok(())
}

/// Run `call_tool` on a cached on-demand client. On a connection-class error
/// the entry is evicted so the next call rebuilds. On success the idle-shutdown
/// timer is reset.
/// RAII guard for the `in_flight` busy flag (see `run_call`). On Drop —
/// including future cancellation — spawns a detached task that clears the
/// flag (identity-guarded) and re-arms the idle timer only for that same entry.
struct InFlightGuard {
    server_name: String,
    client_arc: Arc<Mutex<McpClient>>,
    idle_ms: u64,
    /// Set false on the normal completion path so Drop is a no-op.
    armed: bool,
    #[cfg(test)]
    drop_completed: Option<tokio::sync::oneshot::Sender<()>>,
}

async fn finish_in_flight(
    server_name: &str,
    client_arc: &Arc<Mutex<McpClient>>,
    successful: bool,
) -> bool {
    let mut map = store().write().await;
    match map.get_mut(server_name) {
        Some(entry) if Arc::ptr_eq(&entry.client, client_arc) => {
            entry.in_flight = entry.in_flight.saturating_sub(1);
            if successful {
                entry.last_used = Instant::now();
            }
            true
        }
        _ => false,
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let server_name = self.server_name.clone();
        let client_arc = self.client_arc.clone();
        let idle_ms = self.idle_ms;
        #[cfg(test)]
        let drop_completed = self.drop_completed.take();
        let _ = tokio::spawn(async move {
            let should_rearm = finish_in_flight(&server_name, &client_arc, true).await;
            if should_rearm {
                schedule_idle(&server_name, &client_arc, idle_ms).await;
            }
            #[cfg(test)]
            if let Some(completed) = drop_completed {
                let _ = completed.send(());
            }
        });
    }
}

async fn run_call(
    server_name: &str,
    client_arc: &Arc<Mutex<McpClient>>,
    tool: &str,
    arguments: Value,
    idle_ms: u64,
) -> Result<ToolCallResult> {
    let call_start = Instant::now();
    // Mark the entry busy + bump `last_used` before the call starts. The busy
    // flag is what actually protects long calls: a tool can legally run longer
    // than `idle_ms` (upstream calls may take up to 600s while the default
    // idle is 300s), and during the call `last_used` does not change — so the
    // timer's generation snapshot would keep matching and the entry would be
    // torn down the moment the call returned. The idle timer skips removal
    // while `in_flight` is set and re-arms instead.
    let retry_with_current_entry = {
        let mut map = store().write().await;
        match map.get_mut(server_name) {
            Some(entry) if Arc::ptr_eq(&entry.client, client_arc) => {
                entry.last_used = Instant::now();
                entry.in_flight = entry.in_flight.saturating_add(1);
                false
            }
            Some(_) => true,
            None => {
                return Err(anyhow!(
                    "On-demand client for '{}' is no longer available",
                    server_name
                ))
            }
        }
    };
    if retry_with_current_entry {
        log::debug!(
            "[on-demand] '{}' entry rebuilt during call setup; retrying",
            server_name
        );
        return Box::pin(call_tool_on_demand(server_name, tool, arguments)).await;
    }
    // NOTE: no await between the increment above and the guard construction
    // below — a cancellation in that window would leak the counter (neither
    // the manual clear nor the guard Drop would run). schedule_idle moved
    // after the guard for exactly this reason.
    // Cancellation guard: if this future is dropped mid-await (e.g. the 600s
    // `timeout_tool_call` wrapper fires while we're parked on the client
    // mutex or the upstream call), the manual clear below never runs and
    // `in_flight` would stick true forever — the idle timer would then never
    // tear the entry down (process leak). The guard's Drop spawns a detached
    // clear (ptr_eq-guarded) that runs even on cancellation. Disarmed on the
    // normal path so the manual clear below is the only actor.
    let mut guard = InFlightGuard {
        server_name: server_name.to_string(),
        client_arc: client_arc.clone(),
        idle_ms,
        armed: true,
        #[cfg(test)]
        drop_completed: None,
    };
    // Re-arm AFTER the guard exists (moved from before it): the store
    // read/entry-mutex awaits are now covered — a cancellation there runs the
    // guard's Drop decrement instead of leaking the counter.
    schedule_idle(server_name, client_arc, idle_ms).await;
    let result = {
        let client = client_arc.lock().await;
        client.call_tool(tool, arguments).await
    };
    // Clear the busy flag (both Ok and Err paths) before re-arming the timer.
    // Generation guard: if this call's entry was torn down (lifecycle) and
    // cold-start-rebuilt during the call, `map[server_name]` now holds the NEW
    // generation whose own run_call has set in_flight=true — blindly clearing
    // it by name would let the new generation's idle timer tear down a live
    // in-flight call.
    let entry_is_current = finish_in_flight(server_name, client_arc, result.is_ok()).await;
    guard.armed = false;
    match result {
        Ok(r) => {
            let status = if r.is_error { "error" } else { "success" };
            log::debug!(
                "[on-demand] Tool '{}' on '{}' {} ({}ms)",
                tool,
                server_name,
                status,
                call_start.elapsed().as_millis()
            );
            // Reset the idle timer (last_used + in_flight already updated above).
            if entry_is_current {
                schedule_idle(server_name, client_arc, idle_ms).await;
            }
            Ok(r)
        }
        Err(e) => {
            // Evict ONLY on a genuinely broken connection (see session_pool
            // run_call): upstream JSON-RPC application errors keep the client
            // alive so the on-demand process (and any state it holds) survives
            // tool-level failures. rmcp transports report unhealthy once
            // their service or peer channel closes, including an exited stdio
            // child.
            let still_connected = client_arc.lock().await.is_connected();
            if still_connected {
                log::warn!(
                    "[on-demand] Tool '{}' failed on '{}' but connection healthy, keeping client: {}",
                    tool, server_name, e
                );
                return Err(e);
            }
            let client_to_disconnect = {
                let mut map = store().write().await;
                // Only evict THIS client (ptr_eq) — a concurrent cold start
                // may have replaced the entry between our read and now.
                let evict = map
                    .get(server_name)
                    .map(|e| Arc::ptr_eq(&e.client, client_arc))
                    .unwrap_or(false);
                if evict {
                    map.remove(server_name).map(|e| e.client)
                } else {
                    None
                }
            };
            let evicted = client_to_disconnect.is_some();
            if let Some(arc) = client_to_disconnect {
                let mut client = arc.lock().await;
                let _ = client.disconnect().await;
            }
            // Mark the pool placeholder sleeping (keep cached tools) only when
            // we actually evicted OUR entry — if the ptr_eq guard refused
            // (a concurrent cold start already replaced it), the store holds a
            // live awake client and the pool placeholder must not flip to
            // sleeping.
            if evicted {
                pool::mark_on_demand_sleeping(server_name).await;
            }
            let msg = format!(
                "[on-demand] Tool '{}' call failed on '{}' ({}ms), evicted client: {}",
                tool,
                server_name,
                call_start.elapsed().as_millis(),
                e
            );
            log::warn!("{}", msg);
            app_logger::log_to_db("warn", &msg);
            Err(e)
        }
    }
}

/// Build a client from config, connect it within `CONNECT_TIMEOUT`, and fetch
/// its tool list + server version. On ANY failure (handshake error or timeout)
/// the half-built client is explicitly `disconnect()`-ed before returning the
/// error so the child process tree is reaped (not orphaned).
async fn build_and_connect(
    cfg: &crate::models::server::ServerConfig,
) -> Result<(McpClient, Vec<Tool>, Option<String>)> {
    log::info!(
        "[on-demand] Building + connecting on-demand client for '{}' (type={:?})",
        cfg.name,
        cfg.server_type
    );
    let mut client = pool::build_client(cfg)?;
    match timeout(CONNECT_TIMEOUT, client.connect()).await {
        Ok(Ok(())) => {
            let tools = match timeout(TOOLS_LIST_TIMEOUT, client.list_tools_with_ttl()).await {
                Ok(Ok((tools, ttl))) => {
                    crate::services::list_freshness::record(&cfg.name, "tools", ttl);
                    tools
                }
                Ok(Err(e)) => {
                    log::warn!("[on-demand] '{}' tools/list failed: {}", cfg.name, e);
                    crate::services::list_freshness::record(&cfg.name, "tools", None);
                    Vec::new()
                }
                Err(_) => {
                    log::warn!(
                        "[on-demand] '{}' tools/list timed out after {}s; disconnecting half-built client",
                        cfg.name,
                        TOOLS_LIST_TIMEOUT.as_secs()
                    );
                    let _ = client.disconnect().await;
                    return Err(anyhow!(
                        "tools/list timed out after {}s",
                        TOOLS_LIST_TIMEOUT.as_secs()
                    ));
                }
            };
            let server_version = client.server_version();
            Ok((client, tools, server_version))
        }
        Ok(Err(e)) => {
            log::warn!(
                "[on-demand] Connect handshake failed for '{}', disconnecting half-built client: {}",
                cfg.name, e
            );
            let _ = client.disconnect().await;
            Err(e)
        }
        Err(_) => {
            log::warn!(
                "[on-demand] Connect timed out for '{}' ({}s), disconnecting half-built client",
                cfg.name,
                CONNECT_TIMEOUT.as_secs()
            );
            let _ = client.disconnect().await;
            Err(anyhow!(
                "On-demand client connect timed out after {}s",
                CONNECT_TIMEOUT.as_secs()
            ))
        }
    }
}

/// (Re)arm the idle-shutdown timer for `server_name`. Aborts any pending timer
/// first so each successful call pushes shutdown out by `idle_ms`. The timer
/// task captures the current `last_used` as a generation; if a newer call
/// resets `last_used` before the timer fires, the shutdown is skipped.
/// Non-async timer spawner: keeps the schedule/shutdown future types from
/// forming a recursive opaque-type cycle (shutdown re-arms through this).
/// Re-arm path used by `shutdown_on_demand_idle`'s "newer call arrived"
/// branch: spawn the timer AND register its handle in the entry's
/// `idle_handle` slot, so a later `schedule_idle` can abort it via the
/// normal handle swap. An unregistered fire-and-forget re-arm would dodge
/// that mechanism for the rest of the entry's life.
#[allow(clippy::type_complexity)]
fn spawn_idle_timer_registered(
    server_name: String,
    expected_client: Arc<Mutex<McpClient>>,
    idle_ms: u64,
    snapshot: Instant,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    // Returned as a type-erased boxed future: an `async fn` here would create
    // a compile-time type cycle with `shutdown_on_demand_idle` (shutdown →
    // re-arm → shutdown) that also fails the `Send` bound inference.
    Box::pin(async move {
        let map_key = server_name.clone();
        let timer_client = expected_client.clone();
        let new_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(idle_ms)).await;
            shutdown_on_demand_idle_for(&server_name, &timer_client, snapshot).await;
        });
        let old = {
            let mut map = store().write().await;
            match map.get_mut(&map_key) {
                Some(entry)
                    if Arc::ptr_eq(&entry.client, &expected_client)
                        && entry.last_used == snapshot =>
                {
                    entry.idle_handle.lock().await.replace(new_handle)
                }
                _ => {
                    new_handle.abort();
                    None
                }
            }
        };
        if let Some(old) = old {
            old.abort();
        }
    })
}

async fn schedule_idle(server_name: &str, expected_client: &Arc<Mutex<McpClient>>, idle_ms: u64) {
    let mut map = store().write().await;
    let Some(entry) = map
        .get_mut(server_name)
        .filter(|entry| Arc::ptr_eq(&entry.client, expected_client))
    else {
        return;
    };
    let snapshot = entry.last_used;
    let name = server_name.to_string();
    let timer_client = expected_client.clone();
    let new_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(idle_ms)).await;
        shutdown_on_demand_idle_for(&name, &timer_client, snapshot).await;
    });
    let old_handle = entry.idle_handle.lock().await.replace(new_handle);
    drop(map);
    if let Some(old) = old_handle {
        old.abort();
    }
}

fn idle_shutdown_eligible(entry: &OnDemandEntry, snapshot: Instant) -> bool {
    entry.last_used == snapshot && entry.in_flight == 0
}

/// Idle-timer callback: shut the server down (disconnect process, keep cached
/// tools) unless a newer call reset `last_used` since this timer was armed.
pub async fn shutdown_on_demand_idle(server_name: &str, snapshot: Instant) {
    let expected_client = store()
        .read()
        .await
        .get(server_name)
        .map(|entry| entry.client.clone());
    if let Some(expected_client) = expected_client {
        shutdown_on_demand_idle_for(server_name, &expected_client, snapshot).await;
    }
}

async fn shutdown_on_demand_idle_for(
    server_name: &str,
    expected_client: &Arc<Mutex<McpClient>>,
    snapshot: Instant,
) {
    // Re-check last_used under the write lock. If it changed, a newer call
    // arrived after this timer was armed - abort the shutdown. A call in
    // flight (`in_flight`) also blocks removal: it may legally outlast
    // idle_ms, and tearing down right after it returns would drop a healthy
    // client and lose stateful server state.
    let (client_to_disconnect, rearm) = {
        let mut map = store().write().await;
        let same_client = map
            .get(server_name)
            .map(|entry| Arc::ptr_eq(&entry.client, expected_client))
            .unwrap_or(false);
        if !same_client {
            (None, None)
        } else if map
            .get(server_name)
            .map(|entry| idle_shutdown_eligible(entry, snapshot))
            .unwrap_or(false)
        {
            (map.remove(server_name).map(|entry| entry.client), None)
        } else {
            let entry = map
                .get(server_name)
                .expect("matching entry remains under write lock");
            (
                None,
                Some((entry.client.clone(), entry.idle_ms, entry.last_used)),
            )
        }
    };
    let Some(client_arc) = client_to_disconnect else {
        if let Some((client_arc, idle_ms, last_used)) = rearm {
            log::debug!(
                "[on-demand] Idle shutdown for '{}' skipped (client busy or snapshot changed); re-arming timer",
                server_name
            );
            spawn_idle_timer_registered(server_name.to_string(), client_arc, idle_ms, last_used)
                .await;
        }
        return;
    };
    {
        let mut client = client_arc.lock().await;
        if let Err(e) = client.disconnect().await {
            log::warn!(
                "[on-demand] Error disconnecting idle client for '{}': {}",
                server_name,
                e
            );
        }
    }
    // Mark the pool placeholder sleeping but KEEP cached tools discoverable.
    pool::mark_on_demand_sleeping(server_name).await;
    let msg = format!(
        "[on-demand] Server '{}' shut down after idle (tools cached for next wake-up)",
        server_name
    );
    log::info!("{}", msg);
    app_logger::log_to_db("info", &msg);
}

/// Lifecycle teardown (disable / reload / delete / update): remove the on-demand
/// client and disconnect its process. Does NOT touch the pool placeholder - the
/// caller (`pool::disconnect_server`) owns that. No-op if no client is cached.
pub async fn shutdown_on_demand_lifecycle(server_name: &str) {
    let _lifecycle_guard = lifecycle_lock().lock().await;
    bump_lifecycle_generation(server_name).await;
    let client_to_disconnect = tear_down_on_demand_client(server_name).await;
    if let Some(client_arc) = client_to_disconnect {
        let mut client = client_arc.lock().await;
        if let Err(e) = client.disconnect().await {
            log::warn!(
                "[on-demand] Error disconnecting client for '{}' (lifecycle): {}",
                server_name,
                e
            );
        } else {
            log::info!(
                "[on-demand] Disconnected client for '{}' (lifecycle)",
                server_name
            );
        }
    }
}

/// Remove the on-demand entry for `server_name` under the store write lock and
/// return its client Arc (caller does the disconnect I/O outside the lock).
/// The per-server creation lock is intentionally KEPT in the map: a cold-start
/// holds that lock across connect(120s)+list_tools(60s), and removing it
/// mid-flight lets a concurrent call create a second lock and a second
/// cold-start — the loser's client then overwrites the winner's store entry
/// and is abandoned without disconnect (orphaned stdio grandchildren). The
/// map is bounded by the server count; reuse across re-enables is harmless.
async fn tear_down_on_demand_client(server_name: &str) -> Option<Arc<Mutex<McpClient>>> {
    let removed = {
        let mut map = store().write().await;
        map.remove(server_name).map(|e| e.client)
    };
    removed
}

/// Remove and disconnect **every** on-demand client. Called from
/// `pool::disconnect_all` at application shutdown so child processes are reaped
/// via `kill_process_tree` rather than relying solely on `kill_on_drop`. Best
/// effort: disconnect errors are logged, not propagated.
pub async fn cleanup_all_on_demand() {
    begin_shutdown();
    let _lifecycle_guard = lifecycle_lock().lock().await;
    let names: Vec<String> = {
        let locks = create_locks().lock().await;
        locks.keys().cloned().collect()
    };
    for name in names {
        bump_lifecycle_generation(&name).await;
    }
    let removed: Vec<(String, Arc<Mutex<McpClient>>)> = {
        let mut map = store().write().await;
        map.drain().map(|(name, e)| (name, e.client)).collect()
    };
    {
        let mut locks = create_locks().lock().await;
        locks.clear();
    }
    if removed.is_empty() {
        return;
    }
    let msg = format!(
        "[on-demand] Cleaning up {} on-demand client(s) (shutdown)",
        removed.len()
    );
    log::info!("{}", msg);
    app_logger::log_to_db("info", &msg);
    for (name, client_arc) in removed {
        let mut client = client_arc.lock().await;
        if let Err(e) = client.disconnect().await {
            log::warn!(
                "[on-demand] Error disconnecting client for '{}' (shutdown): {}",
                name,
                e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::server::ToolCallResult;
    use serde_json::json;
    use tokio::sync::oneshot;

    struct TestTransport {
        call_started: Mutex<Option<oneshot::Sender<()>>>,
        release_call: Mutex<Option<oneshot::Receiver<()>>>,
    }

    #[async_trait::async_trait]
    impl super::super::client::McpTransport for TestTransport {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<()> {
            Ok(())
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn list_tools(&self) -> Result<Vec<Tool>> {
            Ok(Vec::new())
        }

        async fn call_tool(&self, _name: &str, _arguments: Value) -> Result<ToolCallResult> {
            if let Some(started) = self.call_started.lock().await.take() {
                let _ = started.send(());
            }
            if let Some(release) = self.release_call.lock().await.take() {
                let _ = release.await;
            }
            Ok(ToolCallResult {
                content: Vec::new(),
                is_error: false,
                structured_content: None,
                raw_meta: None,
            })
        }
    }

    fn test_client(
        name: &str,
        call_started: Option<oneshot::Sender<()>>,
        release_call: Option<oneshot::Receiver<()>>,
    ) -> Arc<Mutex<McpClient>> {
        Arc::new(Mutex::new(McpClient::new(
            name,
            Box::new(TestTransport {
                call_started: Mutex::new(call_started),
                release_call: Mutex::new(release_call),
            }),
        )))
    }

    fn entry(
        client: Arc<Mutex<McpClient>>,
        last_used: Instant,
        idle_ms: u64,
        in_flight: u32,
    ) -> OnDemandEntry {
        OnDemandEntry {
            client,
            last_used,
            idle_ms,
            idle_handle: Mutex::new(None),
            in_flight,
        }
    }

    fn unique_name(label: &str) -> String {
        format!("on-demand-{}-{}", label, std::process::id())
    }

    pub(super) async fn insert_test_entry(
        name: &str,
        client: Arc<Mutex<McpClient>>,
        idle_ms: u64,
        in_flight: u32,
        last_used: Instant,
    ) {
        let previous = store().write().await.insert(
            name.to_string(),
            entry(client, last_used, idle_ms, in_flight),
        );
        assert!(previous.is_none(), "test server name must be unique");
    }

    pub(super) async fn cleanup_test_entry(name: &str, expected_client: &Arc<Mutex<McpClient>>) {
        let removed = {
            let mut map = store().write().await;
            let matches = map
                .get(name)
                .map(|entry| Arc::ptr_eq(&entry.client, expected_client))
                .unwrap_or(false);
            if matches {
                map.remove(name)
            } else {
                None
            }
        };
        if let Some(entry) = removed {
            if let Some(handle) = entry.idle_handle.lock().await.take() {
                handle.abort();
            }
            let mut client = entry.client.lock().await;
            let _ = client.disconnect().await;
        }
    }

    #[test]
    fn idle_timer_preserves_clients_while_calls_are_in_flight() {
        let last_used = Instant::now();
        let client = test_client("idle-predicate", None, None);
        assert!(!idle_shutdown_eligible(
            &entry(client, last_used, DEFAULT_IDLE_MS, 2),
            last_used
        ));
    }

    #[test]
    fn idle_timer_only_removes_the_unchanged_idle_generation() {
        let last_used = Instant::now();
        let client = test_client("idle-generation-predicate", None, None);
        assert!(idle_shutdown_eligible(
            &entry(client.clone(), last_used, DEFAULT_IDLE_MS, 0),
            last_used
        ));
        assert!(!idle_shutdown_eligible(
            &entry(
                client,
                last_used + Duration::from_millis(1),
                DEFAULT_IDLE_MS,
                0
            ),
            last_used
        ));
    }

    #[tokio::test]
    async fn idle_callback_keeps_entry_during_long_in_flight_call() {
        let name = unique_name("long-call");
        let idle_ms = 60_000;
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let client = test_client(&name, Some(started_tx), Some(release_rx));
        let last_used = Instant::now();
        insert_test_entry(&name, client.clone(), idle_ms, 0, last_used).await;

        let call_name = name.clone();
        let call_client = client.clone();
        let call = tokio::spawn(async move {
            run_call(&call_name, &call_client, "blocked", json!({}), idle_ms).await
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("mock tool call should start")
            .expect("mock transport should signal call start");

        let (call_snapshot, in_flight) = {
            let map = store().read().await;
            let entry = map.get(&name).expect("test entry should still exist");
            (entry.last_used, entry.in_flight)
        };
        assert_eq!(in_flight, 1);
        shutdown_on_demand_idle(&name, call_snapshot).await;

        {
            let map = store().read().await;
            let entry = map
                .get(&name)
                .expect("idle callback must preserve busy client");
            assert!(Arc::ptr_eq(&entry.client, &client));
            assert_eq!(entry.in_flight, 1);
        }

        release_tx
            .send(())
            .expect("mock call should still be waiting");
        call.await
            .expect("tool task should not panic")
            .expect("tool call should succeed");
        cleanup_test_entry(&name, &client).await;
    }

    #[tokio::test]
    async fn cancelled_call_releases_in_flight_count_and_rearms_idle_timer() {
        let name = unique_name("cancel-call");
        let idle_ms = 60_000;
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let client = test_client(&name, Some(started_tx), Some(release_rx));
        insert_test_entry(&name, client.clone(), idle_ms, 0, Instant::now()).await;

        let call_name = name.clone();
        let call_client = client.clone();
        let call = tokio::spawn(async move {
            run_call(&call_name, &call_client, "blocked", json!({}), idle_ms).await
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("mock tool call should start")
            .expect("mock transport should signal call start");

        let previous_timer = {
            let map = store().read().await;
            let entry = map.get(&name).expect("test entry should exist");
            assert_eq!(entry.in_flight, 1);
            let mut idle_handle = entry.idle_handle.lock().await;
            let timer = idle_handle
                .take()
                .expect("run_call should arm its initial idle timer");
            drop(idle_handle);
            drop(map);
            timer
        };
        drop(release_tx);
        call.abort();
        assert!(call
            .await
            .expect_err("aborted call should be cancelled")
            .is_cancelled());

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let ready = {
                    let map = store().read().await;
                    if let Some(entry) = map.get(&name) {
                        entry.in_flight == 0 && entry.idle_handle.lock().await.is_some()
                    } else {
                        false
                    }
                };
                if ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("cancellation guard should clear the counter and re-arm idle shutdown");
        previous_timer.abort();
        cleanup_test_entry(&name, &client).await;
    }

    #[tokio::test]
    async fn old_client_drop_callback_does_not_mutate_replacement_entry() {
        let name = unique_name("replacement");
        let idle_ms = 60_000;
        let old_client = test_client("old-client", None, None);
        let new_client = test_client("new-client", None, None);
        let last_used = Instant::now() - Duration::from_secs(1);
        insert_test_entry(&name, new_client.clone(), idle_ms, 3, last_used).await;
        let replacement_timer = tokio::spawn(std::future::pending::<()>());
        let replacement_timer_id = replacement_timer.id();
        {
            let map = store().read().await;
            let entry = map.get(&name).expect("replacement entry should exist");
            *entry.idle_handle.lock().await = Some(replacement_timer);
        }
        let (drop_completed_tx, drop_completed_rx) = oneshot::channel();

        drop(InFlightGuard {
            server_name: name.clone(),
            client_arc: old_client.clone(),
            idle_ms,
            armed: true,
            drop_completed: Some(drop_completed_tx),
        });

        tokio::time::timeout(Duration::from_secs(1), drop_completed_rx)
            .await
            .expect("drop callback should complete")
            .expect("drop callback should signal completion");

        {
            let map = store().read().await;
            let entry = map.get(&name).expect("replacement entry should remain");
            assert!(Arc::ptr_eq(&entry.client, &new_client));
            assert_eq!(
                entry.in_flight, 3,
                "old callback must not decrement new calls"
            );
            assert_eq!(
                entry.last_used, last_used,
                "old callback must not change new timestamp"
            );
            let timer = entry.idle_handle.lock().await;
            assert_eq!(
                timer.as_ref().map(|handle| handle.id()),
                Some(replacement_timer_id),
                "old callback must not replace the new entry's idle timer"
            );
        }

        cleanup_test_entry(&name, &new_client).await;
        let mut old_client = old_client.lock().await;
        let _ = old_client.disconnect().await;
    }

    #[tokio::test]
    async fn stale_schedule_after_replacement_preserves_replacement_timer_and_state() {
        let name = unique_name("stale-schedule");
        let idle_ms = 60_000;
        let old_client = test_client("old-schedule-client", None, None);
        let new_client = test_client("new-schedule-client", None, None);
        let old_last_used = Instant::now() - Duration::from_secs(2);
        insert_test_entry(&name, old_client.clone(), idle_ms, 1, old_last_used).await;

        assert!(finish_in_flight(&name, &old_client, true).await);

        let replacement_last_used = Instant::now();
        let replacement_timer = tokio::spawn(std::future::pending::<()>());
        let replacement_timer_id = replacement_timer.id();
        {
            let mut map = store().write().await;
            let previous = map.insert(
                name.clone(),
                entry(new_client.clone(), replacement_last_used, idle_ms + 1, 4),
            );
            assert!(previous.is_some(), "old entry should be replaced");
            let entry = map.get(&name).expect("replacement entry should exist");
            *entry.idle_handle.lock().await = Some(replacement_timer);
        }

        // This represents the gap between the old call's identity check and
        // its timer scheduling. Registration must re-check identity at swap.
        schedule_idle(&name, &old_client, idle_ms).await;

        {
            let map = store().read().await;
            let entry = map.get(&name).expect("replacement entry should remain");
            assert!(Arc::ptr_eq(&entry.client, &new_client));
            assert_eq!(entry.last_used, replacement_last_used);
            assert_eq!(entry.idle_ms, idle_ms + 1);
            assert_eq!(entry.in_flight, 4);
            let timer = entry.idle_handle.lock().await;
            assert_eq!(
                timer.as_ref().map(|handle| handle.id()),
                Some(replacement_timer_id),
                "stale scheduling must not replace the new entry's timer"
            );
        }

        cleanup_test_entry(&name, &new_client).await;
        let mut old_client = old_client.lock().await;
        let _ = old_client.disconnect().await;
    }
}

#[cfg(all(test, unix))]
#[path = "rmcp_health_tests.rs"]
mod rmcp_health_tests;
