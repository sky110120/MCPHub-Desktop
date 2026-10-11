//! Smart Routing settings read from `config_json.smartRouting` (the same keys
//! the Phase 1 settings UI writes). Every numeric field is clamped on read so
//! a hand-edited config can never push the search into a degenerate state
//! (e.g. max_results=0 → empty results, negative weights → inverted scores).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SmartRoutingReindexReport {
    pub synced_servers: usize,
    pub failed_servers: usize,
    pub skipped_servers: usize,
    /// Servers whose unchanged index was already current.
    pub up_to_date_servers: usize,
    /// Eligible tools discovered for this pass, including unchanged indexes.
    pub total_tools: usize,
    pub results: Vec<SmartRoutingReindexServerResult>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SmartRoutingReindexServerResult {
    pub server_name: String,
    pub tool_count: usize,
    pub ok: bool,
    pub skipped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl SmartRoutingReindexReport {
    pub fn record_synced(&mut self, server_name: &str, tool_count: usize) {
        self.synced_servers += 1;
        self.total_tools += tool_count;
        self.results.push(SmartRoutingReindexServerResult {
            server_name: server_name.to_string(),
            tool_count,
            ok: true,
            skipped: false,
            error: None,
        });
    }

    pub fn record_failed(&mut self, server_name: &str, tool_count: usize, error: String) {
        self.failed_servers += 1;
        self.total_tools += tool_count;
        self.results.push(SmartRoutingReindexServerResult {
            server_name: server_name.to_string(),
            tool_count,
            ok: false,
            skipped: false,
            error: Some(error),
        });
    }

    pub fn record_skipped(&mut self, server_name: &str, reason: impl Into<String>) {
        self.record_skipped_with_tools(server_name, 0, reason);
    }

    pub fn record_skipped_with_tools(
        &mut self,
        server_name: &str,
        tool_count: usize,
        reason: impl Into<String>,
    ) {
        self.skipped_servers += 1;
        self.total_tools += tool_count;
        self.push_skipped(server_name, tool_count, reason.into(), false);
    }

    pub fn record_up_to_date(&mut self, server_name: &str, tool_count: usize) {
        self.skipped_servers += 1;
        self.up_to_date_servers += 1;
        self.total_tools += tool_count;
        self.push_skipped(
            server_name,
            tool_count,
            "index is already up to date".to_string(),
            true,
        );
    }

    fn push_skipped(&mut self, server_name: &str, tool_count: usize, reason: String, ok: bool) {
        self.results.push(SmartRoutingReindexServerResult {
            server_name: server_name.to_string(),
            tool_count,
            ok,
            skipped: true,
            error: Some(reason),
        });
    }
}

#[cfg(test)]
mod reindex_report_tests {
    use super::SmartRoutingReindexReport;

    #[test]
    fn aggregates_mixed_reindex_outcomes_using_actual_tool_counts() {
        let mut report = SmartRoutingReindexReport::default();
        report.record_synced("small-server", 2);
        report.record_failed("large-server", 3, "embedding failed".to_string());
        report.record_up_to_date("current-server", 5);
        report.record_skipped("offline-server", "server is disconnected");

        assert_eq!(report.synced_servers, 1);
        assert_eq!(report.failed_servers, 1);
        assert_eq!(report.skipped_servers, 2);
        assert_eq!(report.up_to_date_servers, 1);
        assert_eq!(report.total_tools, 10);
        assert_eq!(
            report
                .results
                .iter()
                .map(|result| result.tool_count)
                .collect::<Vec<_>>(),
            vec![2, 3, 5, 0]
        );
        assert!(report.results[2].ok && report.results[2].skipped);
        assert_eq!(
            serde_json::to_value(report).unwrap(),
            serde_json::json!({
                "syncedServers": 1,
                "failedServers": 1,
                "skippedServers": 2,
                "upToDateServers": 1,
                "totalTools": 10,
                "results": [
                    { "serverName": "small-server", "toolCount": 2, "ok": true, "skipped": false },
                    { "serverName": "large-server", "toolCount": 3, "ok": false, "skipped": false, "error": "embedding failed" },
                    { "serverName": "current-server", "toolCount": 5, "ok": true, "skipped": true, "error": "index is already up to date" },
                    { "serverName": "offline-server", "toolCount": 0, "ok": false, "skipped": true, "error": "server is disconnected" }
                ]
            })
        );
    }
}

/// Retrieval settings (desktop hybrid search) + the flags search/index need.
/// Defaults mirror the Phase 1 UI defaults (SettingsContext).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SmartRoutingSettings {
    /// Whether the Smart Routing consumer is enabled (drives mv preload at
    /// boot and gates indexing).
    pub enabled: bool,
    /// Meta-tool catalog verbosity: true = 3-step (search/describe/call),
    /// false = 2-step (search/call). Origin default `?? false` (flat 2-step).
    pub progressive_disclosure: bool,
    pub vector_weight: f32,
    pub keyword_weight: f32,
    pub max_results: u32,
    pub score_threshold: f32,
    /// Standard (non-progressive) mode (origin #1234): only the top N hits
    /// carry the full inputSchema; the rest return name/description/serverName
    /// and describe_tool surfaces the schema. None = every hit is full.
    /// (similarityThreshold deliberately not mirrored yet — pending decision.)
    pub full_schema_top_n: Option<u32>,
    /// Origin #1286: optional tool fields that search_tools/describe_tool
    /// results carry next to name/description/inputSchema/serverName. Empty
    /// list = only those four. None = default (title, annotations).
    #[serde(rename = "toolDefinitionFields")]
    pub tool_definition_fields: Option<Vec<String>>,
}

impl Default for SmartRoutingSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            progressive_disclosure: false,
            vector_weight: 0.5,
            keyword_weight: 0.5,
            max_results: 20,
            score_threshold: 0.5,
            full_schema_top_n: None,
            tool_definition_fields: None,
        }
    }
}

impl SmartRoutingSettings {
    fn clamp(&mut self) {
        self.vector_weight = self.vector_weight.clamp(0.0, 1.0);
        self.keyword_weight = self.keyword_weight.clamp(0.0, 1.0);
        self.max_results = self.max_results.clamp(1, 200);
        self.score_threshold = self.score_threshold.clamp(0.0, 1.0);
        if let Some(n) = self.full_schema_top_n {
            self.full_schema_top_n = Some(n.min(200));
        }
        // Unknown field names are dropped (origin validates against the same
        // whitelist); an empty list is meaningful ("only the core four").
        if let Some(fields) = &mut self.tool_definition_fields {
            const ALLOWED: [&str; 6] =
                ["title", "annotations", "outputSchema", "execution", "icons", "_meta"];
            let mut seen = std::collections::HashSet::new();
            fields.retain(|f| ALLOWED.contains(&f.as_str()) && seen.insert(f.clone()));
        }
    }
}

/// Read + clamp the settings. Missing keys fall back to the defaults (the
/// config store deep-merges, so partial user configs keep their set values).
pub async fn get_settings() -> SmartRoutingSettings {
    let node = crate::services::config_service::get()
        .await
        .ok()
        .and_then(|c| c.get("smartRouting").cloned());
    let mut s = match &node {
        None => SmartRoutingSettings::default(),
        Some(v) => match serde_json::from_value::<SmartRoutingSettings>(v.clone()) {
            Ok(s) => s,
            // One wrong-typed field (hand-edit / older frontend) must not
            // silently disable Smart Routing: salvage the known-good fields
            // individually over the defaults and log what was dropped.
            Err(e) => {
                log::warn!("[smart] settings parse failed, salvaging per-field: {e}");
                let mut s = SmartRoutingSettings::default();
                let salvage_bool = |key: &str, cur: bool| -> bool {
                    v.get(key).and_then(|x| x.as_bool()).unwrap_or(cur)
                };
                s.enabled = salvage_bool("enabled", s.enabled);
                s.progressive_disclosure =
                    salvage_bool("progressiveDisclosure", s.progressive_disclosure);
                s.vector_weight = v
                    .get("vectorWeight")
                    .and_then(|x| x.as_f64())
                    .map(|x| x as f32)
                    .unwrap_or(s.vector_weight);
                s.keyword_weight = v
                    .get("keywordWeight")
                    .and_then(|x| x.as_f64())
                    .map(|x| x as f32)
                    .unwrap_or(s.keyword_weight);
                s.max_results = v
                    .get("maxResults")
                    .and_then(|x| x.as_u64())
                    .map(|x| x as u32)
                    .unwrap_or(s.max_results);
                s.score_threshold = v
                    .get("scoreThreshold")
                    .and_then(|x| x.as_f64())
                    .map(|x| x as f32)
                    .unwrap_or(s.score_threshold);
                s.full_schema_top_n = v.get("fullSchemaTopN").and_then(|x| x.as_u64()).map(|x| x as u32);
                s.tool_definition_fields = v
                    .get("toolDefinitionFields")
                    .and_then(|x| x.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|f| f.as_str().map(|s| s.to_string()))
                            .collect()
                    });
                s
            }
        },
    };
    s.clamp();
    s
}
