-- Local schema v25 -> v26. The executable migration is in db/migration.rs.
CREATE VIRTUAL TABLE IF NOT EXISTS fts_servers USING fts5(ref_id UNINDEXED, zh, py, ini);
CREATE VIRTUAL TABLE IF NOT EXISTS fts_groups USING fts5(ref_id UNINDEXED, zh, py, ini);
CREATE VIRTUAL TABLE IF NOT EXISTS fts_rag_docs USING fts5(ref_id UNINDEXED, zh, py, ini);
CREATE VIRTUAL TABLE IF NOT EXISTS fts_skills USING fts5(ref_id UNINDEXED, zh, py, ini);
CREATE VIRTUAL TABLE IF NOT EXISTS fts_prompts USING fts5(ref_id UNINDEXED, zh, py, ini);
CREATE VIRTUAL TABLE IF NOT EXISTS fts_resources USING fts5(ref_id UNINDEXED, zh, py, ini);
CREATE VIRTUAL TABLE IF NOT EXISTS fts_app_log USING fts5(ref_id UNINDEXED, zh, py, ini);
