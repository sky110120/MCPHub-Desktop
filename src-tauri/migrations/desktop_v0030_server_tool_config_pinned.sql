-- Local schema v29 -> v30. Runtime uses add_column_if_missing for idempotence.
ALTER TABLE server_tool_config ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0;
