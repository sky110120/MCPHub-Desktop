-- Local schema v27 -> v28. Runtime uses add_column_if_missing for idempotence.
ALTER TABLE servers ADD COLUMN enable_keep_alive INTEGER;
ALTER TABLE servers ADD COLUMN keep_alive_interval INTEGER;
ALTER TABLE servers ADD COLUMN passthrough_headers TEXT;
