-- Local schema v28 -> v29. Runtime first renames duplicate rows, retaining
-- their ids and payloads, then replaces the legacy non-unique indexes.
DROP INDEX IF EXISTS idx_builtin_prompts_name;
DROP INDEX IF EXISTS idx_builtin_resources_name;
CREATE UNIQUE INDEX idx_builtin_prompts_name ON builtin_prompts(name);
CREATE UNIQUE INDEX idx_builtin_resources_name ON builtin_resources(name);
