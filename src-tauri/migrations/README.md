# migrations/ (historical snapshots)

**此目录仅是历史快照，不是运行时数据源。**

- 实际迁移逻辑以 `src-tauri/src/db/migration.rs`（`TARGET_VERSION = 30`）为**唯一权威**；运行时不读取本目录。
- 本地历史迁移 `0001`–`0020` 不重编号。上游随本次合并带来的 `0024`–`0029` 文件保留作上游快照参考，不能据其文件序号推断本地 schema 版本；其中上游 v29 bearer 表重建不由本地运行时执行。
- 本地新增映射使用 `desktop_v0026`–`desktop_v0030` 快照名，以避免与保留的上游同号快照混淆。Rust 内联迁移仍是可执行实现，SQL 文件仅作说明/参考。
- 本地新增版本：v26 FTS5 表；v27 已知 agent 配置回填；v28 keep-alive / passthrough 列；v29 builtin 名称唯一索引（保留并改名重复数据）；v30 `server_tool_config.pinned`。
