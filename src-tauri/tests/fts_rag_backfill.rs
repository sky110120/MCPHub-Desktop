use mcphub_lib::services::fts_service::{self, FtsTable};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

#[tokio::test]
async fn backfill_rag_docs_uses_the_production_tokenizer_and_fts_table() {
    let options = SqliteConnectOptions::new()
        .filename(":memory:")
        .statement_cache_capacity(0);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("create isolated in-memory database");

    sqlx::query("CREATE TABLE rag_docs (id TEXT PRIMARY KEY, name TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create fixture source table");
    sqlx::query("CREATE VIRTUAL TABLE fts_rag_docs USING fts5(ref_id UNINDEXED, zh, py, ini)")
        .execute(&pool)
        .await
        .expect("create fixture FTS5 table");
    sqlx::query(
        "INSERT INTO rag_docs (id, name) VALUES
         ('doc-zh', '数据库检索'),
         ('doc-en', 'SQLite search')",
    )
    .execute(&pool)
    .await
    .expect("seed fixture source rows");

    let rows = sqlx::query("SELECT id, name FROM rag_docs ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read fixture source rows");
    let mut connection = pool.acquire().await.expect("acquire fixture connection");
    for row in &rows {
        let id: String = sqlx::Row::try_get(row, "id").expect("read document id");
        let name: String = sqlx::Row::try_get(row, "name").expect("read document name");
        fts_service::sync_upsert_tx(&mut *connection, FtsTable::RagDocs, &id, &name)
            .await
            .expect("backfill FTS row with production tokenizer");
    }
    drop(connection);

    let indexed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM fts_rag_docs")
        .fetch_one(&pool)
        .await
        .expect("count indexed rows");
    assert_eq!(indexed, 2);

    let (zh, py, ini) = fts_service::tokenize_fields("数据库检索");
    let stored: (String, String, String) =
        sqlx::query_as("SELECT zh, py, ini FROM fts_rag_docs WHERE ref_id = 'doc-zh'")
            .fetch_one(&pool)
            .await
            .expect("read tokenized FTS fields");
    assert_eq!(stored, (zh, py, ini));

    for (input, expected_id) in [
        ("数据", "doc-zh"),
        ("SQLite", "doc-en"),
        ("search", "doc-en"),
        ("shuj", "doc-zh"),
        ("sj", "doc-zh"),
    ] {
        let query = fts_service::build_match_query(input).expect("non-empty search query");
        let hits: Vec<String> = sqlx::query_scalar(
            "SELECT ref_id FROM fts_rag_docs WHERE fts_rag_docs MATCH ? ORDER BY ref_id",
        )
        .bind(&query)
        .fetch_all(&pool)
        .await
        .expect("search with production query normalization");
        assert_eq!(
            hits,
            vec![expected_id.to_string()],
            "input={input}, query={query}"
        );
    }
}
