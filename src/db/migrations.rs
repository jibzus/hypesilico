//! Database migrations and initialization.

use sqlx::sqlite::{SqliteConnection, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use tracing::info;

/// Initialize the SQLite database with schema and pragmas.
pub async fn init_db(db_path: &str) -> Result<SqlitePool, sqlx::Error> {
    if let Some(parent) = Path::new(db_path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).ok();
        }
    }

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .after_connect(|conn, _meta| Box::pin(async move { configure_pragmas_conn(conn).await }))
        .connect(&format!("sqlite:{}?mode=rwc", db_path))
        .await?;

    run_migrations(&pool).await?;

    info!("Database initialized successfully at {}", db_path);
    Ok(pool)
}

/// Run all database migrations.
async fn run_migrations(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    info!("Running database migrations...");
    let schema_sql = include_str!("schema.sql");

    for statement in schema_sql.split(';') {
        let trimmed = statement.trim();
        if !trimmed.is_empty() {
            sqlx::query(trimmed).execute(pool).await?;
        }
    }

    info!("Migrations completed successfully");
    Ok(())
}

/// Configure SQLite pragmas for optimal performance and reliability.
async fn configure_pragmas_conn(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    use sqlx::Row;
    info!("Configuring SQLite pragmas...");

    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *conn)
        .await?;

    // journal_mode returns the actual mode set; must use fetch to get result
    let row = sqlx::query("PRAGMA journal_mode = WAL")
        .fetch_one(&mut *conn)
        .await?;
    let journal_mode: String = row.get(0);
    info!("SQLite journal_mode set to: {}", journal_mode);

    sqlx::query("PRAGMA busy_timeout = 5000")
        .execute(&mut *conn)
        .await?;
    sqlx::query("PRAGMA synchronous = NORMAL")
        .execute(&mut *conn)
        .await?;

    info!("SQLite pragmas configured");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_init_db_creates_database() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join("test.db")
            .to_string_lossy()
            .to_string();

        let pool = init_db(&db_path).await.expect("init_db failed");
        assert!(Path::new(&db_path).exists());

        let result: (i64,) = sqlx::query_as("SELECT 1")
            .fetch_one(&pool)
            .await
            .expect("query failed");
        assert_eq!(result.0, 1);
    }

    #[tokio::test]
    async fn test_migrations_create_tables() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join("test.db")
            .to_string_lossy()
            .to_string();
        let pool = init_db(&db_path).await.expect("init_db failed");

        let result: (String,) = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='raw_fills'",
        )
        .fetch_one(&pool)
        .await
        .expect("query failed");
        assert_eq!(result.0, "raw_fills");
    }

    #[tokio::test]
    async fn test_migrations_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join("test.db")
            .to_string_lossy()
            .to_string();
        let pool = init_db(&db_path).await.expect("init_db failed");

        run_migrations(&pool)
            .await
            .expect("second migration run failed");

        let result: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                .fetch_one(&pool)
                .await
                .expect("query failed");
        assert!(result.0 > 0);
    }

    #[tokio::test]
    async fn test_pragmas_configured() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join("test.db")
            .to_string_lossy()
            .to_string();
        let pool = init_db(&db_path).await.expect("init_db failed");

        let result: (i64,) = sqlx::query_as("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .expect("query failed");
        assert_eq!(result.0, 1);

        let result: (String,) = sqlx::query_as("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .expect("query failed");
        // `journal_mode=WAL` is best-effort; SQLite can fall back depending on environment.
        assert!(
            matches!(result.0.as_str(), "wal" | "delete"),
            "unexpected journal_mode: {}",
            result.0
        );
    }
}
