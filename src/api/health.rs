use axum::Json;

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

pub async fn ready() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ready"}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_returns_ok() {
        let Json(body) = health().await;
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn test_ready_returns_ready() {
        let Json(body) = ready().await;
        assert_eq!(body["status"], "ready");
    }
}
