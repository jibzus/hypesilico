use axum::http::StatusCode;

pub async fn health() -> (StatusCode, String) {
    (StatusCode::OK, "ok".to_string())
}

pub async fn ready() -> (StatusCode, String) {
    (StatusCode::OK, "ready".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_returns_ok() {
        let (status, body) = health().await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok");
    }

    #[tokio::test]
    async fn test_ready_returns_ready() {
        let (status, body) = ready().await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ready");
    }
}
