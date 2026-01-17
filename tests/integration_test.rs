use axum::http::StatusCode;
use hypesilico::api;
use tower::util::ServiceExt;

#[tokio::test]
async fn test_health_endpoint() {
    let app = api::create_router();

    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/health")
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    let body_str = String::from_utf8(body.to_vec()).unwrap();
    assert!(body_str.contains("ok"));
}

#[tokio::test]
async fn test_ready_endpoint() {
    let app = api::create_router();

    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/ready")
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    let body_str = String::from_utf8(body.to_vec()).unwrap();
    assert!(body_str.contains("ready"));
}
