use hypesilico::datasource::HyperliquidDataSource;
use hypesilico::orchestration::ensure::Ingestor;
use hypesilico::{api, config::Config, db::init_db, DataSource, Repository};
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()),
        )
        .init();

    // Load configuration
    let config = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            std::process::exit(1);
        }
    };

    let port = config.port;

    // Initialize database and dependencies
    let pool = match init_db(&config.database_path).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to initialize database: {}", e);
            std::process::exit(1);
        }
    };

    let repo = Arc::new(Repository::new(pool));
    let datasource: Arc<dyn DataSource> =
        Arc::new(HyperliquidDataSource::new(config.hyperliquid_api_url.clone()));
    let ingestor = Arc::new(Ingestor::new(datasource, repo.clone(), config.clone()));

    // Create router
    let app = api::create_router(api::AppState { repo, ingestor });

    // Bind to address
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to bind to {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    tracing::info!("Server listening on {}", addr);

    // Run server
    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
