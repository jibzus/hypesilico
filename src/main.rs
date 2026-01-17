use hypesilico::api::{self, AppState};
use hypesilico::config::Config;
use hypesilico::datasource::HyperliquidDataSource;
use hypesilico::db::init_db;
use hypesilico::engine::EquityResolver;
use hypesilico::orchestration::ensure::Ingestor;
use hypesilico::orchestration::orchestrator::Orchestrator;
use hypesilico::Repository;
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

    // Initialize database and app state
    let pool = match init_db(&config.database_path).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to initialize database: {}", e);
            std::process::exit(1);
        }
    };

    let repo = Arc::new(Repository::new(pool));
    let datasource = Arc::new(HyperliquidDataSource::new(config.hyperliquid_api_url.clone()));
    let ingestor = Ingestor::new(datasource, repo.clone(), config.clone());
    let orchestrator = Arc::new(Orchestrator::new(ingestor, repo.clone()));
    let equity_resolver = Arc::new(EquityResolver::new(repo.clone()));

    let state = AppState::new(repo, config.clone(), orchestrator, equity_resolver);

    // Create router
    let app = api::create_router(state);

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
