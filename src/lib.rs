pub mod api;
pub mod compile;
pub mod config;
pub mod datasource;
pub mod db;
pub mod domain;
pub mod engine;
pub mod error;
pub mod orchestration;

pub use compile::CompileState;
pub use config::Config;
pub use datasource::{DataSource, DataSourceError, Deposit, HyperliquidDataSource, MockDataSource};
pub use db::{init_db, Repository};
pub use domain::{
    Address, Attribution, AttributionConfidence, AttributionMode, Coin, Confidence, Decimal, Fill,
    Side, TimeMs,
};
pub use error::AppError;
