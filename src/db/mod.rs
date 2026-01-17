//! Database module for SQLite operations.
//!
//! This module provides:
//! - Database initialization and migrations
//! - SQLite pragma configuration
//! - Repository layer for database operations

pub mod migrations;
pub mod repo;

pub use migrations::init_db;
pub use repo::Repository;
