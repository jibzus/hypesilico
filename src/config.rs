use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub database_path: String,
    pub hyperliquid_api_url: String,
    pub target_builder: String,
    pub builder_attribution_mode: BuilderAttributionMode,
    pub pnl_mode: PnlMode,
    pub lookback_ms: i64,
    pub leaderboard_users: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuilderAttributionMode {
    Auto,
    Heuristic,
    Logs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PnlMode {
    Gross,
    Net,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Missing required environment variable: {0}")]
    MissingEnv(String),
    #[error("Invalid value for {0}: {1}")]
    InvalidValue(String, String),
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_env_map(std::env::vars().collect())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn from_env_map(env_map: HashMap<String, String>) -> Result<Self, ConfigError> {
        let port = env_map
            .get("PORT")
            .map(|s| s.as_str())
            .unwrap_or("8080")
            .parse::<u16>()
            .map_err(|_| {
                ConfigError::InvalidValue("PORT".to_string(), "must be a valid u16".to_string())
            })?;

        let database_path = env_map
            .get("DATABASE_PATH")
            .cloned()
            .ok_or_else(|| ConfigError::MissingEnv("DATABASE_PATH".to_string()))?;

        let hyperliquid_api_url = env_map
            .get("HYPERLIQUID_API_URL")
            .cloned()
            .ok_or_else(|| ConfigError::MissingEnv("HYPERLIQUID_API_URL".to_string()))?;

        let target_builder = env_map
            .get("TARGET_BUILDER")
            .cloned()
            .ok_or_else(|| ConfigError::MissingEnv("TARGET_BUILDER".to_string()))?;

        let builder_attribution_mode = match env_map
            .get("BUILDER_ATTRIBUTION_MODE")
            .map(|s| s.as_str())
            .unwrap_or("auto")
        {
            "auto" => BuilderAttributionMode::Auto,
            "heuristic" => BuilderAttributionMode::Heuristic,
            "logs" => BuilderAttributionMode::Logs,
            other => {
                return Err(ConfigError::InvalidValue(
                    "BUILDER_ATTRIBUTION_MODE".to_string(),
                    format!("must be auto, heuristic, or logs, got {}", other),
                ))
            }
        };

        let pnl_mode = match env_map
            .get("PNL_MODE")
            .map(|s| s.as_str())
            .unwrap_or("gross")
        {
            "gross" => PnlMode::Gross,
            "net" => PnlMode::Net,
            other => {
                return Err(ConfigError::InvalidValue(
                    "PNL_MODE".to_string(),
                    format!("must be gross or net, got {}", other),
                ))
            }
        };

        let lookback_ms = env_map
            .get("LOOKBACK_MS")
            .map(|s| s.as_str())
            .unwrap_or("86400000")
            .parse::<i64>()
            .map_err(|_| {
                ConfigError::InvalidValue(
                    "LOOKBACK_MS".to_string(),
                    "must be a valid i64".to_string(),
                )
            })?;

        let leaderboard_users = parse_leaderboard_users_from_map(&env_map)?;

        Ok(Config {
            port,
            database_path,
            hyperliquid_api_url,
            target_builder,
            builder_attribution_mode,
            pnl_mode,
            lookback_ms,
            leaderboard_users,
        })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn parse_leaderboard_users_from_map(
    env_map: &HashMap<String, String>,
) -> Result<Vec<String>, ConfigError> {
    if let Some(users_str) = env_map.get("LEADERBOARD_USERS") {
        Ok(users_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect())
    } else if let Some(file_path) = env_map.get("LEADERBOARD_USERS_FILE") {
        let content = std::fs::read_to_string(file_path).map_err(|_| {
            ConfigError::InvalidValue(
                "LEADERBOARD_USERS_FILE".to_string(),
                "file not found or unreadable".to_string(),
            )
        })?;
        Ok(content
            .lines()
            .map(|line| line.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect())
    } else {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_required_env() -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("DATABASE_PATH".to_string(), "/tmp/test.db".to_string());
        map.insert(
            "HYPERLIQUID_API_URL".to_string(),
            "https://api.hyperliquid.xyz".to_string(),
        );
        map.insert("TARGET_BUILDER".to_string(), "0x123".to_string());
        map
    }

    #[test]
    fn test_missing_database_path() {
        let mut env_map = setup_required_env();
        env_map.remove("DATABASE_PATH");
        let result = Config::from_env_map(env_map);
        match result {
            Err(ConfigError::MissingEnv(s)) => assert_eq!(s, "DATABASE_PATH"),
            _ => panic!("Expected MissingEnv error"),
        }
    }

    #[test]
    fn test_missing_hyperliquid_api_url() {
        let mut env_map = setup_required_env();
        env_map.remove("HYPERLIQUID_API_URL");
        let result = Config::from_env_map(env_map);
        match result {
            Err(ConfigError::MissingEnv(s)) => assert_eq!(s, "HYPERLIQUID_API_URL"),
            _ => panic!("Expected MissingEnv error"),
        }
    }

    #[test]
    fn test_missing_target_builder() {
        let mut env_map = setup_required_env();
        env_map.remove("TARGET_BUILDER");
        let result = Config::from_env_map(env_map);
        match result {
            Err(ConfigError::MissingEnv(s)) => assert_eq!(s, "TARGET_BUILDER"),
            _ => panic!("Expected MissingEnv error"),
        }
    }

    #[test]
    fn test_invalid_port() {
        let mut env_map = setup_required_env();
        env_map.insert("PORT".to_string(), "not_a_number".to_string());
        let result = Config::from_env_map(env_map);
        match result {
            Err(ConfigError::InvalidValue(k, _)) => assert_eq!(k, "PORT"),
            _ => panic!("Expected InvalidValue error"),
        }
    }

    #[test]
    fn test_invalid_builder_attribution_mode() {
        let mut env_map = setup_required_env();
        env_map.insert(
            "BUILDER_ATTRIBUTION_MODE".to_string(),
            "invalid".to_string(),
        );
        let result = Config::from_env_map(env_map);
        match result {
            Err(ConfigError::InvalidValue(k, _)) => assert_eq!(k, "BUILDER_ATTRIBUTION_MODE"),
            _ => panic!("Expected InvalidValue error"),
        }
    }

    #[test]
    fn test_invalid_pnl_mode() {
        let mut env_map = setup_required_env();
        env_map.insert("PNL_MODE".to_string(), "invalid".to_string());
        let result = Config::from_env_map(env_map);
        match result {
            Err(ConfigError::InvalidValue(k, _)) => assert_eq!(k, "PNL_MODE"),
            _ => panic!("Expected InvalidValue error"),
        }
    }
}
