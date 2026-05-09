use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "webhook-listener", version)]
pub struct CliArgs {
    /// Bind address.
    #[arg(long, env = "WEBHOOK_BIND", default_value = "127.0.0.1:8080")]
    pub bind: String,

    /// SQLite database file path.
    #[arg(long, env = "WEBHOOK_DB_PATH", default_value = "webhooks.db")]
    pub db_path: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub db_path: String,
    pub body_limit_bytes: usize,
    pub retain_per_endpoint: i64,
    pub dashboard_user: String,
    pub dashboard_password: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("environment variable {0} is required and must be non-empty")]
    MissingEnv(&'static str),
    #[error("environment variable {var} has invalid value: {message}")]
    InvalidEnv { var: &'static str, message: String },
}

fn require_env(name: &'static str) -> Result<String, ConfigError> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(ConfigError::MissingEnv(name)),
    }
}

fn optional_usize_env(name: &'static str, default: usize) -> Result<usize, ConfigError> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v.parse::<usize>().map_err(|e| ConfigError::InvalidEnv {
            var: name,
            message: e.to_string(),
        }),
        _ => Ok(default),
    }
}

fn optional_i64_env(name: &'static str, default: i64) -> Result<i64, ConfigError> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v.parse::<i64>().map_err(|e| ConfigError::InvalidEnv {
            var: name,
            message: e.to_string(),
        }),
        _ => Ok(default),
    }
}

impl Config {
    pub fn from_args_and_env(args: CliArgs) -> Result<Self, ConfigError> {
        let dashboard_user = require_env("WEBHOOK_DASHBOARD_USER")?;
        let dashboard_password = require_env("WEBHOOK_DASHBOARD_PASSWORD")?;
        let body_limit_bytes = optional_usize_env("WEBHOOK_BODY_LIMIT_BYTES", 1_048_576)?;
        let retain_per_endpoint = optional_i64_env("WEBHOOK_RETAIN_PER_ENDPOINT", 250)?;
        if retain_per_endpoint < 1 {
            return Err(ConfigError::InvalidEnv {
                var: "WEBHOOK_RETAIN_PER_ENDPOINT",
                message: "must be >= 1".into(),
            });
        }
        Ok(Self {
            bind: args.bind,
            db_path: args.db_path,
            body_limit_bytes,
            retain_per_endpoint,
            dashboard_user,
            dashboard_password,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> CliArgs {
        CliArgs {
            bind: "127.0.0.1:8080".into(),
            db_path: "webhooks.db".into(),
        }
    }

    // Note: env-var tests must serialize because std::env::set_var is process-global.
    // We use a simple Mutex from std for this.
    fn env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    fn clear_all() {
        for v in [
            "WEBHOOK_DASHBOARD_USER",
            "WEBHOOK_DASHBOARD_PASSWORD",
            "WEBHOOK_BODY_LIMIT_BYTES",
            "WEBHOOK_RETAIN_PER_ENDPOINT",
        ] {
            // SAFETY: tests are serialized via env_lock().
            unsafe { std::env::remove_var(v) };
        }
    }

    #[test]
    fn missing_user_is_error() {
        let _g = env_lock().lock().unwrap();
        clear_all();
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_PASSWORD", "p") };
        let err = Config::from_args_and_env(args()).unwrap_err();
        match err {
            ConfigError::MissingEnv("WEBHOOK_DASHBOARD_USER") => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn missing_password_is_error() {
        let _g = env_lock().lock().unwrap();
        clear_all();
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_USER", "u") };
        let err = Config::from_args_and_env(args()).unwrap_err();
        match err {
            ConfigError::MissingEnv("WEBHOOK_DASHBOARD_PASSWORD") => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn defaults_apply_when_only_credentials_set() {
        let _g = env_lock().lock().unwrap();
        clear_all();
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_USER", "u") };
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_PASSWORD", "p") };
        let cfg = Config::from_args_and_env(args()).unwrap();
        assert_eq!(cfg.dashboard_user, "u");
        assert_eq!(cfg.dashboard_password, "p");
        assert_eq!(cfg.body_limit_bytes, 1_048_576);
        assert_eq!(cfg.retain_per_endpoint, 250);
    }

    #[test]
    fn invalid_retain_zero_is_error() {
        let _g = env_lock().lock().unwrap();
        clear_all();
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_USER", "u") };
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_PASSWORD", "p") };
        unsafe { std::env::set_var("WEBHOOK_RETAIN_PER_ENDPOINT", "0") };
        let err = Config::from_args_and_env(args()).unwrap_err();
        match err {
            ConfigError::InvalidEnv {
                var: "WEBHOOK_RETAIN_PER_ENDPOINT",
                ..
            } => {}
            other => panic!("unexpected: {other:?}"),
        }
    }
}
