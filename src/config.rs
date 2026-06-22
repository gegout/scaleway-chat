// Copyright 2026 Cedric Gegout
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#![allow(dead_code)]
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

use crate::error::{AppError, Result};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub scaleway: ScalewayConfig,
    pub instance: InstanceConfig,
    pub nemotron: NemotronConfig,
    pub timeouts: TimeoutsConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ScalewayConfig {
    pub access_key: String,
    pub secret_key: SecretString,
    pub project_id: String,
    pub organization_id: String,
    pub zone: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct InstanceConfig {
    pub name: String,
    #[serde(default)]
    pub instance_type: String,
    pub snapshot_id: String,
    pub public_ip: String,
    pub gpu_types: Option<Vec<String>>,
}

impl InstanceConfig {
    pub fn effective_gpu_types(&self) -> Vec<String> {
        if let Some(ref gpus) = self.gpu_types {
            gpus.clone()
        } else if !self.instance_type.trim().is_empty() {
            vec![self.instance_type.clone()]
        } else {
            vec![]
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct NemotronConfig {
    pub port: u16,
    pub api_key: SecretString,
    pub model: String,
    pub max_tokens: u32,
    pub temperature: f32,
    pub system_prompt: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TimeoutsConfig {
    pub instance_creation_seconds: u64,
    pub instance_poll_interval_seconds: u64,
    pub nemotron_startup_seconds: u64,
    pub nemotron_poll_interval_seconds: u64,
    pub cleanup_timeout_seconds: u64,
    pub cleanup_poll_interval_seconds: u64,
    #[serde(default = "default_inference_timeout")]
    pub inference_timeout_seconds: u64,
}

fn default_inference_timeout() -> u64 {
    300
}

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    pub verbose: bool,
}

impl Config {
    pub fn load_default() -> Result<(Self, PathBuf)> {
        let home = dirs::home_dir().ok_or_else(|| {
            AppError::InvalidConfig("Could not locate home directory".to_string())
        })?;
        let config_path = home.join(".config/scaleway-chat/config.toml");
        let config = Self::load_from_path(&config_path)?;
        Ok((config, config_path))
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Err(AppError::ConfigNotFound);
        }

        // Warn on broad permissions
        if let Ok(metadata) = fs::metadata(path) {
            let mode = metadata.permissions().mode();
            // 0o600 means only owner can read/write. If any other bit is set in 0o077, warn
            if (mode & 0o077) != 0 {
                eprintln!(
                    "Warning: Configuration file permissions are broader than 0600 (current mode: {:o}).",
                    mode
                );
                eprintln!(
                    "Recommended action:\n  chmod 600 {}",
                    path.to_string_lossy()
                );
            }
        }

        let contents = fs::read_to_string(path)
            .map_err(|e| AppError::InvalidConfig(format!("Failed to read config file: {}", e)))?;
        let config: Config = toml::from_str(&contents)
            .map_err(|e| AppError::InvalidConfig(format!("Failed to parse TOML: {}", e)))?;

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        // 1. Mandatory string checking
        if self.scaleway.access_key.trim().is_empty() {
            return Err(AppError::InvalidConfig(
                "access_key cannot be empty".to_string(),
            ));
        }
        if self.scaleway.secret_key.expose_secret().trim().is_empty() {
            return Err(AppError::InvalidConfig(
                "secret_key cannot be empty".to_string(),
            ));
        }
        if self.scaleway.zone.trim().is_empty() {
            return Err(AppError::InvalidConfig("zone cannot be empty".to_string()));
        }
        if self.instance.name.trim().is_empty() {
            return Err(AppError::InvalidConfig(
                "instance name cannot be empty".to_string(),
            ));
        }
        let effective_gpus = self.instance.effective_gpu_types();
        if effective_gpus.is_empty() {
            return Err(AppError::InvalidConfig(
                "No GPU types configured. Specify either instance_type or gpu_types.".to_string(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for gpu in &effective_gpus {
            if gpu.trim().is_empty() {
                return Err(AppError::InvalidConfig(
                    "GPU type name cannot be empty".to_string(),
                ));
            }
            if !seen.insert(gpu.clone()) {
                return Err(AppError::InvalidConfig(format!(
                    "Duplicate GPU type entry in config: {}",
                    gpu
                )));
            }
        }
        if self.nemotron.api_key.expose_secret().trim().is_empty() {
            return Err(AppError::InvalidConfig(
                "nemotron api_key cannot be empty".to_string(),
            ));
        }
        if self.nemotron.model.trim().is_empty() {
            return Err(AppError::InvalidConfig(
                "nemotron model cannot be empty".to_string(),
            ));
        }

        // 2. UUID validations
        Uuid::parse_str(&self.scaleway.project_id)
            .map_err(|_| AppError::InvalidConfig("project_id must be a valid UUID".to_string()))?;
        Uuid::parse_str(&self.scaleway.organization_id).map_err(|_| {
            AppError::InvalidConfig("organization_id must be a valid UUID".to_string())
        })?;
        Uuid::parse_str(&self.instance.snapshot_id)
            .map_err(|_| AppError::InvalidConfig("snapshot_id must be a valid UUID".to_string()))?;

        // 3. Zone format validation (e.g. fr-par-2, nl-ams-1, pl-waw-2)
        let zone = &self.scaleway.zone;
        let parts: Vec<&str> = zone.split('-').collect();
        if parts.len() != 3
            || parts[0].len() != 2
            || parts[1].len() != 3
            || parts[2].parse::<u32>().is_err()
        {
            return Err(AppError::InvalidConfig(format!(
                "zone '{}' does not follow expected format (e.g., fr-par-2)",
                zone
            )));
        }

        // 4. Port validation
        if self.nemotron.port == 0 {
            return Err(AppError::InvalidConfig(
                "nemotron port must be between 1 and 65535".to_string(),
            ));
        }

        // 5. Max tokens positive
        if self.nemotron.max_tokens == 0 {
            return Err(AppError::InvalidConfig(
                "max_tokens must be positive".to_string(),
            ));
        }

        // 6. Temperature range validation
        if self.nemotron.temperature < 0.0 || self.nemotron.temperature > 2.0 {
            return Err(AppError::InvalidConfig(
                "temperature must be between 0.0 and 2.0".to_string(),
            ));
        }

        // 7. Timeouts validation
        if self.timeouts.instance_creation_seconds == 0 {
            return Err(AppError::InvalidConfig(
                "instance_creation_seconds must be positive".to_string(),
            ));
        }
        if self.timeouts.instance_poll_interval_seconds == 0 {
            return Err(AppError::InvalidConfig(
                "instance_poll_interval_seconds must be positive".to_string(),
            ));
        }
        if self.timeouts.nemotron_startup_seconds == 0 {
            return Err(AppError::InvalidConfig(
                "nemotron_startup_seconds must be positive".to_string(),
            ));
        }
        if self.timeouts.nemotron_poll_interval_seconds == 0 {
            return Err(AppError::InvalidConfig(
                "nemotron_poll_interval_seconds must be positive".to_string(),
            ));
        }
        if self.timeouts.cleanup_timeout_seconds == 0 {
            return Err(AppError::InvalidConfig(
                "cleanup_timeout_seconds must be positive".to_string(),
            ));
        }
        if self.timeouts.cleanup_poll_interval_seconds == 0 {
            return Err(AppError::InvalidConfig(
                "cleanup_poll_interval_seconds must be positive".to_string(),
            ));
        }

        // 8. Public IP validation
        if self.instance.public_ip != "new" {
            return Err(AppError::InvalidConfig(format!(
                "unsupported public_ip value '{}' (only 'new' is supported in this version)",
                self.instance.public_ip
            )));
        }

        Ok(())
    }
}
