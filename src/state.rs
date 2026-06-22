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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::error::{AppError, Result};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProvisioningPhase {
    Preflight,
    CreatingInstance,
    DiscoveringBootVolume,
    PoweringOn,
    WaitingForRunning,
    AllocatingIp,
    WaitingForNemotron,
    Ready,
    CleaningUp,
    CleanupIncomplete,
}

/// Local persisted state tracking remote Scaleway resource identifiers.
/// Preserving this file allows the application to recover or clean up resources
/// after an unexpected crash, terminal exit, or power failure.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct State {
    /// Schema version (currently 4 for transactional fallback provisioning)
    pub version: u32,
    /// The active/attempted phase of provisioning
    pub phase: ProvisioningPhase,
    /// Mode indicator (e.g. Some("snapshot_direct"))
    #[serde(default)]
    pub creation_mode: Option<String>,
    /// Unique attempt UUID to tag and name resources for this run
    pub attempt_id: String,
    /// The current selected candidate GPU type
    pub selected_gpu_type: String,
    /// The ID of the golden snapshot from which the instance was created
    pub snapshot_id: String,
    /// The Scaleway zone where resources reside (e.g. "fr-par-2")
    pub zone: String,
    /// UUID of the running GPU Instance (if created)
    pub instance_id: Option<String>,
    /// UUID of the restored boot Block Storage volume (if created)
    #[serde(alias = "boot_volume_id", alias = "volume_id")]
    pub volume_id: Option<String>,
    /// UUID of the allocated flexible IP resource (if allocated)
    pub public_ip_id: Option<String>,
    /// The public IPv4 address associated with the instance (if allocated)
    pub public_ip_address: Option<String>,
    /// Chronological list of GPU types attempted during this session
    pub attempted_gpu_types: Vec<String>,
    /// Timestamp when this state was first initialized
    pub created_at: DateTime<Utc>,
    /// Runtime-only storage for the state file path
    #[serde(skip)]
    pub path: Option<PathBuf>,
}

impl State {
    /// Initialize a new state record for snapshot-direct provisioning.
    pub fn new(snapshot_id: String, zone: String) -> Self {
        Self {
            version: 4,
            phase: ProvisioningPhase::Preflight,
            creation_mode: Some("snapshot_direct".to_string()),
            attempt_id: uuid::Uuid::new_v4().to_string(),
            selected_gpu_type: String::new(),
            snapshot_id,
            zone,
            instance_id: None,
            volume_id: None,
            public_ip_id: None,
            public_ip_address: None,
            attempted_gpu_types: Vec::new(),
            created_at: Utc::now(),
            path: None,
        }
    }

    /// Reset attempt-specific resource IDs and select the candidate GPU type
    pub fn start_attempt(&mut self, gpu_type: &str) {
        self.phase = ProvisioningPhase::Preflight;
        self.selected_gpu_type = gpu_type.to_string();
        self.instance_id = None;
        self.volume_id = None;
        self.public_ip_id = None;
        self.public_ip_address = None;
        if !self.attempted_gpu_types.contains(&gpu_type.to_string()) {
            self.attempted_gpu_types.push(gpu_type.to_string());
        }
    }

    /// Resolve the default state file path: ~/.local/state/scaleway-chat/state.toml
    pub fn default_path() -> Result<PathBuf> {
        let home = dirs::home_dir().ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Could not locate home directory",
            ))
        })?;
        Ok(home.join(".local/state/scaleway-chat/state.toml"))
    }

    /// Load state from the default path if it exists.
    pub fn load_default() -> Result<Option<Self>> {
        let path = Self::default_path()?;
        Self::load_from_path(&path)
    }

    /// Read and deserialize the state file from a specific path.
    pub fn load_from_path(path: &Path) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(path)?;
        let raw_val: toml::Value = toml::from_str(&contents).map_err(AppError::Toml)?;
        let version = raw_val
            .get("version")
            .and_then(|v| v.as_integer())
            .unwrap_or(2) as u32;

        if version < 4 {
            let instance_id = raw_val
                .get("instance_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let volume_id = raw_val
                .get("volume_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let public_ip_id = raw_val
                .get("public_ip_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let public_ip_address = raw_val
                .get("public_ip_address")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let snapshot_id = raw_val
                .get("snapshot_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    AppError::InvalidConfig("Legacy state missing snapshot_id".to_string())
                })?;
            let zone = raw_val
                .get("zone")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| AppError::InvalidConfig("Legacy state missing zone".to_string()))?;
            let created_at_str = raw_val
                .get("created_at")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let created_at = if let Some(ref c) = created_at_str {
                chrono::DateTime::parse_from_rfc3339(c)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now())
            } else {
                Utc::now()
            };

            let state = State {
                version: 4,
                phase: ProvisioningPhase::Ready,
                creation_mode: Some("snapshot_direct".to_string()),
                attempt_id: uuid::Uuid::new_v4().to_string(),
                selected_gpu_type: "L40S-1-48G".to_string(),
                snapshot_id,
                zone,
                instance_id,
                volume_id,
                public_ip_id,
                public_ip_address,
                attempted_gpu_types: vec!["L40S-1-48G".to_string()],
                created_at,
                path: Some(path.to_path_buf()),
            };
            return Ok(Some(state));
        }

        let mut state: State = toml::from_str(&contents).map_err(AppError::Toml)?;
        state.path = Some(path.to_path_buf());
        Ok(Some(state))
    }

    /// Save state back to its current path (or default path if unset).
    pub fn save_default(&self) -> Result<()> {
        let path = match &self.path {
            Some(p) => p.clone(),
            None => Self::default_path()?,
        };
        self.save_to_path(&path)
    }

    /// Atomically serialize and write the state using a temp-file swap.
    /// This prevents corruption if the process crashes mid-write.
    pub fn save_to_path(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = path.with_extension("tmp");
        let toml_str = toml::to_string_pretty(self)
            .map_err(|e| AppError::Io(std::io::Error::other(e.to_string())))?;

        // 1. Open the temporary file for writing
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;

        // 2. Set strict file permissions (0600 - Owner read/write only) to protect credentials
        let mut permissions = file.metadata()?.permissions();
        permissions.set_mode(0o600);
        file.set_permissions(permissions)?;

        // 3. Write TOML data and call sync_all to flush data blocks to disk
        file.write_all(toml_str.as_bytes())?;
        file.sync_all()?;
        drop(file);

        // 4. Atomically swap the temporary file to the final destination path
        fs::rename(&temp_path, path)?;

        Ok(())
    }

    /// Delete the local state file on complete resources cleanup.
    pub fn remove(&self) -> Result<()> {
        let path = match &self.path {
            Some(p) => p.clone(),
            None => Self::default_path()?,
        };
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}
