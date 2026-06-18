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

use tracing::{error, info, warn};

use crate::config::Config;
use crate::error::{AppError, Result};
use crate::scaleway::client::ScalewayClient;
use crate::state::State;

impl ScalewayClient {
    pub async fn perform_cleanup(&self, config: &Config, mut state: State) -> Result<()> {
        let mut errors = Vec::new();
        let snapshot_id = &config.instance.snapshot_id;

        // Safety assertions
        if state.instance_id.as_ref() == Some(snapshot_id) {
            return Err(AppError::SafetyViolation(
                "Refusing to delete source snapshot".to_string(),
            ));
        }
        if state.volume_id.as_ref() == Some(snapshot_id) {
            return Err(AppError::SafetyViolation(
                "Refusing to delete source snapshot".to_string(),
            ));
        }
        if state.public_ip_id.as_ref() == Some(snapshot_id) {
            return Err(AppError::SafetyViolation(
                "Refusing to delete source snapshot".to_string(),
            ));
        }

        // 1. Power off and delete instance
        if let Some(ref server_id) = state.instance_id {
            info!("[Cleanup] Stopping and deleting Instance {}...", server_id);
            if let Err(e) = self.power_off_instance(server_id).await {
                warn!("Power off call failed: {}. Proceeding to delete...", e);
            }

            // Wait for stopped state
            let _ = self.wait_for_instance_stopped(server_id, 120, 5).await;

            match self.delete_instance(server_id, snapshot_id).await {
                Ok(_) => {
                    state.instance_id = None;
                    let _ = state.save_default();
                }
                Err(e) => {
                    error!("Failed to delete instance: {}", e);
                    errors.push(e);
                }
            }
        }

        // 2. Delete Volume
        if let Some(ref volume_id) = state.volume_id {
            info!(
                "[Cleanup] Deleting restored Block Storage volume {}...",
                volume_id
            );
            match self.delete_volume(volume_id, snapshot_id).await {
                Ok(_) => {
                    state.volume_id = None;
                    let _ = state.save_default();
                }
                Err(e) => {
                    error!("Failed to delete volume: {}", e);
                    errors.push(e);
                }
            }
        }

        // 3. Delete Public IP
        if let Some(ref ip_id) = state.public_ip_id {
            info!("[Cleanup] Deleting allocated public IP {}...", ip_id);
            match self.delete_public_ip(ip_id, snapshot_id).await {
                Ok(_) => {
                    state.public_ip_id = None;
                    state.public_ip_address = None;
                    let _ = state.save_default();
                }
                Err(e) => {
                    error!("Failed to delete public IP: {}", e);
                    errors.push(e);
                }
            }
        }

        // 4. Verify Snapshot Preserved
        info!("[Cleanup] Verifying source snapshot...");
        match self.get_snapshot(snapshot_id).await {
            Ok(_) => info!("[Cleanup] Snapshot preserved: {}", snapshot_id),
            Err(e) => warn!("Snapshot verification check failed: {}", e),
        }

        if errors.is_empty() {
            let _ = State::remove_default();
            info!("[Cleanup] Complete. GPU billing resources have been removed.");
            Ok(())
        } else {
            let remaining_report = format!(
                "Cleanup incomplete. State file retained. Existing resources: Instance: {:?}, Volume: {:?}, IP: {:?}",
                state.instance_id, state.volume_id, state.public_ip_id
            );
            error!("{}", remaining_report);
            Err(AppError::CleanupIncomplete(remaining_report))
        }
    }

    pub async fn provision_resources(&self, config: &Config, state: &mut State) -> Result<String> {
        // 1. Verify credentials and project
        self.validate_auth_and_project().await?;

        // 2. Validate snapshot
        info!("[Scaleway] Validating source snapshot...");
        self.get_snapshot(&config.instance.snapshot_id).await?;
        info!("[Scaleway] Snapshot is available in {}.", self.zone);

        // 3. Verify L40S-1-48G type in catalog
        self.validate_instance_type_available(&config.instance.instance_type)
            .await?;

        // Migration Check
        if state.version < 2 || state.creation_mode.as_deref() != Some("snapshot_direct") {
            info!(
                "[Scaleway] Legacy state (version {}) detected. Running migration checks...",
                state.version
            );
            if let Some(ref iid) = state.instance_id {
                match self.get_server(iid).await {
                    Ok(server) => {
                        info!(
                            "[Scaleway] Legacy instance {} exists. Migrating state to version 2...",
                            iid
                        );
                        let boot_volume_id = server
                            .volumes
                            .get("0")
                            .or_else(|| {
                                server
                                    .volumes
                                    .values()
                                    .find(|v| v.volume_type == "sbs_volume")
                            })
                            .map(|v| v.id.clone());
                        state.version = 2;
                        state.creation_mode = Some("snapshot_direct".to_string());
                        if let Some(ref vol_id) = boot_volume_id {
                            state.volume_id = Some(vol_id.clone());
                        }
                        state.save_default()?;
                    }
                    Err(_) => {
                        if state.volume_id.is_some() || state.public_ip_id.is_some() {
                            return Err(AppError::InvalidConfig(
                                "Stale legacy state detected with pre-created resources (volume/IP) but no instance exists on Scaleway. \
                                Please run 'scaleway-chat kill' to clean up these resources first.".to_string()
                            ));
                        } else {
                            // Empty state, safely migrate
                            state.version = 2;
                            state.creation_mode = Some("snapshot_direct".to_string());
                            state.save_default()?;
                        }
                    }
                }
            } else {
                if state.volume_id.is_some() || state.public_ip_id.is_some() {
                    return Err(AppError::InvalidConfig(
                        "Stale legacy state detected with pre-created resources (volume/IP) but no instance. \
                        Please run 'scaleway-chat kill' to clean up these resources first.".to_string()
                    ));
                } else {
                    // Empty state, safely migrate
                    state.version = 2;
                    state.creation_mode = Some("snapshot_direct".to_string());
                    state.save_default()?;
                }
            }
        }

        // 4. Create instance directly booting from snapshot if not exists
        let server = if let Some(ref iid) = state.instance_id {
            info!("[State] Adopting existing instance ID: {}", iid);
            let srv = self.get_server(iid).await?;
            if state.volume_id.is_none() {
                let boot_vol_id = srv
                    .volumes
                    .get("0")
                    .or_else(|| srv.volumes.values().find(|v| v.volume_type == "sbs_volume"))
                    .map(|v| v.id.clone())
                    .ok_or_else(|| {
                        AppError::InstanceCreationFailed(
                            "No boot volume found on existing instance".to_string(),
                        )
                    })?;
                state.volume_id = Some(boot_vol_id);
                state.save_default()?;
            }
            srv
        } else {
            info!("[Scaleway] Creating Instance directly from snapshot...");
            info!("[Scaleway] Instance creation request uses base_snapshot.");
            let srv = self
                .create_instance(
                    &config.instance.name,
                    &config.instance.instance_type,
                    &config.instance.snapshot_id,
                )
                .await?;
            state.instance_id = Some(srv.id.clone());
            state.save_default()?;
            info!("[Scaleway] Instance created.");

            // Discover and persist boot-volume ID
            let boot_vol_id = srv
                .volumes
                .get("0")
                .or_else(|| srv.volumes.values().find(|v| v.volume_type == "sbs_volume"))
                .map(|v| v.id.clone())
                .ok_or_else(|| {
                    AppError::InstanceCreationFailed(
                        "No boot volume returned in server creation response".to_string(),
                    )
                })?;
            state.volume_id = Some(boot_vol_id.clone());
            state.save_default()?;
            info!("[Scaleway] Boot volume created from snapshot.");
            info!("[Scaleway] Boot volume ID: {}", boot_vol_id);

            // Verify boot volume originated from the configured snapshot
            let volume = self.get_volume(&boot_vol_id).await?;
            if let Some(ref parent_snap) = volume.snapshot_id {
                if parent_snap != &config.instance.snapshot_id {
                    return Err(AppError::VolumeCreationFailed(format!(
                        "Volume snapshot ID mismatch: expected {}, found {}",
                        config.instance.snapshot_id, parent_snap
                    )));
                }
            }
            srv
        };

        // 5. Allocate public IP if not exists
        let ip_id = if let Some(ref ip_id) = state.public_ip_id {
            info!("[State] Adopting existing public IP ID: {}", ip_id);
            ip_id.clone()
        } else {
            info!("[Scaleway] Allocating public IP...");
            let ip_resp = self.allocate_public_ip().await?;
            state.public_ip_id = Some(ip_resp.ip.id.clone());
            state.public_ip_address = Some(ip_resp.ip.address.clone());
            state.save_default()?;
            ip_resp.ip.id
        };

        // 6. Idempotent public IP attachment & verification
        // 6.1 Retrieve current IP resource
        let ip_resource = self.get_public_ip(&ip_id).await?;
        let ip_addr = ip_resource.address.clone();

        // 6.2 Check whether it is already attached to the target Instance
        let should_attach = match ip_resource.server {
            Some(ref server_ref) => {
                if server_ref.id == server.id {
                    info!("[Scaleway] Public IP is already attached to this Instance.");
                    false
                } else {
                    return Err(AppError::IpAllocationFailed(format!(
                        "Conflicting attachment: Public IP {} is already attached to a different server: {} ({})",
                        ip_id, server_ref.name, server_ref.id
                    )));
                }
            }
            None => {
                info!("[Scaleway] Public IP is currently unattached.");
                true
            }
        };

        // 6.3 Attach it if unattached
        if should_attach {
            self.attach_ip_to_server(&ip_id, &server.id).await?;

            // 6.4 Verify the attachment afterward
            info!("[Scaleway] Verifying IP attachment...");
            let ip_verified = self.get_public_ip(&ip_id).await?;
            match ip_verified.server {
                Some(ref server_ref) if server_ref.id == server.id => {
                    info!("[Scaleway] Public IP attached successfully.");
                }
                _ => {
                    return Err(AppError::IpAllocationFailed(
                        "Verification failed: Public IP is still not attached to the target server after attach call".to_string()
                    ));
                }
            }
        }

        // 7. Power on the Instance if required
        if server.state != "running" {
            info!("[Scaleway] Powering on Instance...");
            self.power_on_instance(&server.id).await?;
        }

        // 8. Wait for the Instance to become running
        let _server = self
            .wait_for_instance_running(
                &server.id,
                config.timeouts.instance_creation_seconds,
                config.timeouts.instance_poll_interval_seconds,
            )
            .await?;

        // Keep state updated
        state.public_ip_address = Some(ip_addr.clone());
        state.save_default()?;

        Ok(ip_addr)
    }
}
