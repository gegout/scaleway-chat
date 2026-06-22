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

use std::time::Duration;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::error::{AppError, Result};
use crate::scaleway::client::ScalewayClient;
use crate::state::{ProvisioningPhase, State};
use crate::ProgressReporter;

#[derive(Debug, Clone)]
pub struct CleanupReport {
    pub instance_deleted: bool,
    pub volume_deleted: bool,
    pub ip_deleted: bool,
    pub snapshot_preserved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationOutcome {
    ResumeRunning {
        ip_address: String,
        boot_volume_id: Option<String>,
    },
    NeedsCleanupAndRestart,
    NoState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Compatibility {
    Compatible,
    Skip { reason: String },
}

impl ScalewayClient {
    pub async fn perform_cleanup(&self, config: &Config, mut state: State) -> Result<()> {
        let _report = self.cleanup_failed_attempt(config, &mut state).await?;
        Ok(())
    }

    pub async fn cleanup_failed_attempt(
        &self,
        config: &Config,
        state: &mut State,
    ) -> Result<CleanupReport> {
        let mut report = CleanupReport {
            instance_deleted: false,
            volume_deleted: false,
            ip_deleted: false,
            snapshot_preserved: false,
        };
        let mut errors = Vec::new();
        let snapshot_id = &config.instance.snapshot_id;
        let timeout = Duration::from_secs(config.timeouts.cleanup_timeout_seconds);
        let poll_interval = Duration::from_secs(config.timeouts.cleanup_poll_interval_seconds);

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

        // Set phase to CleaningUp
        state.phase = ProvisioningPhase::CleaningUp;
        let _ = state.save_default();

        // 1. Stop and Delete Instance
        if let Some(ref server_id) = state.instance_id {
            info!("[Cleanup] Stopping and deleting Instance {}...", server_id);
            if let Err(e) = self.power_off_instance(server_id).await {
                warn!("Power off call failed: {}. Proceeding to delete...", e);
            }

            // Wait for stopped state (max 120s, poll 5s)
            let _ = self.wait_for_instance_stopped(server_id, 120, 5).await;

            match self.delete_instance(server_id, snapshot_id).await {
                Ok(_) => {
                    // Verify instance deletion
                    match self
                        .verify_instance_deleted(server_id, timeout, poll_interval)
                        .await
                    {
                        Ok(_) => {
                            report.instance_deleted = true;
                            state.instance_id = None;
                            let _ = state.save_default();
                        }
                        Err(e) => {
                            error!("Instance deletion verification failed: {}", e);
                            errors.push(e);
                        }
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("404") || err_str.contains("not_found") {
                        report.instance_deleted = true;
                        state.instance_id = None;
                        let _ = state.save_default();
                    } else {
                        error!("Failed to delete instance: {}", e);
                        errors.push(e);
                    }
                }
            }
        } else {
            report.instance_deleted = true;
        }

        // 2. Delete boot volume
        if let Some(ref volume_id) = state.volume_id {
            info!(
                "[Cleanup] Deleting restored Block Storage volume {}...",
                volume_id
            );
            match self.delete_volume(volume_id, snapshot_id).await {
                Ok(_) => {
                    // Verify volume deletion
                    match self
                        .verify_volume_deleted(volume_id, timeout, poll_interval)
                        .await
                    {
                        Ok(_) => {
                            report.volume_deleted = true;
                            state.volume_id = None;
                            let _ = state.save_default();
                        }
                        Err(e) => {
                            error!("Volume deletion verification failed: {}", e);
                            errors.push(e);
                        }
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("404") || err_str.contains("not_found") {
                        report.volume_deleted = true;
                        state.volume_id = None;
                        let _ = state.save_default();
                    } else {
                        error!("Failed to delete volume: {}", e);
                        errors.push(e);
                    }
                }
            }
        } else {
            report.volume_deleted = true;
        }

        // 3. Delete Public IP
        if let Some(ref ip_id) = state.public_ip_id {
            info!("[Cleanup] Deleting allocated public IP {}...", ip_id);
            match self.delete_public_ip(ip_id, snapshot_id).await {
                Ok(_) => {
                    // Verify IP deletion
                    match self
                        .verify_public_ip_deleted(ip_id, timeout, poll_interval)
                        .await
                    {
                        Ok(_) => {
                            report.ip_deleted = true;
                            state.public_ip_id = None;
                            state.public_ip_address = None;
                            let _ = state.save_default();
                        }
                        Err(e) => {
                            error!("Public IP deletion verification failed: {}", e);
                            errors.push(e);
                        }
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("404") || err_str.contains("not_found") {
                        report.ip_deleted = true;
                        state.public_ip_id = None;
                        state.public_ip_address = None;
                        let _ = state.save_default();
                    } else {
                        error!("Failed to delete public IP: {}", e);
                        errors.push(e);
                    }
                }
            }
        } else {
            report.ip_deleted = true;
        }

        // 4. Verify Snapshot Preserved
        info!("[Cleanup] Verifying source snapshot...");
        match self.get_snapshot(snapshot_id).await {
            Ok(_) => {
                info!("[Cleanup] Snapshot preserved: {}", snapshot_id);
                report.snapshot_preserved = true;
            }
            Err(e) => {
                error!("Snapshot verification check failed: {}", e);
                errors.push(e);
            }
        }

        if errors.is_empty() {
            let _ = state.remove();
            info!("[Cleanup] Complete. GPU billing resources have been removed.");
            Ok(report)
        } else {
            state.phase = ProvisioningPhase::CleanupIncomplete;
            let _ = state.save_default();
            let remaining_report = format!(
                "Cleanup incomplete. State file retained. Existing resources: Instance: {:?}, Volume: {:?}, IP: {:?}",
                state.instance_id, state.volume_id, state.public_ip_id
            );
            error!("{}", remaining_report);
            Err(AppError::CleanupIncomplete(remaining_report))
        }
    }

    pub async fn reconcile_state(
        &self,
        _config: &Config,
        state: &State,
    ) -> Result<ReconciliationOutcome> {
        if state.instance_id.is_none() && state.volume_id.is_none() && state.public_ip_id.is_none()
        {
            return Ok(ReconciliationOutcome::NoState);
        }
        if let Some(ref iid) = state.instance_id {
            match self.get_server(iid).await {
                Ok(server) => {
                    if server.state == "running" && state.phase == ProvisioningPhase::Ready {
                        if let Some(ref ip_id) = state.public_ip_id {
                            match self.get_public_ip(ip_id).await {
                                Ok(ip_resource) => {
                                    if let Some(ref server_ref) = ip_resource.server {
                                        if server_ref.id != server.id {
                                            return Err(AppError::IpAllocationFailed(format!(
                                                "Conflicting attachment: Public IP {} is already attached to a different server: {} ({})",
                                                ip_id, server_ref.name, server_ref.id
                                            )));
                                        }
                                    }
                                }
                                Err(e) => {
                                    let err_str = e.to_string();
                                    if err_str.contains("404") || err_str.contains("not_found") {
                                        info!("[Recovery] Persisted public IP ID not found on remote. Stale IP. Cleanup required.");
                                        return Ok(ReconciliationOutcome::NeedsCleanupAndRestart);
                                    } else {
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        let boot_vol_id = server
                            .volumes
                            .get("0")
                            .or_else(|| {
                                server
                                    .volumes
                                    .values()
                                    .find(|v| v.volume_type == "sbs_volume")
                            })
                            .map(|v| v.id.clone());

                        if let Some(ref ip) = state.public_ip_address {
                            info!(
                                "[Recovery] Found running Instance. Resuming session at IP: {}",
                                ip
                            );
                            return Ok(ReconciliationOutcome::ResumeRunning {
                                ip_address: ip.clone(),
                                boot_volume_id: boot_vol_id,
                            });
                        }
                    }
                    info!("[Recovery] Found non-running/incomplete Instance (state: {}). Cleanup required.", server.state);
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("404") || err_str.contains("not_found") {
                        info!("[Recovery] Persisted Instance ID not found on remote. Stale state file. Cleanup required.");
                    } else {
                        return Err(e);
                    }
                }
            }
        } else {
            info!("[Recovery] Incomplete provisioning state (no instance, but other resources exist). Cleanup required.");
        }
        Ok(ReconciliationOutcome::NeedsCleanupAndRestart)
    }

    pub async fn validate_candidate_compatibility(
        &self,
        _config: &Config,
        gpu_type: &str,
        snapshot: &crate::scaleway::models::Snapshot,
    ) -> Result<Compatibility> {
        let path = format!("/instance/v1/zones/{}/products/servers", self.zone);
        let types_resp: crate::scaleway::models::ServerTypesResponse =
            self.request(reqwest::Method::GET, &path, |req| req).await?;

        let product = match types_resp.servers.get(gpu_type) {
            Some(p) => p,
            None => {
                return Ok(Compatibility::Skip {
                    reason: format!("Product {} is not offered in {}", gpu_type, self.zone),
                });
            }
        };

        // 1. Verify minimum size constraints if volumes_constraint is present
        if let Some(ref constraints) = product.volumes_constraint {
            if snapshot.size < constraints.min_size {
                return Ok(Compatibility::Skip {
                    reason: format!(
                        "Snapshot size {} bytes is below product minimum constraint of {} bytes",
                        snapshot.size, constraints.min_size
                    ),
                });
            }
            if constraints.max_size > 0 && snapshot.size > constraints.max_size {
                return Ok(Compatibility::Skip {
                    reason: format!(
                        "Snapshot size {} bytes exceeds product maximum constraint of {} bytes",
                        snapshot.size, constraints.max_size
                    ),
                });
            }
        }

        // 2. Arch compatibility check (if returned in metadata)
        if let Some(ref arch) = product.arch {
            if arch != "x86_64" {
                return Ok(Compatibility::Skip {
                    reason: format!(
                        "Product architecture {} is incompatible (expected x86_64)",
                        arch
                    ),
                });
            }
        }

        info!(
            "Instance type {} is offered in {}. Actual capacity will be confirmed during creation or power-on.",
            gpu_type, self.zone
        );

        Ok(Compatibility::Compatible)
    }

    pub async fn provision_single_attempt(
        &self,
        config: &Config,
        state: &mut State,
        progress: &dyn ProgressReporter,
        gpu_type: &str,
    ) -> Result<String> {
        let attempt_id = state.attempt_id.clone();
        let snapshot_id = &config.instance.snapshot_id;

        // 1. Create instance directly booting from snapshot
        state.phase = ProvisioningPhase::CreatingInstance;
        state.save_default()?;

        let clean_gpu_type = gpu_type.to_lowercase().replace('_', "-");
        let short_uuid = &attempt_id[..8];
        let attempt_name = format!("{}-{}-{}", config.instance.name, clean_gpu_type, short_uuid);

        let srv = self
            .create_instance(&attempt_name, gpu_type, snapshot_id, &attempt_id)
            .await;

        let srv = match srv {
            Ok(s) => s,
            Err(e) => {
                if e.is_out_of_stock() {
                    return Err(AppError::OutOfStock {
                        gpu_type: gpu_type.to_string(),
                        zone: self.zone.clone(),
                    });
                }
                return Err(e);
            }
        };

        state.instance_id = Some(srv.id.clone());
        state.save_default()?;

        // 2. Discover and persist generated boot-volume ID
        state.phase = ProvisioningPhase::DiscoveringBootVolume;
        state.save_default()?;

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

        // Verify boot volume originated from the configured snapshot
        let volume = self.get_volume(&boot_vol_id).await?;
        if let Some(ref parent_snap) = volume.snapshot_id {
            if parent_snap != snapshot_id {
                return Err(AppError::IncompatibleVolume {
                    gpu_type: gpu_type.to_string(),
                    reason: format!(
                        "Volume snapshot ID mismatch: expected {}, found {}",
                        snapshot_id, parent_snap
                    ),
                });
            }
        }

        // 3. Power on the Instance if required
        state.phase = ProvisioningPhase::PoweringOn;
        state.save_default()?;

        if srv.state != "running" {
            if let Err(e) = self.power_on_instance(&srv.id).await {
                if e.is_out_of_stock() {
                    return Err(AppError::OutOfStock {
                        gpu_type: gpu_type.to_string(),
                        zone: self.zone.clone(),
                    });
                }
                return Err(e);
            }
        }

        // 4. Wait for running state
        state.phase = ProvisioningPhase::WaitingForRunning;
        state.save_default()?;

        let srv_running = self
            .wait_for_instance_running_with_progress(
                &srv.id,
                config.timeouts.instance_creation_seconds,
                config.timeouts.instance_poll_interval_seconds,
                |p, m| progress.report(p, m),
            )
            .await?;

        // 5. Allocate public IP
        state.phase = ProvisioningPhase::AllocatingIp;
        state.save_default()?;

        info!(
            "[Scaleway] Allocating public IP for attempt {}...",
            attempt_id
        );
        let ip_resp = self.allocate_public_ip(&attempt_id).await?;
        let ip_id = ip_resp.ip.id.clone();
        let ip_addr = ip_resp.ip.address.clone();

        state.public_ip_id = Some(ip_id.clone());
        state.public_ip_address = Some(ip_addr.clone());
        state.save_default()?;

        // 6. Attach IP to server and verify
        self.attach_ip_to_server(&ip_id, &srv_running.id).await?;

        // Verify attachment
        let ip_verified = self.get_public_ip(&ip_id).await?;
        match ip_verified.server {
            Some(ref server_ref) if server_ref.id == srv_running.id => {
                info!("[Scaleway] Public IP attached successfully.");
            }
            _ => {
                return Err(AppError::IpAllocationFailed(
                    "Verification failed: Public IP is still not attached to the target server after attach call".to_string()
                ));
            }
        }

        // 7. Wait for Nemotron
        state.phase = ProvisioningPhase::WaitingForNemotron;
        state.save_default()?;

        let nemotron_client = crate::nemotron::NemotronClient::new(
            ip_addr.clone(),
            config.nemotron.port,
            config.nemotron.api_key.clone(),
            config.nemotron.model.clone(),
            config.timeouts.inference_timeout_seconds,
        );

        let nemotron_wait = nemotron_client
            .wait_for_ready_with_progress(
                config.timeouts.nemotron_startup_seconds,
                config.timeouts.nemotron_poll_interval_seconds,
                &|p, m| progress.report(p, m),
            )
            .await;

        match nemotron_wait {
            Ok(_) => Ok(ip_addr),
            Err(e) => {
                if let AppError::GuestRuntimeIncompatible { reason, .. } = e {
                    Err(AppError::GuestRuntimeIncompatible {
                        gpu_type: gpu_type.to_string(),
                        reason,
                    })
                } else {
                    Err(e)
                }
            }
        }
    }

    pub async fn provision_resources(
        &self,
        config: &Config,
        state: &mut State,
        progress: &dyn ProgressReporter,
    ) -> Result<String> {
        self.ensure_ready(config, state, progress).await
    }

    pub async fn ensure_ready(
        &self,
        config: &Config,
        state: &mut State,
        progress: &dyn ProgressReporter,
    ) -> Result<String> {
        if state.version < 4 {
            info!("[State] Upgrading in-memory legacy state to version 4...");
            state.version = 4;
            state.phase = ProvisioningPhase::Ready;
            state.creation_mode = Some("snapshot_direct".to_string());
            if state.attempt_id.is_empty() {
                state.attempt_id = uuid::Uuid::new_v4().to_string();
            }
            if state.selected_gpu_type.is_empty() {
                state.selected_gpu_type = "L40S-1-48G".to_string();
            }
            if state.attempted_gpu_types.is_empty() {
                state.attempted_gpu_types = vec!["L40S-1-48G".to_string()];
            }
            let _ = state.save_default();
        }

        // 1. Reconciliation: check if state contains incomplete resources and clean them up
        let reconciliation = self.reconcile_state(config, state).await?;
        match reconciliation {
            ReconciliationOutcome::ResumeRunning {
                ip_address,
                boot_volume_id,
            } => {
                if state.volume_id.is_none() && boot_volume_id.is_some() {
                    state.volume_id = boot_volume_id;
                    let _ = state.save_default();
                }
                progress.report(100, "Resumed existing running session.");
                return Ok(ip_address);
            }
            ReconciliationOutcome::NeedsCleanupAndRestart => {
                progress.report(10, "Cleaning up incomplete session from previous run...");
                self.cleanup_failed_attempt(config, state).await?;
                let path = state.path.clone();
                *state = State::new(
                    config.instance.snapshot_id.clone(),
                    config.scaleway.zone.clone(),
                );
                state.path = path;
                state.save_default()?;
            }
            ReconciliationOutcome::NoState => {}
        }

        let gpu_list = config.instance.effective_gpu_types();

        // Verify credentials and snapshot once before loop (preflight validation)
        self.validate_auth_and_project().await?;
        let snapshot = self.get_snapshot(&config.instance.snapshot_id).await?;

        for gpu_type in &gpu_list {
            progress.report(15, &format!("Checking compatibility for {}...", gpu_type));
            match self
                .validate_candidate_compatibility(config, gpu_type, &snapshot)
                .await
            {
                Ok(Compatibility::Compatible) => {}
                Ok(Compatibility::Skip { reason }) => {
                    warn!("[Compatibility] Skipping GPU type {}: {}", gpu_type, reason);
                    continue;
                }
                Err(e) => {
                    return Err(e);
                }
            }

            if gpu_type == "L40S-2-48G" {
                info!("[Compatibility] L40S-2-48G provides two GPUs.");
                info!("[Compatibility] The current Nemotron configuration may use only one GPU.");
            }

            state.start_attempt(gpu_type);
            state.save_default()?;

            info!("[Provisioning] Trying {} in {}...", gpu_type, self.zone);
            progress.report(20, &format!("Provisioning {}...", gpu_type));

            let outcome = self
                .provision_single_attempt(config, state, progress, gpu_type)
                .await;
            match outcome {
                Ok(ip) => {
                    state.phase = ProvisioningPhase::Ready;
                    state.save_default()?;
                    progress.report(100, "Successfully provisioned GPU instance.");
                    return Ok(ip);
                }
                Err(err) => {
                    error!("[Provisioning] Attempt for {} failed: {}", gpu_type, err);

                    if err.should_try_next_gpu() {
                        progress.report(
                            85,
                            &format!("Cleaning up failed attempt for {}...", gpu_type),
                        );
                        let cleanup_res = self.cleanup_failed_attempt(config, state).await;
                        if let Err(cleanup_err) = cleanup_res {
                            eprintln!(
                                "CRITICAL: Cleanup failed with: {:?}. Original error: {:?}",
                                cleanup_err, err
                            );
                            error!(
                                "[Cleanup] Cleanup failed and is incomplete: {}",
                                cleanup_err
                            );
                            return Err(cleanup_err);
                        }
                        let path = state.path.clone();
                        *state = State::new(
                            config.instance.snapshot_id.clone(),
                            config.scaleway.zone.clone(),
                        );
                        state.path = path;
                        state.save_default()?;
                        continue;
                    } else {
                        progress.report(85, "Cleaning up and stopping due to hard error...");
                        let cleanup_res = self.cleanup_failed_attempt(config, state).await;
                        if let Err(cleanup_err) = cleanup_res {
                            eprintln!("CRITICAL: Hard error cleanup failed with: {:?}. Original error: {:?}", cleanup_err, err);
                            error!(
                                "[Cleanup] Cleanup failed and is incomplete: {}",
                                cleanup_err
                            );
                            return Err(cleanup_err);
                        }
                        return Err(err);
                    }
                }
            }
        }

        let final_err = AppError::NoCompatibleGpuAvailable {
            zone: self.zone.clone(),
            attempted: gpu_list,
        };
        error!("[Provisioning] All GPU candidates exhausted or failed.");
        Err(final_err)
    }

    pub async fn get_status(&self, config: &Config, state: &State) -> Result<String> {
        use secrecy::ExposeSecret;
        let mut report = String::new();

        // 1. Snapshot status
        let snapshot_status = match self.get_snapshot(&config.instance.snapshot_id).await {
            Ok(s) => format!(
                "found (status: '{}', size: {} GB)",
                s.status,
                s.size / 1_000_000_000
            ),
            Err(e) => format!("error: {}", e),
        };
        report.push_str(&format!(
            "<b>Source Snapshot:</b> {} ({})\n",
            config.instance.snapshot_id, snapshot_status
        ));

        // 2. Instance status
        let instance_status = if let Some(ref iid) = state.instance_id {
            match self.get_server(iid).await {
                Ok(srv) => format!("found (state: '{}')", srv.state),
                Err(e) => format!("error checking: {}", e),
            }
        } else {
            "None".to_string()
        };
        report.push_str(&format!(
            "<b>GPU Instance:</b> {:?} ({})\n",
            state.instance_id, instance_status
        ));

        // 3. Volume status
        let volume_status = if let Some(ref vid) = state.volume_id {
            match self.get_volume(vid).await {
                Ok(v) => format!("found (status: '{}')", v.status),
                Err(e) => format!("error checking: {}", e),
            }
        } else {
            "None".to_string()
        };
        report.push_str(&format!(
            "<b>Boot Volume:</b> {:?} ({})\n",
            state.volume_id, volume_status
        ));

        // 4. IP status
        let ip_status = if let Some(ref ipid) = state.public_ip_id {
            match self.get_public_ip(ipid).await {
                Ok(ip) => format!("allocated (address: {})", ip.address),
                Err(e) => format!("error checking: {}", e),
            }
        } else {
            "None".to_string()
        };
        report.push_str(&format!(
            "<b>Flexible IP:</b> {:?} ({})\n",
            state.public_ip_id, ip_status
        ));

        // 5. Endpoint & Model readiness status
        if let Some(ref ip) = state.public_ip_address {
            let endpoint = format!("http://{}:{}", ip, config.nemotron.port);
            report.push_str(&format!("<b>Inference Endpoint:</b> {}\n", endpoint));

            let nemotron_client = crate::nemotron::NemotronClient::new(
                ip.clone(),
                config.nemotron.port,
                config.nemotron.api_key.clone(),
                config.nemotron.model.clone(),
                10, // short timeout: only used for /v1/models status probe
            );
            let url = format!("{}/models", nemotron_client.endpoint());
            let model_status = match nemotron_client
                .http_client()
                .get(&url)
                .header(
                    "Authorization",
                    format!("Bearer {}", nemotron_client.api_key().expose_secret()),
                )
                .send()
                .await
            {
                Ok(resp) => {
                    if resp.status().is_success() {
                        "ready".to_string()
                    } else if resp.status().as_u16() == 503 {
                        "loading (HTTP 503)".to_string()
                    } else {
                        format!("error (HTTP {})", resp.status())
                    }
                }
                Err(_) => "offline/loading".to_string(),
            };
            report.push_str(&format!("<b>Model Readiness:</b> {}\n", model_status));
        } else {
            report.push_str("<b>Inference Endpoint:</b> None\n");
            report.push_str("<b>Model Readiness:</b> offline\n");
        }

        Ok(report)
    }
}
