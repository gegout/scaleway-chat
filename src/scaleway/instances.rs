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

use reqwest::Method;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info};

use crate::error::{AppError, Result};
use crate::scaleway::client::ScalewayClient;
use crate::scaleway::models::{
    CreateServerRequest, InstanceVolumeType, Server, ServerActionRequest, ServerResponse,
    ServerTypesResponse, SnapshotBootVolume,
};

impl ScalewayClient {
    /// Queries the Scaleway server products API to check if the requested
    /// instance type (e.g. "L40S-1-48G") is supported in the current zone.
    pub async fn validate_instance_type_available(&self, instance_type: &str) -> Result<()> {
        info!(
            "[Scaleway] Checking {} availability in {}...",
            instance_type, self.zone
        );
        let path = format!("/instance/v1/zones/{}/products/servers", self.zone);

        let types_resp: ServerTypesResponse = self.request(Method::GET, &path, |req| req).await?;

        if !types_resp.servers.contains_key(instance_type) {
            return Err(AppError::InstanceTypeUnavailable(format!(
                "Instance type {} is not available in zone {}. Available types: {:?}",
                instance_type,
                self.zone,
                types_resp.servers.keys().collect::<Vec<_>>()
            )));
        }

        info!(
            "[Scaleway] Instance type {} is supported in zone.",
            instance_type
        );
        Ok(())
    }

    /// Creates a GPU virtual server directly from the golden snapshot ID.
    /// This defines a custom volumes map mapping "0" to `SnapshotBootVolume` with `base_snapshot`.
    /// The API automatically spawns and registers a block boot volume on server creation.
    pub async fn create_instance(
        &self,
        name: &str,
        instance_type: &str,
        snapshot_id: &str,
    ) -> Result<Server> {
        info!(
            "[Scaleway] Creating GPU Instance {} directly from snapshot...",
            name
        );

        let path = format!("/instance/v1/zones/{}/servers", self.zone);
        let base_snap_uuid = uuid::Uuid::parse_str(snapshot_id)
            .map_err(|e| AppError::InvalidConfig(format!("Invalid snapshot UUID: {}", e)))?;

        // Construct the root boot volume restore config using the snapshot ID
        let mut volumes = HashMap::new();
        volumes.insert(
            "0".to_string(),
            SnapshotBootVolume {
                base_snapshot: base_snap_uuid,
                name: format!("{}-root", name),
                volume_type: InstanceVolumeType::SbsVolume,
                boot: true,
            },
        );

        let project_uuid = uuid::Uuid::parse_str(&self.project_id)
            .map_err(|e| AppError::InvalidConfig(format!("Invalid project UUID: {}", e)))?;

        let request_payload = CreateServerRequest {
            name: name.to_string(),
            project: project_uuid,
            commercial_type: instance_type.to_string(),
            volumes,
            tags: vec![
                "managed-by=scaleway-chat".to_string(),
                "application=scaleway-chat".to_string(),
            ],
        };

        // For debugging, we log the sanitized creation JSON.
        if tracing::enabled!(tracing::Level::DEBUG) {
            let serialized = serde_json::to_string(&request_payload)
                .unwrap_or_else(|_| "Serialization failed".to_string());
            debug!("Sanitized Instance creation JSON: {}", serialized);
        }

        let server_resp: ServerResponse = self
            .request(Method::POST, &path, |req| req.json(&request_payload))
            .await
            .map_err(|e| AppError::InstanceCreationFailed(e.to_string()))?;

        info!(
            "[Scaleway] Instance created: {} ({})",
            server_resp.server.name, server_resp.server.id
        );
        Ok(server_resp.server)
    }

    /// Powers on a stopped Scaleway server instance.
    pub async fn power_on_instance(&self, server_id: &str) -> Result<()> {
        info!("[Scaleway] Powering on the Instance...");
        let path = format!(
            "/instance/v1/zones/{}/servers/{}/action",
            self.zone, server_id
        );
        let request_payload = ServerActionRequest {
            action: "poweron".to_string(),
        };

        let _: serde_json::Value = self
            .request(Method::POST, &path, |req| req.json(&request_payload))
            .await
            .map_err(|e| AppError::InstanceCreationFailed(format!("Failed to power on: {}", e)))?;

        info!("[Scaleway] Power-on action sent.");
        Ok(())
    }

    /// Powers off an active Scaleway server instance.
    /// If the instance is already stopped, any 400/conflict error is safely ignored.
    pub async fn power_off_instance(&self, server_id: &str) -> Result<()> {
        info!("[Cleanup] Powering off Instance...");
        let path = format!(
            "/instance/v1/zones/{}/servers/{}/action",
            self.zone, server_id
        );
        let request_payload = ServerActionRequest {
            action: "poweroff".to_string(),
        };

        match self
            .request::<serde_json::Value, _>(Method::POST, &path, |req| req.json(&request_payload))
            .await
        {
            Ok(_) => {
                info!("[Cleanup] Power-off action sent.");
                Ok(())
            }
            Err(e) => {
                // If it's already stopped, ignore
                debug!("Power off error (might be already stopped): {}", e);
                Ok(())
            }
        }
    }

    /// Periodically queries the server GET endpoint to wait until the instance
    /// status is "running" or fails if it hits "error" state or times out.
    pub async fn wait_for_instance_running(
        &self,
        server_id: &str,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> Result<Server> {
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let interval = Duration::from_secs(poll_interval_secs);
        let path = format!("/instance/v1/zones/{}/servers/{}", self.zone, server_id);

        loop {
            if start.elapsed() > timeout {
                return Err(AppError::InstanceStartupTimeout);
            }

            match self
                .request::<ServerResponse, _>(Method::GET, &path, |req| req)
                .await
            {
                Ok(resp) => {
                    let server = resp.server;
                    info!("[Scaleway] Instance state: {}", server.state);
                    if server.state == "running" {
                        return Ok(server);
                    }
                    if server.state == "error" {
                        return Err(AppError::InstanceCreationFailed(
                            "Instance entered error state".to_string(),
                        ));
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to query server status: {}. Retrying...", e);
                }
            }

            tokio::time::sleep(interval).await;
        }
    }

    /// Periodically queries the server GET endpoint to wait until the instance
    /// status becomes "stopped" (meaning it has shut down and is ready for deletion).
    /// If the server is already deleted (returns 404), this is treated as a success.
    pub async fn wait_for_instance_stopped(
        &self,
        server_id: &str,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let interval = Duration::from_secs(poll_interval_secs);
        let path = format!("/instance/v1/zones/{}/servers/{}", self.zone, server_id);

        loop {
            if start.elapsed() > timeout {
                return Err(AppError::CleanupIncomplete(
                    "Timeout waiting for server to stop".to_string(),
                ));
            }

            match self
                .request::<ServerResponse, _>(Method::GET, &path, |req| req)
                .await
            {
                Ok(resp) => {
                    let server = resp.server;
                    debug!("Instance state during shutdown: {}", server.state);
                    if server.state == "stopped" {
                        info!("[Cleanup] Instance stopped.");
                        return Ok(());
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("404") || err_str.contains("not_found") {
                        info!("[Cleanup] Instance not found (already deleted).");
                        return Ok(());
                    }
                    tracing::warn!(
                        "Failed to query server status during shutdown: {}. Retrying...",
                        e
                    );
                }
            }

            tokio::time::sleep(interval).await;
        }
    }

    /// Deletes a stopped server instance.
    /// Incorporates a CRITICAL safety check ensuring that we never attempt to delete the source snapshot.
    pub async fn delete_instance(&self, server_id: &str, snapshot_id: &str) -> Result<()> {
        info!("[Cleanup] Deleting Instance {}...", server_id);

        // Safety assertion: Make absolutely sure the instance ID to delete is not the snapshot ID
        if server_id == snapshot_id {
            return Err(AppError::SafetyViolation(
                "Refusing to delete source snapshot as a server".to_string(),
            ));
        }

        let path = format!("/instance/v1/zones/{}/servers/{}", self.zone, server_id);

        match self
            .request_no_content(Method::DELETE, &path, |req| req)
            .await
        {
            Ok(_) => {
                info!("[Cleanup] Instance deleted.");
                Ok(())
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("404") || err_str.contains("not_found") {
                    info!("[Cleanup] Instance already deleted.");
                    Ok(())
                } else {
                    Err(AppError::CleanupIncomplete(format!(
                        "Failed to delete server {}: {}",
                        server_id, e
                    )))
                }
            }
        }
    }

    pub async fn get_server(&self, server_id: &str) -> Result<Server> {
        let path = format!("/instance/v1/zones/{}/servers/{}", self.zone, server_id);
        let resp: ServerResponse = self.request(Method::GET, &path, |req| req).await?;
        Ok(resp.server)
    }
}
