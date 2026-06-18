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
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::error::{AppError, Result};
use crate::scaleway::client::ScalewayClient;
use crate::scaleway::models::{CreateVolumeRequest, SnapshotSource, Volume};

impl ScalewayClient {
    pub async fn create_volume_from_snapshot(
        &self,
        snapshot_id: &str,
        volume_name: &str,
    ) -> Result<Volume> {
        info!(
            "[Scaleway] Creating Block Storage volume from snapshot {}...",
            snapshot_id
        );

        let path = format!("/block/v1/zones/{}/volumes", self.zone);
        let request_payload = CreateVolumeRequest {
            name: volume_name.to_string(),
            project_id: self.project_id.clone(),
            perf_iops: 5000, // standard SBS performance
            from_snapshot: Some(SnapshotSource {
                snapshot_id: snapshot_id.to_string(),
            }),
            tags: vec![
                "managed-by=scaleway-chat".to_string(),
                "application=scaleway-chat".to_string(),
                format!("snapshot-id={}", snapshot_id),
            ],
        };

        let volume: Volume = self
            .request(Method::POST, &path, |req| req.json(&request_payload))
            .await
            .map_err(|e| AppError::VolumeCreationFailed(e.to_string()))?;

        info!("[Scaleway] Volume restoration initiated: {}", volume.id);
        Ok(volume)
    }

    pub async fn wait_for_volume_ready(
        &self,
        volume_id: &str,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> Result<Volume> {
        info!(
            "[Scaleway] Waiting for volume {} to become available...",
            volume_id
        );

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let interval = Duration::from_secs(poll_interval_secs);
        let path = format!("/block/v1/zones/{}/volumes/{}", self.zone, volume_id);

        loop {
            if start.elapsed() > timeout {
                return Err(AppError::VolumeCreationFailed(
                    "Timeout waiting for volume to become available".to_string(),
                ));
            }

            match self
                .request::<Volume, _>(Method::GET, &path, |req| req)
                .await
            {
                Ok(volume) => {
                    debug!("Volume {} status: {}", volume_id, volume.status);
                    if volume.status == "available" {
                        info!("[Scaleway] Volume is ready.");
                        return Ok(volume);
                    }
                    if volume.status == "error" || volume.status == "failed" {
                        return Err(AppError::VolumeCreationFailed(format!(
                            "Volume reached failed state: {}",
                            volume.status
                        )));
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to query volume status: {}. Retrying...", e);
                }
            }

            tokio::time::sleep(interval).await;
        }
    }

    /// Deletes a restored Block Storage volume.
    /// Incorporates a retry loop (up to 6 attempts with a 5-second interval) to handle
    /// HTTP 412 (Precondition Failed) errors when the volume is still in detaching state.
    /// Also enforces the critical safety barrier protecting the source snapshot ID.
    pub async fn delete_volume(&self, volume_id: &str, snapshot_id: &str) -> Result<()> {
        info!("[Scaleway] Deleting restored volume {}...", volume_id);

        // Safety assertion: prevent deletion call targeting source snapshot
        if volume_id == snapshot_id {
            return Err(AppError::SafetyViolation(
                "Refusing to delete source snapshot as a volume".to_string(),
            ));
        }

        let path = format!("/block/v1/zones/{}/volumes/{}", self.zone, volume_id);

        let mut attempts = 0;
        let max_attempts = 6;
        let delay = Duration::from_secs(5);

        loop {
            attempts += 1;
            match self
                .request_no_content(Method::DELETE, &path, |req| req)
                .await
            {
                Ok(_) => {
                    info!("[Scaleway] Volume deleted.");
                    return Ok(());
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("404") || err_str.contains("not_found") {
                        info!("[Scaleway] Volume already deleted.");
                        return Ok(());
                    }
                    // If instance delete completes but the block driver hasn't fully unmounted
                    // or detached the volume yet, the API returns HTTP 412 Precondition Failed.
                    if (err_str.contains("412") || err_str.contains("precondition"))
                        && attempts < max_attempts
                    {
                        warn!(
                            "Volume deletion precondition failed (volume might still be detaching). Retrying in {:?} (attempt {}/{})",
                            delay, attempts, max_attempts
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(AppError::CleanupIncomplete(format!(
                        "Failed to delete volume {}: {}",
                        volume_id, e
                    )));
                }
            }
        }
    }

    /// Retrieves volume metadata, including status and provenance snapshot details.
    pub async fn get_volume(&self, volume_id: &str) -> Result<Volume> {
        let path = format!("/block/v1/zones/{}/volumes/{}", self.zone, volume_id);
        self.request(Method::GET, &path, |req| req).await
    }
}
