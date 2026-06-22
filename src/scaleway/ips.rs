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
use tracing::info;

use crate::error::{AppError, Result};
use crate::scaleway::client::ScalewayClient;
use crate::scaleway::models::{AttachIpRequest, CreateIpRequest, InstanceIp, IpResponse};

impl ScalewayClient {
    pub async fn allocate_public_ip(&self, attempt_id: &str) -> Result<IpResponse> {
        info!("[Scaleway] Allocating a new public IPv4 address...");

        let path = format!("/instance/v1/zones/{}/ips", self.zone);
        let request_payload = CreateIpRequest {
            project: self.project_id.clone(),
            ip_type: "routed_ipv4".to_string(),
            tags: vec![
                "managed-by=scaleway-chat".to_string(),
                "application=scaleway-chat".to_string(),
                format!("attempt-id={}", attempt_id),
            ],
        };

        let ip_resp: IpResponse = self
            .request(Method::POST, &path, |req| req.json(&request_payload))
            .await
            .map_err(|e| AppError::IpAllocationFailed(e.to_string()))?;

        info!(
            "[Scaleway] Public IP allocated: {} ({})",
            ip_resp.ip.address, ip_resp.ip.id
        );
        Ok(ip_resp)
    }

    /// Retrieves status and attachment information of a public IP.
    pub async fn get_public_ip(&self, ip_id: &str) -> Result<InstanceIp> {
        let path = format!("/instance/v1/zones/{}/ips/{}", self.zone, ip_id);
        let resp: IpResponse = self.request(Method::GET, &path, |req| req).await?;
        Ok(resp.ip)
    }

    /// Attaches an existing flexible IP to a target server instance.
    /// This uses the PATCH HTTP method on the `/ips/{ip_id}` endpoint.
    /// Crucially, the request payload must contain the server UUID as a direct string field:
    /// `{"server": "server-uuid-string"}`.
    pub async fn attach_ip_to_server(&self, ip_id: &str, server_id: &str) -> Result<()> {
        let ip_id_short = if ip_id.len() > 8 {
            format!("{}...", &ip_id[..8])
        } else {
            ip_id.to_string()
        };
        let server_id_short = if server_id.len() > 8 {
            format!("{}...", &server_id[..8])
        } else {
            server_id.to_string()
        };

        info!(
            "[Scaleway] Attaching public IP {} to server {}...",
            ip_id_short, server_id_short
        );
        info!("[Scaleway] Method: PATCH");
        info!(
            "[Scaleway] Endpoint: /instance/v1/zones/{}/ips/{}",
            self.zone, ip_id_short
        );

        let request_payload = AttachIpRequest {
            server: Some(server_id.to_string()),
        };
        let sanitized_payload = format!("{{\n  \"server\": \"{}\"\n}}", server_id_short);
        info!("[Scaleway] Request body: {}", sanitized_payload);

        let path = format!("/instance/v1/zones/{}/ips/{}", self.zone, ip_id);
        let payload_json =
            serde_json::to_string(&request_payload).unwrap_or_else(|_| "{}".to_string());
        tracing::debug!("Exact JSON payload: {}", payload_json);

        let _: serde_json::Value = self
            .request(Method::PATCH, &path, |req| req.json(&request_payload))
            .await
            .map_err(|e| AppError::IpAllocationFailed(format!("Failed to attach IP:\n{}", e)))?;

        info!("[Scaleway] Public IP attached successfully.");
        Ok(())
    }

    /// Releases and deletes an allocated flexible IP from Scaleway.
    /// Enforces the safety barrier protecting the source snapshot ID.
    pub async fn delete_public_ip(&self, ip_id: &str, snapshot_id: &str) -> Result<()> {
        info!("[Scaleway] Deleting public IP {}...", ip_id);

        // Safety assertion: prevent deletion targeting source snapshot
        if ip_id == snapshot_id {
            return Err(AppError::SafetyViolation(
                "Refusing to delete source snapshot as an IP".to_string(),
            ));
        }

        let path = format!("/instance/v1/zones/{}/ips/{}", self.zone, ip_id);

        match self
            .request_no_content(Method::DELETE, &path, |req| req)
            .await
        {
            Ok(_) => {
                info!("[Scaleway] Public IP deleted.");
                Ok(())
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("404") || err_str.contains("not_found") {
                    info!("[Scaleway] Public IP already deleted.");
                    Ok(())
                } else {
                    Err(AppError::CleanupIncomplete(format!(
                        "Failed to delete IP {}: {}",
                        ip_id, e
                    )))
                }
            }
        }
    }

    pub async fn verify_public_ip_deleted(
        &self,
        ip_id: &str,
        timeout: std::time::Duration,
        interval: std::time::Duration,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let path = format!("/instance/v1/zones/{}/ips/{}", self.zone, ip_id);
        loop {
            if start.elapsed() > timeout {
                return Err(AppError::CleanupIncomplete(format!(
                    "Timeout waiting for public IP {} deletion verification",
                    ip_id
                )));
            }
            match self
                .request::<IpResponse, _>(Method::GET, &path, |req| req)
                .await
            {
                Ok(_) => {
                    // Still exists, wait and poll
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("404") || err_str.contains("not_found") {
                        info!("[Cleanup] Public IP deletion verified.");
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(interval).await;
        }
    }
}
