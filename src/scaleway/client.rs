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
use reqwest::{Client, Method, RequestBuilder, Response, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use std::time::Duration;
use tracing::{debug, warn};

use crate::config::Config;
use crate::error::{AppError, Result};

#[derive(Clone)]
pub struct ScalewayClient {
    http_client: Client,
    secret_key: SecretString,
    pub project_id: String,
    pub organization_id: String,
    pub zone: String,
    base_url: String,
}

impl ScalewayClient {
    pub fn new(config: &Config) -> Self {
        Self {
            http_client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            secret_key: config.scaleway.secret_key.clone(),
            project_id: config.scaleway.project_id.clone(),
            organization_id: config.scaleway.organization_id.clone(),
            zone: config.scaleway.zone.clone(),
            base_url: "https://api.scaleway.com".to_string(),
        }
    }

    pub fn new_with_url(config: &Config, base_url: String) -> Self {
        Self {
            http_client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            secret_key: config.scaleway.secret_key.clone(),
            project_id: config.scaleway.project_id.clone(),
            organization_id: config.scaleway.organization_id.clone(),
            zone: config.scaleway.zone.clone(),
            base_url,
        }
    }

    pub async fn request<T, F>(&self, method: Method, path: &str, configure: F) -> Result<T>
    where
        F: Fn(RequestBuilder) -> RequestBuilder,
        T: for<'de> serde::Deserialize<'de>,
    {
        let resp = self.send_request(method, path, configure).await?;
        let bytes = resp.bytes().await?;
        let val = serde_json::from_slice(&bytes)?;
        Ok(val)
    }

    pub async fn request_no_content<F>(
        &self,
        method: Method,
        path: &str,
        configure: F,
    ) -> Result<()>
    where
        F: Fn(RequestBuilder) -> RequestBuilder,
    {
        let _resp = self.send_request(method, path, configure).await?;
        Ok(())
    }

    async fn send_request<F>(&self, method: Method, path: &str, configure: F) -> Result<Response>
    where
        F: Fn(RequestBuilder) -> RequestBuilder,
    {
        let mut attempt = 0;
        let max_attempts = 5;
        let mut delay = Duration::from_millis(500);

        loop {
            attempt += 1;
            let url = format!("{}{}", self.base_url, path);
            debug!("Sending {} request to {}", method, path);

            let mut req = self
                .http_client
                .request(method.clone(), &url)
                .header("X-Auth-Token", self.secret_key.expose_secret())
                .header("User-Agent", "scaleway-chat/0.1.0");

            req = configure(req);

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return Ok(resp);
                    }

                    if attempt >= max_attempts {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(self.map_api_error(status, body));
                    }

                    // Check if error is transient / retryable
                    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                        let retry_after = resp
                            .headers()
                            .get("Retry-After")
                            .and_then(|h| h.to_str().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .map(Duration::from_secs);

                        let wait = retry_after.unwrap_or_else(|| {
                            let jitter = rand::random::<u64>() % 100;
                            let next_delay = delay + Duration::from_millis(jitter);
                            delay *= 2;
                            next_delay
                        });

                        warn!(
                            "Request failed with status {}. Retrying in {:?} (attempt {}/{})",
                            status, wait, attempt, max_attempts
                        );
                        tokio::time::sleep(wait).await;
                        continue;
                    } else {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(self.map_api_error(status, body));
                    }
                }
                Err(e) => {
                    if attempt >= max_attempts {
                        return Err(AppError::Reqwest(e));
                    }

                    let wait = delay + Duration::from_millis(rand::random::<u64>() % 100);
                    delay *= 2;
                    warn!(
                        "Network connection error: {}. Retrying in {:?} (attempt {}/{})",
                        e, wait, attempt, max_attempts
                    );
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    pub fn map_api_error(&self, status: StatusCode, body: String) -> AppError {
        // Attempt to parse the body as a Scaleway structured API error
        let parsed_err = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|val| {
                let msg = val.get("message")?.as_str()?;
                let err_type = val
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("unknown_error");
                let mut detail_msgs = Vec::new();
                if let Some(details) = val.get("details").and_then(|d| d.as_array()) {
                    for det in details {
                        let arg = det
                            .get("argument_name")
                            .and_then(|a| a.as_str())
                            .unwrap_or("");
                        let reason = det.get("reason").and_then(|r| r.as_str()).unwrap_or("");
                        let help = det
                            .get("help_message")
                            .and_then(|h| h.as_str())
                            .unwrap_or("");
                        detail_msgs.push(format!(
                            "- argument_name: {}\n  reason: {}\n  help_message: {}",
                            arg, reason, help
                        ));
                    }
                }

                if detail_msgs.is_empty() {
                    Some(format!("{}:\n- message: {}", err_type, msg))
                } else {
                    Some(format!("{}:\n{}", err_type, detail_msgs.join("\n")))
                }
            });

        let formatted_msg = parsed_err.unwrap_or_else(|| body.clone());
        let error_msg = format!("API Error (status {}): {}", status, formatted_msg);

        match status {
            StatusCode::UNAUTHORIZED => AppError::AuthenticationFailed(error_msg),
            StatusCode::FORBIDDEN => AppError::PermissionDenied(error_msg),
            StatusCode::NOT_FOUND => AppError::InvalidConfig(error_msg),
            StatusCode::CONFLICT => AppError::CapacityUnavailable(error_msg),
            _ => AppError::ApiError(error_msg),
        }
    }
}
