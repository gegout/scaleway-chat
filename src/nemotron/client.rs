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

use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use secrecy::{ExposeSecret, SecretString};
use std::time::{Duration, Instant};
use tracing::debug;

use crate::config::Config;
use crate::error::{AppError, Result};
use crate::nemotron::models::{ChatCompletionRequest, Message};

pub struct NemotronClient {
    ip_address: String,
    port: u16,
    api_key: SecretString,
    #[allow(dead_code)]
    model: String,
    http_client: Client,
}

impl NemotronClient {
    pub fn new(
        ip_address: String,
        port: u16,
        api_key: SecretString,
        model: String,
        inference_timeout_secs: u64,
    ) -> Self {
        Self {
            ip_address,
            port,
            api_key,
            model,
            http_client: Client::builder()
                .timeout(Duration::from_secs(inference_timeout_secs))
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}:{}/v1", self.ip_address, self.port)
    }

    pub async fn wait_for_ready(&self, timeout_secs: u64, poll_interval_secs: u64) -> Result<()> {
        // Warning output
        println!("Warning: the Nemotron API uses plain HTTP.");
        println!("The Bearer API key and chat content are not encrypted in transit.");

        // Indicatif loading spinner
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::default_spinner()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .template("{spinner:.green} {msg}")
                .unwrap(),
        );
        spinner.enable_steady_tick(Duration::from_millis(100));
        spinner.set_message(format!(
            "Waiting for Nemotron to load on {}:{}...",
            self.ip_address, self.port
        ));

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let interval = Duration::from_secs(poll_interval_secs);
        let url = format!("{}/models", self.endpoint());

        loop {
            if start.elapsed() > timeout {
                spinner.finish_and_clear();
                return Err(AppError::NemotronStartupTimeout);
            }

            let elapsed_mins = start.elapsed().as_secs() / 60;
            let elapsed_secs = start.elapsed().as_secs() % 60;
            let elapsed_str = if elapsed_mins > 0 {
                format!("{}m {}s", elapsed_mins, elapsed_secs)
            } else {
                format!("{}s", elapsed_secs)
            };

            let req_builder = self.http_client.get(&url).header(
                "Authorization",
                format!("Bearer {}", self.api_key.expose_secret()),
            );

            match req_builder.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        spinner.finish_and_clear();
                        println!("Nemotron is ready.");
                        return Ok(());
                    }

                    match status.as_u16() {
                        401 => {
                            spinner.finish_and_clear();
                            return Err(AppError::NemotronAuthenticationFailed);
                        }
                        503 => {
                            spinner.set_message(format!(
                                "Nemotron endpoint reachable; model is still loading\nLast response: HTTP 503\nElapsed: {}",
                                elapsed_str
                            ));
                        }
                        s if (400..500).contains(&s) => {
                            spinner.finish_and_clear();
                            return Err(AppError::ChatRequestFailed(format!(
                                "Configuration or authentication error (HTTP {})",
                                status
                            )));
                        }
                        _ => {
                            if status.is_server_error() {
                                let body = resp.text().await.unwrap_or_default();
                                let body_lower = body.to_lowercase();
                                if body_lower.contains("cuda")
                                    || body_lower.contains("nvidia")
                                    || body_lower.contains("driver")
                                    || body_lower.contains("incompatible")
                                {
                                    spinner.finish_and_clear();
                                    return Err(AppError::GuestRuntimeIncompatible {
                                        gpu_type: String::new(),
                                        reason: body,
                                    });
                                }
                                spinner.set_message(format!(
                                    "Nemotron loading status check failed (HTTP {})\nBody: {}\nElapsed: {}",
                                    status, body, elapsed_str
                                ));
                            } else {
                                spinner.set_message(format!(
                                    "Nemotron loading status check failed (HTTP {})\nElapsed: {}",
                                    status, elapsed_str
                                ));
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("Polling connection error: {}", e);
                    spinner.set_message(format!(
                        "Waiting for Nemotron service to start (connection refused)\nElapsed: {}",
                        elapsed_str
                    ));
                }
            }

            tokio::time::sleep(interval).await;
        }
    }

    pub async fn wait_for_ready_with_progress(
        &self,
        timeout_secs: u64,
        poll_interval_secs: u64,
        progress: &dyn Fn(u32, &str),
    ) -> Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let interval = Duration::from_secs(poll_interval_secs);
        let url = format!("{}/models", self.endpoint());

        loop {
            if start.elapsed() > timeout {
                return Err(AppError::NemotronStartupTimeout);
            }

            let req_builder = self.http_client.get(&url).header(
                "Authorization",
                format!("Bearer {}", self.api_key.expose_secret()),
            );

            match req_builder.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return Ok(());
                    }

                    match status.as_u16() {
                        401 => {
                            return Err(AppError::NemotronAuthenticationFailed);
                        }
                        503 => {
                            progress(
                                80,
                                "Nemotron endpoint reachable; model is still loading (HTTP 503)",
                            );
                        }
                        s if (400..500).contains(&s) => {
                            return Err(AppError::ChatRequestFailed(format!(
                                "Configuration or authentication error (HTTP {})",
                                status
                            )));
                        }
                        _ => {
                            if status.is_server_error() {
                                let body = resp.text().await.unwrap_or_default();
                                let body_lower = body.to_lowercase();
                                if body_lower.contains("cuda")
                                    || body_lower.contains("nvidia")
                                    || body_lower.contains("driver")
                                    || body_lower.contains("incompatible")
                                {
                                    return Err(AppError::GuestRuntimeIncompatible {
                                        gpu_type: String::new(),
                                        reason: body,
                                    });
                                }
                                progress(
                                    80,
                                    &format!(
                                        "Nemotron status check returning HTTP {} (Body: {})",
                                        status, body
                                    ),
                                );
                            } else {
                                progress(
                                    80,
                                    &format!("Nemotron status check returning HTTP {}", status),
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("Polling connection error: {}", e);
                    progress(
                        80,
                        "Waiting for Nemotron service to start (connection refused)",
                    );
                }
            }

            tokio::time::sleep(interval).await;
        }
    }

    pub async fn complete_once(&self, config: &Config, user_prompt: &str) -> Result<String> {
        let messages = vec![
            Message {
                role: "system".to_string(),
                content: config.nemotron.system_prompt.clone(),
            },
            Message {
                role: "user".to_string(),
                content: user_prompt.to_string(),
            },
        ];

        let request_payload = ChatCompletionRequest {
            model: config.nemotron.model.clone(),
            messages,
            max_tokens: config.nemotron.max_tokens,
            temperature: config.nemotron.temperature,
            stream: true,
        };

        let url = format!("{}/chat/completions", self.endpoint());

        let resp = self
            .http_client()
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.api_key.expose_secret()),
            )
            .json(&request_payload)
            .send()
            .await
            .map_err(|e| crate::error::AppError::ChatRequestFailed(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let err_body = resp.text().await.unwrap_or_default();
            return Err(crate::error::AppError::ChatRequestFailed(format!(
                "Chat completion API error {}: {}",
                status, err_body
            )));
        }

        let generating = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut full_response = String::new();
        let mut on_token = |token: &str| {
            full_response.push_str(token);
        };

        crate::nemotron::stream::process_chat_stream(resp, generating, &mut on_token).await?;

        Ok(full_response)
    }

    pub fn http_client(&self) -> &Client {
        &self.http_client
    }

    pub fn api_key(&self) -> &SecretString {
        &self.api_key
    }
}
