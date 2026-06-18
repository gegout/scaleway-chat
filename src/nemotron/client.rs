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

use crate::error::{AppError, Result};

pub struct NemotronClient {
    ip_address: String,
    port: u16,
    api_key: SecretString,
    #[allow(dead_code)]
    model: String,
    http_client: Client,
}

impl NemotronClient {
    pub fn new(ip_address: String, port: u16, api_key: SecretString, model: String) -> Self {
        Self {
            ip_address,
            port,
            api_key,
            model,
            http_client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}:{}/v1", self.ip_address, self.port)
    }

    pub async fn wait_for_ready(&self, timeout_secs: u64, poll_interval_secs: u64) -> Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let interval = Duration::from_secs(poll_interval_secs);
        let url = format!("{}/models", self.endpoint());

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
                            spinner.set_message(format!(
                                "Nemotron loading status check failed (HTTP {})\nElapsed: {}",
                                status, elapsed_str
                            ));
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

    pub fn http_client(&self) -> &Client {
        &self.http_client
    }

    pub fn api_key(&self) -> &SecretString {
        &self.api_key
    }
}
