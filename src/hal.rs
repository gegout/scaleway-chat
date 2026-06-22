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

use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Write};
use tracing::error;

use crate::config::Config;
use crate::error::{AppError, Result};
use crate::nemotron::NemotronClient;
use crate::scaleway::ScalewayClient;
use crate::state::State;
use crate::ProgressReporter;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum HalArguments {
    Array(Vec<String>),
    String(String),
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct HalRequest {
    pub request_id: String,
    pub command: String,
    pub arguments: HalArguments,
}

impl HalRequest {
    pub fn prompt_string(&self) -> String {
        match &self.arguments {
            HalArguments::Array(vec) => vec.join(" "),
            HalArguments::String(s) => s.clone(),
        }
    }

    pub fn is_kill_confirmed(&self) -> bool {
        match &self.arguments {
            HalArguments::Array(vec) => vec.len() == 1 && vec[0].trim() == "KILL",
            HalArguments::String(s) => s.trim() == "KILL",
        }
    }
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "type")]
pub enum HalResponse<'a> {
    #[serde(rename = "progress")]
    Progress {
        request_id: &'a str,
        percent: u32,
        message: &'a str,
        format: &'a str,
    },
    #[serde(rename = "final")]
    Final {
        request_id: &'a str,
        format: &'a str,
        message: &'a str,
        trusted_html: bool,
    },
    #[serde(rename = "error")]
    Error {
        request_id: &'a str,
        reason: &'a str,
        technical_details: &'a str,
        suggested_action: &'a str,
        format: &'a str,
    },
}

pub fn send_response(resp: &HalResponse) {
    if let Ok(json_str) = serde_json::to_string(resp) {
        println!("{}", json_str);
        let _ = io::stdout().flush();
    }
}

struct HalReporter<'a> {
    request_id: &'a str,
}

impl<'a> ProgressReporter for HalReporter<'a> {
    fn report(&self, percent: u32, message: &str) {
        send_response(&HalResponse::Progress {
            request_id: self.request_id,
            percent,
            message,
            format: "html",
        });
    }
}

pub fn escape_html(raw: &str) -> String {
    raw.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

pub async fn run_hal_mode(client: &ScalewayClient, config: &Config) -> Result<()> {
    let stdin = io::stdin();
    let mut handle = stdin.lock();
    let mut line = String::new();

    if handle.read_line(&mut line)? == 0 {
        return Err(AppError::InvalidConfig(
            "Empty stdin in HAL mode".to_string(),
        ));
    }

    let req: HalRequest = match serde_json::from_str(&line) {
        Ok(r) => r,
        Err(e) => {
            let resp = HalResponse::Error {
                request_id: "unknown",
                reason: "Malformed request JSON",
                technical_details: &e.to_string(),
                suggested_action:
                    "Provide a valid JSON line conforming to the HAL subprocess protocol.",
                format: "html",
            };
            send_response(&resp);
            return Err(AppError::InvalidConfig(format!("Malformed request JSON: {}", e)));
        }
    };

    let req_id = req.request_id.clone();

    if req_id.trim().is_empty() {
        let resp = HalResponse::Error {
            request_id: "unknown",
            reason: "Missing request_id",
            technical_details: "The request_id field is empty or absent.",
            suggested_action: "Ensure your JSON request contains a valid 'request_id' UUID string.",
            format: "html",
        };
        send_response(&resp);
        return Err(AppError::InvalidConfig("Missing request_id".to_string()));
    }

    // Process subcommands
    let res = match req.command.as_str() {
        "scaleway" => handle_scaleway(&req_id, &req, client, config).await,
        "scaleway_start" => handle_scaleway_start(&req_id, client, config).await,
        "scaleway_status" => handle_scaleway_status(&req_id, client, config).await,
        "scaleway_kill" => handle_scaleway_kill(&req_id, &req, client, config).await,
        "scaleway_help" => handle_scaleway_help(&req_id),
        other => {
            let resp = HalResponse::Error {
                request_id: &req_id,
                reason: "Unsupported command",
                technical_details: &format!("Command '{}' is not recognized.", other),
                suggested_action: "Use one of the supported commands: scaleway, scaleway_start, scaleway_status, scaleway_kill, scaleway_help.",
                format: "html",
            };
            send_response(&resp);
            return Err(AppError::InvalidConfig(format!("Unsupported command: {}", other)));
        }
    };

    if let Err(err) = res {
        error!("HAL command failed: {}", err);
        let resp = HalResponse::Error {
            request_id: &req_id,
            reason: "Execution error",
            technical_details: &err.to_string(),
            suggested_action: "Please retry or run status/kill to check resource states.",
            format: "html",
        };
        send_response(&resp);
        return Err(err);
    }

    Ok(())
}

async fn handle_scaleway(
    req_id: &str,
    req: &HalRequest,
    client: &ScalewayClient,
    config: &Config,
) -> Result<()> {
    let prompt = req.prompt_string();
    if prompt.trim().is_empty() {
        return Err(AppError::InvalidConfig(
            "Prompt cannot be empty".to_string(),
        ));
    }

    let mut state = match State::load_default()? {
        Some(s) => s,
        None => State::new(
            config.instance.snapshot_id.clone(),
            config.scaleway.zone.clone(),
        ),
    };

    let progress_callback = HalReporter { request_id: req_id };

    // Ensure resources are running and ready
    let ip = client
        .ensure_ready(config, &mut state, &progress_callback)
        .await?;

    // 90%: generating the answer
    progress_callback.report(
        90,
        "Connecting to inference server and generating completions...",
    );

    let nemotron_client = NemotronClient::new(
        ip,
        config.nemotron.port,
        config.nemotron.api_key.clone(),
        config.nemotron.model.clone(),
        config.timeouts.inference_timeout_seconds,
    );

    let answer = nemotron_client.complete_once(config, &prompt).await?;

    let final_message = format!(
        "{}\n\n⚠️ <b>Billing Notice:</b> The GPU Instance remains active and will continue billing. Use <code>/scaleway_kill KILL</code> to terminate it when you are finished.",
        escape_html(&answer)
    );

    send_response(&HalResponse::Final {
        request_id: req_id,
        format: "html",
        message: &final_message,
        trusted_html: true,
    });

    Ok(())
}

async fn handle_scaleway_start(
    req_id: &str,
    client: &ScalewayClient,
    config: &Config,
) -> Result<()> {
    let mut state = match State::load_default()? {
        Some(s) => s,
        None => State::new(
            config.instance.snapshot_id.clone(),
            config.scaleway.zone.clone(),
        ),
    };

    let progress_callback = HalReporter { request_id: req_id };

    let ip = client
        .ensure_ready(config, &mut state, &progress_callback)
        .await?;

    let html_msg = format!(
        "<b>GPU Instance:</b> {} (<code>{}</code>)\n\
         <b>State:</b> Running\n\
         <b>Public IP:</b> {}\n\
         <b>Inference Endpoint:</b> http://{}:{}\n\
         <b>Configured Model:</b> <code>{}</code>\n\n\
         ⚠️ <b>Billing Warning:</b> The GPU Instance is active and generating charges. Run <code>/scaleway_kill KILL</code> to tear it down.",
        config.instance.name,
        state.instance_id.as_deref().unwrap_or("unknown"),
        ip,
        ip,
        config.nemotron.port,
        config.nemotron.model
    );

    send_response(&HalResponse::Final {
        request_id: req_id,
        format: "html",
        message: &html_msg,
        trusted_html: true,
    });

    Ok(())
}

async fn handle_scaleway_status(
    req_id: &str,
    client: &ScalewayClient,
    config: &Config,
) -> Result<()> {
    let state = match State::load_default()? {
        Some(s) => s,
        None => State::new(
            config.instance.snapshot_id.clone(),
            config.scaleway.zone.clone(),
        ),
    };

    let html_status = client.get_status(config, &state).await?;

    send_response(&HalResponse::Final {
        request_id: req_id,
        format: "html",
        message: &html_status,
        trusted_html: true,
    });

    Ok(())
}

async fn handle_scaleway_kill(
    req_id: &str,
    req: &HalRequest,
    client: &ScalewayClient,
    config: &Config,
) -> Result<()> {
    if !req.is_kill_confirmed() {
        let error_msg = "Aborted. To terminate all resources, you must pass exactly the KILL argument: <code>/scaleway_kill KILL</code>";
        send_response(&HalResponse::Final {
            request_id: req_id,
            format: "html",
            message: error_msg,
            trusted_html: true,
        });
        return Ok(());
    }

    let state_opt = State::load_default()?;
    let state = match state_opt {
        Some(s) => s,
        None => {
            send_response(&HalResponse::Final {
                request_id: req_id,
                format: "html",
                message: "No active state file found. Nothing to clean up.",
                trusted_html: true,
            });
            return Ok(());
        }
    };

    // Run clean up
    client.perform_cleanup(config, state).await?;

    send_response(&HalResponse::Final {
        request_id: req_id,
        format: "html",
        message: "Teardown complete. All Scaleway GPU resources (Instance, volume, and flexible IP) have been stopped and deleted.",
        trusted_html: true,
    });

    Ok(())
}

fn handle_scaleway_help(req_id: &str) -> Result<()> {
    let help_msg = "<b>Available Scaleway GPU Commands:</b>\n\n\
                    • <code>/scaleway &lt;prompt&gt;</code>: Provision/resume GPU and ask a query.\n\
                    • <code>/scaleway_start</code>: Provision/resume GPU resources and wait for readiness.\n\
                    • <code>/scaleway_status</code>: Check status of GPU instance, IP, and model loading.\n\
                    • <code>/scaleway_kill KILL</code>: Stop and delete all resources (IP, Volume, Instance).\n\
                    • <code>/scaleway_help</code>: Show this help message.";

    send_response(&HalResponse::Final {
        request_id: req_id,
        format: "html",
        message: help_msg,
        trusted_html: true,
    });

    Ok(())
}
