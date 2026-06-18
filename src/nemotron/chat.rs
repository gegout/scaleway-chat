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

use secrecy::ExposeSecret;
use std::io::{self, Write};

use crate::config::Config;
use crate::error::Result;
use crate::nemotron::client::NemotronClient;
use crate::nemotron::models::{ChatCompletionRequest, Message};
use crate::nemotron::stream::process_chat_stream;

pub enum ChatAction {
    Exit,
    Kill,
}

pub async fn start_chat(
    client: &NemotronClient,
    config: &Config,
    active_state_info: &str,
) -> Result<ChatAction> {
    let generating = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let generating_clone = generating.clone();

    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_ok()
                && !generating_clone.load(std::sync::atomic::Ordering::SeqCst)
            {
                println!("\n\nWarning: The Scaleway GPU Instance is still running and may continue generating charges.");
                println!("Use /kill to delete it.");
                std::process::exit(130);
            }
        }
    });
    println!("\nConnected to {}", config.nemotron.model);
    println!("Endpoint: {}", client.endpoint());
    println!("Commands: /clear, /status, /kill, /exit\n");

    let mut messages = vec![Message {
        role: "system".to_string(),
        content: config.nemotron.system_prompt.clone(),
    }];

    loop {
        print!("You: ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed_input = input.trim();

        if trimmed_input.is_empty() {
            continue;
        }

        if trimmed_input.starts_with('/') {
            match trimmed_input {
                "/clear" => {
                    messages.truncate(1);
                    println!("History cleared. Reset to system prompt.");
                    println!();
                    continue;
                }
                "/status" => {
                    println!("\n--- Status Info ---");
                    println!("{}", active_state_info);
                    println!("Model: {}", config.nemotron.model);
                    println!("Endpoint: {}", client.endpoint());
                    println!("Max Tokens Limit: {}", config.nemotron.max_tokens);
                    println!("Temperature: {}", config.nemotron.temperature);
                    println!("-------------------\n");
                    continue;
                }
                "/exit" => {
                    println!("\nWarning: The Scaleway GPU Instance is still running and may continue generating charges.");
                    println!("Use /kill to delete it.");
                    return Ok(ChatAction::Exit);
                }
                "/kill" => {
                    return Ok(ChatAction::Kill);
                }
                _ => {
                    println!("Unknown command. Available commands: /clear, /status, /kill, /exit");
                    continue;
                }
            }
        }

        // Add user message to history
        messages.push(Message {
            role: "user".to_string(),
            content: trimmed_input.to_string(),
        });

        let request_payload = ChatCompletionRequest {
            model: config.nemotron.model.clone(),
            messages: messages.clone(),
            max_tokens: config.nemotron.max_tokens,
            temperature: config.nemotron.temperature,
            stream: true,
        };

        println!("\nNemotron:");
        io::stdout().flush()?;

        let url = format!("{}/chat/completions", client.endpoint());

        let resp = match client
            .http_client()
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", client.api_key().expose_secret()),
            )
            .json(&request_payload)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() {
                    let err_body = resp.text().await.unwrap_or_default();
                    println!("\nChat request failed: {}", err_body);
                    // Remove last user message from history to prevent corrupting next query
                    messages.pop();
                    continue;
                }
                resp
            }
            Err(e) => {
                println!("\nNetwork error sending chat prompt: {}", e);
                messages.pop();
                continue;
            }
        };

        // Stream parsing
        let mut on_token = |token: &str| {
            print!("{}", token);
            let _ = io::stdout().flush();
        };

        match process_chat_stream(resp, generating.clone(), &mut on_token).await {
            Ok(full_response) => {
                println!("\n");
                // Add assistant response to history
                messages.push(Message {
                    role: "assistant".to_string(),
                    content: full_response,
                });
            }
            Err(e) => {
                println!("\nError parsing response stream: {}", e);
                messages.pop();
            }
        }
    }
}
