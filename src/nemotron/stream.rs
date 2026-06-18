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

use crate::error::{AppError, Result};
use crate::nemotron::models::ChatCompletionChunk;
use serde_json;

pub struct SseParser {
    buffer: Vec<u8>,
}

impl Default for SseParser {
    fn default() -> Self {
        Self::new()
    }
}

impl SseParser {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn feed(&mut self, chunk: &[u8]) -> Vec<String> {
        self.buffer.extend_from_slice(chunk);
        let mut lines = Vec::new();
        let mut start = 0;

        while let Some(pos) = self.buffer[start..].iter().position(|&b| b == b'\n') {
            let end = start + pos;
            let line_bytes = &self.buffer[start..end];
            let len = line_bytes.len();
            let line_bytes_trimmed = if len > 0 && line_bytes[len - 1] == b'\r' {
                &line_bytes[..len - 1]
            } else {
                line_bytes
            };

            if let Ok(line) = std::str::from_utf8(line_bytes_trimmed) {
                lines.push(line.to_string());
            }
            start = end + 1;
        }

        if start > 0 {
            self.buffer.drain(0..start);
        }

        lines
    }

    pub fn finish(self) -> Option<String> {
        if self.buffer.is_empty() {
            return None;
        }
        let len = self.buffer.len();
        let trimmed = if len > 0 && self.buffer[len - 1] == b'\r' {
            &self.buffer[..len - 1]
        } else {
            &self.buffer[..]
        };
        std::str::from_utf8(trimmed).ok().map(|s| s.to_string())
    }
}

pub async fn process_chat_stream(
    response: reqwest::Response,
    generating: std::sync::Arc<std::sync::atomic::AtomicBool>,
    mut on_token: impl FnMut(&str),
) -> Result<String> {
    use futures_util::StreamExt;
    use std::sync::atomic::Ordering;

    generating.store(true, Ordering::SeqCst);

    struct GeneratingGuard(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for GeneratingGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
    let _guard = GeneratingGuard(generating.clone());

    let mut parser = SseParser::new();
    let mut stream = response.bytes_stream();
    let mut accumulated = String::new();
    let mut received_any = false;
    let mut done = false;

    loop {
        if done {
            break;
        }

        let chunk_result = tokio::select! {
            next_chunk = stream.next() => {
                match next_chunk {
                    Some(c) => c,
                    None => break,
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n[Signal] Generation cancelled by user.");
                break;
            }
        };

        let chunk = chunk_result.map_err(|e| AppError::ChatRequestFailed(e.to_string()))?;
        let lines = parser.feed(&chunk);

        for line in lines {
            if let Some(token) = parse_sse_line(&line)? {
                if token == "[DONE]" {
                    done = true;
                    break;
                }
                on_token(&token);
                accumulated.push_str(&token);
                received_any = true;
            }
        }
    }

    if !done {
        if let Some(line) = parser.finish() {
            if let Some(token) = parse_sse_line(&line)? {
                if token != "[DONE]" {
                    on_token(&token);
                    accumulated.push_str(&token);
                    received_any = true;
                }
            }
        }
    }

    if !received_any {
        return Err(AppError::ChatRequestFailed(
            "No content received from stream".to_string(),
        ));
    }

    Ok(accumulated)
}

pub fn parse_sse_line(line: &str) -> Result<Option<String>> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
    }
    if line.starts_with(':') {
        return Ok(None);
    }
    if let Some(stripped) = line.strip_prefix("data:") {
        let data_str = stripped.trim();
        if data_str == "[DONE]" {
            return Ok(Some("[DONE]".to_string()));
        }

        let chunk: ChatCompletionChunk = match serde_json::from_str(data_str) {
            Ok(c) => c,
            Err(e) => {
                // Check if it represents a structured API error
                if let Ok(error_val) = serde_json::from_str::<serde_json::Value>(data_str) {
                    if let Some(err_msg) = error_val
                        .get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(|m| m.as_str())
                    {
                        return Err(AppError::ChatRequestFailed(err_msg.to_string()));
                    }
                }
                tracing::debug!(
                    "Skipping invalid or malformed SSE data block: {}. Error: {}",
                    data_str,
                    e
                );
                return Ok(None);
            }
        };

        if let Some(choice) = chunk.choices.first() {
            if let Some(ref delta) = choice.delta {
                if let Some(ref content) = delta.content {
                    return Ok(Some(content.clone()));
                }
            }
            if let Some(ref message) = choice.message {
                if let Some(ref content) = message.content {
                    return Ok(Some(content.clone()));
                }
            }
        }
    }
    Ok(None)
}
