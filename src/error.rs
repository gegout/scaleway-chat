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
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Configuration file not found")]
    ConfigNotFound,

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Project not found: {0}")]
    ProjectNotFound(String),

    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(String),

    #[error("Snapshot zone {snapshot_zone} does not match configured zone {expected_zone}")]
    SnapshotWrongZone {
        snapshot_zone: String,
        expected_zone: String,
    },

    #[error("Instance type unavailable: {0}")]
    InstanceTypeUnavailable(String),

    #[error("Capacity unavailable: {0}")]
    CapacityUnavailable(String),

    #[error("Volume creation failed: {0}")]
    VolumeCreationFailed(String),

    #[error("IP allocation failed: {0}")]
    IpAllocationFailed(String),

    #[error("Instance creation failed: {0}")]
    InstanceCreationFailed(String),

    #[error("Instance startup timed out")]
    InstanceStartupTimeout,

    #[error("Nemotron authentication failed (invalid API key)")]
    NemotronAuthenticationFailed,

    #[error("Nemotron startup timed out")]
    NemotronStartupTimeout,

    #[error("Chat request failed: {0}")]
    ChatRequestFailed(String),

    #[error("Malformed SSE event: {0}")]
    MalformedStreamEvent(String),

    #[error("Cleanup incomplete: {0}")]
    CleanupIncomplete(String),

    #[error("Safety violation: {0}")]
    SafetyViolation(String),

    #[error("API Error: {0}")]
    ApiError(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP client error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("JSON processing error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("TOML processing error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("UUID processing error: {0}")]
    Uuid(#[from] uuid::Error),
}

pub type Result<T> = std::result::Result<T, AppError>;
