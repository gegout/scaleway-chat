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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Snapshot Models
#[derive(Debug, Deserialize, Clone)]
pub struct Snapshot {
    pub id: String,
    pub name: String,
    pub status: String,
    pub size: u64,
    pub project_id: String,
    pub zone: String,
}

// Volume Models
#[derive(Debug, Serialize)]
pub struct CreateVolumeRequest {
    pub name: String,
    pub project_id: String,
    pub perf_iops: u32,
    pub from_snapshot: Option<SnapshotSource>,
    pub tags: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotSource {
    pub snapshot_id: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Volume {
    pub id: String,
    pub name: String,
    pub status: String,
    pub project_id: String,
    pub zone: String,
    pub snapshot_id: Option<String>,
}

// IP Models
#[derive(Debug, Serialize)]
pub struct CreateIpRequest {
    pub project: String,
    #[serde(rename = "type")]
    pub ip_type: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct InstanceIp {
    pub id: String,
    pub address: String,
    pub project: String,
    pub zone: String,
    pub server: Option<ServerRef>,
}

pub type Ip = InstanceIp;

#[derive(Debug, Deserialize, Clone)]
pub struct ServerRef {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct IpResponse {
    pub ip: InstanceIp,
}

#[derive(Debug, Serialize)]
pub struct AttachIpRequest {
    pub server: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UpdateIpRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reverse: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct IpAttachmentState {
    pub ip: InstanceIp,
}

// Product/Server Type Models
#[derive(Debug, Deserialize, Clone)]
pub struct ServerTypesResponse {
    pub servers: HashMap<String, ServerTypeDetails>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerTypeDetails {
    pub volumes_constraint: Option<VolumesConstraint>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VolumesConstraint {
    pub min_size: u64,
    pub max_size: u64,
}

// Instance/Server Models
#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstanceVolumeType {
    SbsVolume,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct SnapshotBootVolume {
    pub base_snapshot: uuid::Uuid,
    pub name: String,
    pub volume_type: InstanceVolumeType,
    pub boot: bool,
}

#[derive(Debug, Serialize)]
pub struct CreateServerRequest {
    pub name: String,
    pub project: uuid::Uuid,
    pub commercial_type: String,
    pub volumes: HashMap<String, SnapshotBootVolume>,
    pub tags: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerVolumeDetail {
    pub id: String,
    pub name: Option<String>,
    pub volume_type: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub id: String,
    pub name: String,
    pub state: String,
    pub public_ip: Option<ServerPublicIp>,
    #[serde(default)]
    pub volumes: HashMap<String, ServerVolumeDetail>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerPublicIp {
    pub id: String,
    pub address: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerResponse {
    pub server: Server,
}

#[derive(Debug, Serialize)]
pub struct ServerActionRequest {
    pub action: String,
}
