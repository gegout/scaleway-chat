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

use crate::error::{AppError, Result};
use crate::scaleway::client::ScalewayClient;
use crate::scaleway::models::Snapshot;

impl ScalewayClient {
    pub async fn get_snapshot(&self, snapshot_id: &str) -> Result<Snapshot> {
        let path = format!("/block/v1/zones/{}/snapshots/{}", self.zone, snapshot_id);

        match self
            .request::<Snapshot, _>(Method::GET, &path, |req| req)
            .await
        {
            Ok(snapshot) => {
                // Validate project alignment
                if snapshot.project_id != self.project_id {
                    return Err(AppError::PermissionDenied(format!(
                        "Snapshot belongs to project {} but configured project is {}",
                        snapshot.project_id, self.project_id
                    )));
                }

                // Validate status is ready or available
                if snapshot.status != "ready" && snapshot.status != "available" {
                    return Err(AppError::SnapshotNotFound(format!(
                        "Snapshot {} is not ready or available (current status: {})",
                        snapshot_id, snapshot.status
                    )));
                }

                // Validate zone alignment
                if snapshot.zone != self.zone {
                    return Err(AppError::SnapshotWrongZone {
                        snapshot_zone: snapshot.zone.clone(),
                        expected_zone: self.zone.clone(),
                    });
                }

                Ok(snapshot)
            }
            Err(e) => {
                // Interpret 404/403 as snapshot not found or not in correct zone
                match e {
                    AppError::InvalidConfig(_) => Err(AppError::SnapshotNotFound(format!(
                        "Snapshot {} not found in zone {}. Ensure snapshot ID and zone are correct.",
                        snapshot_id, self.zone
                    ))),
                    _ => Err(e),
                }
            }
        }
    }
}
