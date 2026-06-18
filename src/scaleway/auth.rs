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

impl ScalewayClient {
    pub async fn validate_auth_and_project(&self) -> Result<()> {
        info!("[Scaleway] Authenticating and validating project access...");

        // Query server list for this project to check IAM credentials and Project validity
        let path = format!("/instance/v1/zones/{}/servers", self.zone);

        let _res: serde_json::Value = match self
            .request(Method::GET, &path, |req| {
                req.query(&[("project", &self.project_id)])
            })
            .await
        {
            Ok(v) => v,
            Err(e) => {
                // Check for specific error mapping
                return match e {
                    AppError::AuthenticationFailed(msg) => Err(AppError::AuthenticationFailed(
                        format!("Invalid API Key credentials: {}", msg),
                    )),
                    AppError::PermissionDenied(msg) => Err(AppError::PermissionDenied(format!(
                        "Missing IAM permissions for zone {} or project {}: {}",
                        self.zone, self.project_id, msg
                    ))),
                    AppError::InvalidConfig(msg) => Err(AppError::ProjectNotFound(format!(
                        "Project ID {} not found or invalid: {}",
                        self.project_id, msg
                    ))),
                    _ => Err(e),
                };
            }
        };

        // If it compiled to JSON and completed, the Project ID and credentials are valid
        info!("[Scaleway] Authentication successful. Project verified.");
        Ok(())
    }
}
