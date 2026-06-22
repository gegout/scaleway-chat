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

#![allow(unused_imports, dead_code)]
use secrecy::SecretString;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// Inject paths from binary source as needed or test helper structures
use scaleway_chat::config::Config;
use scaleway_chat::error::{AppError, Result};
use scaleway_chat::nemotron::client::NemotronClient;
use scaleway_chat::nemotron::stream::{process_chat_stream, SseParser};
use scaleway_chat::scaleway::models::{
    CreateServerRequest, InstanceVolumeType, Ip, IpResponse, Server, ServerResponse, Snapshot,
    SnapshotBootVolume, Volume,
};
use scaleway_chat::scaleway::ScalewayClient;
use scaleway_chat::state::{ProvisioningPhase, State};

// 1. SSE PARSER UNIT TESTS
#[test]
fn test_sse_parser_split_chunks() {
    let mut parser = SseParser::new();

    let chunk1 =
        b"data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\ndata: {\"choices\":[";
    let chunk2 = b"{\"delta\":{\"content\":\" World\"}}]}\n\ndata: [DONE]\n";

    let lines1: Vec<String> = parser
        .feed(chunk1)
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .collect();
    assert_eq!(lines1.len(), 1);
    assert_eq!(
        lines1[0],
        "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}"
    );

    let lines2: Vec<String> = parser
        .feed(chunk2)
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .collect();
    assert_eq!(lines2.len(), 2);
    assert_eq!(
        lines2[0],
        "data: {\"choices\":[{\"delta\":{\"content\":\" World\"}}]}"
    );
    assert_eq!(lines2[1], "data: [DONE]");
}

#[test]
fn test_sse_parser_crlf_events() {
    let mut parser = SseParser::new();
    let chunk = b"data: {\"choices\":[{\"delta\":{\"content\":\"Line\"}}]}\r\n\r\ndata: [DONE]\r\n";
    let lines: Vec<String> = parser
        .feed(chunk)
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(
        lines[0],
        "data: {\"choices\":[{\"delta\":{\"content\":\"Line\"}}]}"
    );
    assert_eq!(lines[1], "data: [DONE]");
}

// 2. CONFIGURATION VALIDATION TESTS
#[test]
fn test_invalid_uuid_validation() {
    let toml_str = r#"
[scaleway]
access_key = "SCWXXXXXXXXXXXXXXXXX"
secret_key = "invalid-secret"
project_id = "invalid-uuid"
organization_id = "00000000-0000-0000-0000-000000000000"
zone = "fr-par-2"

[instance]
name = "nemotron-l40s"
instance_type = "L40S-1-48G"
snapshot_id = "00000000-0000-0000-0000-000000000000"
public_ip = "new"

[nemotron]
port = 8330
api_key = "REPLACE_WITH_NEMOTRON_API_KEY"
model = "nemotron-3-nano-30b-a3b-q4-k-m"
max_tokens = 4096
temperature = 0.7
system_prompt = "You are a helpful assistant."

[timeouts]
instance_creation_seconds = 1200
instance_poll_interval_seconds = 10
nemotron_startup_seconds = 1200
nemotron_poll_interval_seconds = 10
cleanup_timeout_seconds = 300
cleanup_poll_interval_seconds = 5

[logging]
verbose = true
"#;

    let config_res: std::result::Result<Config, _> = toml::from_str(toml_str);
    assert!(config_res.is_ok());
    let config = config_res.unwrap();
    // Validate must fail on project_id being invalid UUID
    let validation_res = config.validate();
    assert!(validation_res.is_err());
}

// 3. CLEANUP SAFETY INVARIANTS TEST
#[tokio::test]
async fn test_cleanup_safety_invariants() {
    let config = Config {
        scaleway: scaleway_chat::config::ScalewayConfig {
            access_key: "SCW123".to_string(),
            secret_key: SecretString::new("secret".to_string()),
            project_id: "00000000-0000-0000-0000-000000000000".to_string(),
            organization_id: "00000000-0000-0000-0000-000000000000".to_string(),
            zone: "fr-par-2".to_string(),
        },
        instance: scaleway_chat::config::InstanceConfig {
            name: "nemotron-l40s".to_string(),
            instance_type: "L40S-1-48G".to_string(),
            snapshot_id: "1b552e81-401d-4c15-b0b2-3c89e2d46c28".to_string(),
            public_ip: "new".to_string(),
            gpu_types: None,
        },
        nemotron: scaleway_chat::config::NemotronConfig {
            port: 8330,
            api_key: SecretString::new("key".to_string()),
            model: "model".to_string(),
            max_tokens: 10,
            temperature: 0.7,
            system_prompt: "prompt".to_string(),
        },
        timeouts: scaleway_chat::config::TimeoutsConfig {
            instance_creation_seconds: 10,
            instance_poll_interval_seconds: 1,
            nemotron_startup_seconds: 10,
            nemotron_poll_interval_seconds: 1,
            cleanup_timeout_seconds: 5,
            cleanup_poll_interval_seconds: 1,
            inference_timeout_seconds: 300,
        },
        logging: scaleway_chat::config::LoggingConfig { verbose: false },
    };

    let client = ScalewayClient::new(&config);

    // Call delete volume, instance, IP with snapshot_id and verify it fails with SafetyViolation
    let snapshot_id = &config.instance.snapshot_id;

    let delete_vol_res = client.delete_volume(snapshot_id, snapshot_id).await;
    assert!(matches!(delete_vol_res, Err(AppError::SafetyViolation(_))));

    let delete_inst_res = client.delete_instance(snapshot_id, snapshot_id).await;
    assert!(matches!(delete_inst_res, Err(AppError::SafetyViolation(_))));

    let delete_ip_res = client.delete_public_ip(snapshot_id, snapshot_id).await;
    assert!(matches!(delete_ip_res, Err(AppError::SafetyViolation(_))));
}

// 4. MOCK INTEGRATION LIFE CYCLE TEST
#[tokio::test]
async fn test_mock_provisioning_and_cleanup() {
    let mock_server = MockServer::start().await;

    let config = Config {
        scaleway: scaleway_chat::config::ScalewayConfig {
            access_key: "SCW123".to_string(),
            secret_key: SecretString::new("secret".to_string()),
            project_id: "00000000-0000-0000-0000-000000000000".to_string(),
            organization_id: "00000000-0000-0000-0000-000000000000".to_string(),
            zone: "fr-par-2".to_string(),
        },
        instance: scaleway_chat::config::InstanceConfig {
            name: "nemotron-l40s".to_string(),
            instance_type: "L40S-1-48G".to_string(),
            snapshot_id: "1b552e81-401d-4c15-b0b2-3c89e2d46c28".to_string(),
            public_ip: "new".to_string(),
            gpu_types: None,
        },
        nemotron: scaleway_chat::config::NemotronConfig {
            port: 8330,
            api_key: SecretString::new("key".to_string()),
            model: "model".to_string(),
            max_tokens: 10,
            temperature: 0.7,
            system_prompt: "prompt".to_string(),
        },
        timeouts: scaleway_chat::config::TimeoutsConfig {
            instance_creation_seconds: 10,
            instance_poll_interval_seconds: 1,
            nemotron_startup_seconds: 10,
            nemotron_poll_interval_seconds: 1,
            cleanup_timeout_seconds: 5,
            cleanup_poll_interval_seconds: 1,
            inference_timeout_seconds: 300,
        },
        logging: scaleway_chat::config::LoggingConfig { verbose: false },
    };

    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    // 1. Mock Authentication validate project access
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .and(query_param(
            "project",
            "00000000-0000-0000-0000-000000000000",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": []
        })))
        .mount(&mock_server)
        .await;

    // 2. Mock Snapshot Validation
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "name": "snapshot-1",
            "status": "ready",
            "size": 100000000000u64,
            "project_id": "00000000-0000-0000-0000-000000000000",
            "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    // 3. Mock Server Types Availability
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": {
                "L40S-1-48G": {
                    "name": "L40S-1-48G",
                    "volumes_constraint": {
                        "min_size": 1000000000,
                        "max_size": 1000000000000u64
                    }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // Verify calls work cleanly
    assert!(client.validate_auth_and_project().await.is_ok());

    let snapshot = client
        .get_snapshot("1b552e81-401d-4c15-b0b2-3c89e2d46c28")
        .await;
    assert!(snapshot.is_ok());
    assert_eq!(snapshot.unwrap().status, "ready");

    assert!(client
        .validate_instance_type_available("L40S-1-48G")
        .await
        .is_ok());
}

// 5. ADDITIONAL COMPREHENSIVE UNIT & INTEGRATION TESTS

#[test]
fn test_config_validation_failures() {
    let base_toml = r#"
[scaleway]
access_key = "SCWXXXXXXXXXXXXXXXXX"
secret_key = "secret"
project_id = "00000000-0000-0000-0000-000000000000"
organization_id = "00000000-0000-0000-0000-000000000000"
zone = "fr-par-2"

[instance]
name = "nemotron-l40s"
instance_type = "L40S-1-48G"
snapshot_id = "00000000-0000-0000-0000-000000000000"
public_ip = "new"

[nemotron]
port = 8330
api_key = "REPLACE_WITH_NEMOTRON_API_KEY"
model = "nemotron-3-nano-30b-a3b-q4-k-m"
max_tokens = 4096
temperature = 0.7
system_prompt = "You are a helpful assistant."

[timeouts]
instance_creation_seconds = 1200
instance_poll_interval_seconds = 10
nemotron_startup_seconds = 1200
nemotron_poll_interval_seconds = 10
cleanup_timeout_seconds = 300
cleanup_poll_interval_seconds = 5

[logging]
verbose = true
"#;

    let parse_and_validate = |target: &str, replacement: &str| -> std::result::Result<(), String> {
        let toml_str = base_toml.replace(target, replacement);
        let config: Config =
            toml::from_str(&toml_str).map_err(|e| format!("Toml parse error: {}", e))?;
        config.validate().map_err(|e| e.to_string())
    };

    // 1. Access Key empty
    assert!(
        parse_and_validate("access_key = \"SCWXXXXXXXXXXXXXXXXX\"", "access_key = \"\"").is_err()
    );
    // 2. Secret Key empty
    assert!(parse_and_validate("secret_key = \"secret\"", "secret_key = \"\"").is_err());
    // 3. Zone empty
    assert!(parse_and_validate("zone = \"fr-par-2\"", "zone = \"\"").is_err());
    // 4. Invalid Zone formats
    assert!(parse_and_validate("zone = \"fr-par-2\"", "zone = \"invalid-zone-fmt\"").is_err());
    assert!(parse_and_validate("zone = \"fr-par-2\"", "zone = \"fr-par-invalid\"").is_err());
    // 5. Instance Name empty
    assert!(parse_and_validate("name = \"nemotron-l40s\"", "name = \"\"").is_err());
    // 6. Instance Type empty
    assert!(parse_and_validate("instance_type = \"L40S-1-48G\"", "instance_type = \"\"").is_err());
    // 7. Nemotron API Key empty
    assert!(parse_and_validate(
        "api_key = \"REPLACE_WITH_NEMOTRON_API_KEY\"",
        "api_key = \"\""
    )
    .is_err());
    // 8. Nemotron Model empty
    assert!(
        parse_and_validate("model = \"nemotron-3-nano-30b-a3b-q4-k-m\"", "model = \"\"").is_err()
    );
    // 9. Invalid project_id UUID
    assert!(parse_and_validate(
        "project_id = \"00000000-0000-0000-0000-000000000000\"",
        "project_id = \"not-a-uuid\""
    )
    .is_err());
    // 10. Invalid organization_id UUID
    assert!(parse_and_validate(
        "organization_id = \"00000000-0000-0000-0000-000000000000\"",
        "organization_id = \"not-a-uuid\""
    )
    .is_err());
    // 11. Invalid snapshot_id UUID
    assert!(parse_and_validate(
        "snapshot_id = \"00000000-0000-0000-0000-000000000000\"",
        "snapshot_id = \"not-a-uuid\""
    )
    .is_err());
    // 12. Nemotron port is 0
    assert!(parse_and_validate("port = 8330", "port = 0").is_err());
    // 13. Max tokens is 0
    assert!(parse_and_validate("max_tokens = 4096", "max_tokens = 0").is_err());
    // 14. Temperature out of bounds
    assert!(parse_and_validate("temperature = 0.7", "temperature = -0.5").is_err());
    assert!(parse_and_validate("temperature = 0.7", "temperature = 2.5").is_err());
    // 15. Timeouts are 0
    assert!(parse_and_validate(
        "instance_creation_seconds = 1200",
        "instance_creation_seconds = 0"
    )
    .is_err());
    assert!(parse_and_validate(
        "instance_poll_interval_seconds = 10",
        "instance_poll_interval_seconds = 0"
    )
    .is_err());
    assert!(parse_and_validate(
        "nemotron_startup_seconds = 1200",
        "nemotron_startup_seconds = 0"
    )
    .is_err());
    assert!(parse_and_validate(
        "nemotron_poll_interval_seconds = 10",
        "nemotron_poll_interval_seconds = 0"
    )
    .is_err());
    // 16. Unsupported Public IP value
    assert!(parse_and_validate("public_ip = \"new\"", "public_ip = \"existing\"").is_err());
}

#[test]
fn test_state_persistence_and_permissions() {
    let temp_file = std::env::temp_dir().join(format!("state-{}.toml", uuid::Uuid::new_v4()));

    // 1. Loading non-existent file returns Ok(None)
    let state_opt = State::load_from_path(&temp_file).unwrap();
    assert!(state_opt.is_none());

    // 2. Initialize and save state
    let state = State::new(
        "1b552e81-401d-4c15-b0b2-3c89e2d46c28".to_string(),
        "fr-par-2".to_string(),
    );
    state.save_to_path(&temp_file).unwrap();

    // 3. Verify it was written and has 0600 permissions
    assert!(temp_file.exists());
    let metadata = std::fs::metadata(&temp_file).unwrap();
    use std::os::unix::fs::PermissionsExt;
    let mode = metadata.permissions().mode();
    // Verify only owner has read/write (0600), ie (mode & 0o777) == 0o600
    assert_eq!(mode & 0o777, 0o600);

    // 4. Load state and verify fields
    let loaded = State::load_from_path(&temp_file).unwrap().unwrap();
    assert_eq!(loaded.version, 4);
    assert_eq!(loaded.creation_mode, Some("snapshot_direct".to_string()));
    assert_eq!(loaded.snapshot_id, "1b552e81-401d-4c15-b0b2-3c89e2d46c28");
    assert_eq!(loaded.zone, "fr-par-2");
    assert_eq!(loaded.instance_id, None);

    // 5. Test loading invalid TOML
    std::fs::write(&temp_file, "invalid-toml-content=bar=baz").unwrap();
    let load_err = State::load_from_path(&temp_file);
    assert!(load_err.is_err());

    // Clean up
    let _ = std::fs::remove_file(temp_file);
}

#[test]
fn test_sse_parser_comments_and_malformed() {
    // 1. Empty lines should return None
    assert!(matches!(
        scaleway_chat::nemotron::stream::parse_sse_line("   "),
        Ok(None)
    ));

    // 2. Comments (starting with ':') should return None
    assert!(matches!(
        scaleway_chat::nemotron::stream::parse_sse_line(": ping"),
        Ok(None)
    ));
    assert!(matches!(
        scaleway_chat::nemotron::stream::parse_sse_line(":\n"),
        Ok(None)
    ));

    // 3. Malformed JSON SSE payloads should be skipped (return Ok(None))
    assert!(matches!(
        scaleway_chat::nemotron::stream::parse_sse_line("data: {invalid-json}"),
        Ok(None)
    ));

    // 4. Structured API errors from Nemotron
    let err_payload = "data: {\"error\": {\"message\": \"Model rate limit exceeded\"}}";
    let res = scaleway_chat::nemotron::stream::parse_sse_line(err_payload);
    assert!(res.is_err());
    if let Err(AppError::ChatRequestFailed(msg)) = res {
        assert_eq!(msg, "Model rate limit exceeded");
    } else {
        panic!("Expected AppError::ChatRequestFailed containing rate limit message");
    }
}

#[test]
fn test_client_map_api_error() {
    let config = Config {
        scaleway: scaleway_chat::config::ScalewayConfig {
            access_key: "SCW123".to_string(),
            secret_key: SecretString::new("secret".to_string()),
            project_id: "00000000-0000-0000-0000-000000000000".to_string(),
            organization_id: "00000000-0000-0000-0000-000000000000".to_string(),
            zone: "fr-par-2".to_string(),
        },
        instance: scaleway_chat::config::InstanceConfig {
            name: "nemotron-l40s".to_string(),
            instance_type: "L40S-1-48G".to_string(),
            snapshot_id: "1b552e81-401d-4c15-b0b2-3c89e2d46c28".to_string(),
            public_ip: "new".to_string(),
            gpu_types: None,
        },
        nemotron: scaleway_chat::config::NemotronConfig {
            port: 8330,
            api_key: SecretString::new("key".to_string()),
            model: "model".to_string(),
            max_tokens: 10,
            temperature: 0.7,
            system_prompt: "prompt".to_string(),
        },
        timeouts: scaleway_chat::config::TimeoutsConfig {
            instance_creation_seconds: 10,
            instance_poll_interval_seconds: 1,
            nemotron_startup_seconds: 10,
            nemotron_poll_interval_seconds: 1,
            cleanup_timeout_seconds: 5,
            cleanup_poll_interval_seconds: 1,
            inference_timeout_seconds: 300,
        },
        logging: scaleway_chat::config::LoggingConfig { verbose: false },
    };

    let client = ScalewayClient::new(&config);

    let err = client.map_api_error(
        reqwest::StatusCode::UNAUTHORIZED,
        "invalid token".to_string(),
    );
    assert!(matches!(err, AppError::AuthenticationFailed(_)));

    let err = client.map_api_error(reqwest::StatusCode::FORBIDDEN, "forbidden".to_string());
    assert!(matches!(err, AppError::PermissionDenied(_)));

    let err = client.map_api_error(reqwest::StatusCode::NOT_FOUND, "not found".to_string());
    assert!(matches!(err, AppError::InvalidConfig(_)));

    let err = client.map_api_error(reqwest::StatusCode::CONFLICT, "out of capacity".to_string());
    assert!(matches!(err, AppError::CapacityUnavailable(_)));

    let err = client.map_api_error(
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "server error".to_string(),
    );
    assert!(matches!(err, AppError::ApiError(_)));
}

#[tokio::test]
async fn test_client_retry_on_transient_error() {
    let mock_server = MockServer::start().await;

    let config = Config {
        scaleway: scaleway_chat::config::ScalewayConfig {
            access_key: "SCW123".to_string(),
            secret_key: SecretString::new("secret".to_string()),
            project_id: "00000000-0000-0000-0000-000000000000".to_string(),
            organization_id: "00000000-0000-0000-0000-000000000000".to_string(),
            zone: "fr-par-2".to_string(),
        },
        instance: scaleway_chat::config::InstanceConfig {
            name: "nemotron-l40s".to_string(),
            instance_type: "L40S-1-48G".to_string(),
            snapshot_id: "1b552e81-401d-4c15-b0b2-3c89e2d46c28".to_string(),
            public_ip: "new".to_string(),
            gpu_types: None,
        },
        nemotron: scaleway_chat::config::NemotronConfig {
            port: 8330,
            api_key: SecretString::new("key".to_string()),
            model: "model".to_string(),
            max_tokens: 10,
            temperature: 0.7,
            system_prompt: "prompt".to_string(),
        },
        timeouts: scaleway_chat::config::TimeoutsConfig {
            instance_creation_seconds: 10,
            instance_poll_interval_seconds: 1,
            nemotron_startup_seconds: 10,
            nemotron_poll_interval_seconds: 1,
            cleanup_timeout_seconds: 5,
            cleanup_poll_interval_seconds: 1,
            inference_timeout_seconds: 300,
        },
        logging: scaleway_chat::config::LoggingConfig { verbose: false },
    };

    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    // Mock transient failures:
    // 1st request -> 429 Too Many Requests with Retry-After: 1
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "1"))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // 2nd request -> 500 Internal Server Error
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // 3rd request -> 200 OK
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": []
        })))
        .mount(&mock_server)
        .await;

    let start = std::time::Instant::now();
    let res = client.validate_auth_and_project().await;
    assert!(res.is_ok());
    // Verification: It should take at least 1 second due to Retry-After: 1
    assert!(start.elapsed() >= std::time::Duration::from_secs(1));
}

#[tokio::test]
#[ignore]
async fn test_real_scaleway_integration() {
    // 1. Load configuration from default path (~/.config/scaleway-chat/config.toml)
    let (config, config_path) = match Config::load_default() {
        Ok(res) => res,
        Err(e) => {
            panic!("Failed to load configuration from default path: {:?}", e);
        }
    };
    println!("Loaded real configuration from {}", config_path.display());

    // 2. Initialize real client (points to https://api.scaleway.com)
    let client = ScalewayClient::new(&config);

    // 3. Validate authentication and project access
    println!("Validating authentication and project access...");
    let auth_res = client.validate_auth_and_project().await;
    assert!(
        auth_res.is_ok(),
        "Authentication/Project validation failed: {:?}",
        auth_res
    );
    println!("Authentication and project access are valid!");

    // 4. Validate configured instance type availability
    println!(
        "Validating instance type '{}' availability...",
        config.instance.instance_type
    );
    let inst_res = client
        .validate_instance_type_available(&config.instance.instance_type)
        .await;
    assert!(
        inst_res.is_ok(),
        "Instance type validation failed: {:?}",
        inst_res
    );
    println!("Instance type is available!");

    // 5. Validate that the snapshot exists and is ready
    println!("Validating snapshot '{}'...", config.instance.snapshot_id);
    let snap_res = client.get_snapshot(&config.instance.snapshot_id).await;
    match snap_res {
        Ok(snapshot) => {
            println!(
                "Snapshot found! Status: {}, Size: {} bytes, Zone: {}",
                snapshot.status, snapshot.size, snapshot.zone
            );
            assert!(
                snapshot.status == "ready" || snapshot.status == "available",
                "Snapshot is not in ready or available status"
            );
            assert_eq!(
                snapshot.zone, config.scaleway.zone,
                "Snapshot zone mismatch"
            );
        }
        Err(e) => {
            panic!("Failed to fetch snapshot details: {:?}", e);
        }
    }
    println!("Scaleway back-end integration checks completed successfully!");
}

#[test]
fn test_create_server_payload_serialization() {
    let mut volumes = HashMap::new();
    let base_snap_uuid = uuid::Uuid::parse_str("1b552e81-401d-4c15-b0b2-3c89e2d46c28").unwrap();
    volumes.insert(
        "0".to_string(),
        SnapshotBootVolume {
            base_snapshot: base_snap_uuid,
            name: "test-server-root".to_string(),
            volume_type: InstanceVolumeType::SbsVolume,
            boot: true,
        },
    );

    let project_uuid = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap();
    let req = CreateServerRequest {
        name: "test-server".to_string(),
        project: project_uuid,
        commercial_type: "L40S-1-48G".to_string(),
        volumes,
        tags: vec!["test".to_string()],
    };

    let serialized = serde_json::to_value(&req).unwrap();

    assert_eq!(serialized["name"], "test-server");
    assert_eq!(
        serialized["project"],
        "00000000-0000-0000-0000-000000000000"
    );
    assert_eq!(serialized["commercial_type"], "L40S-1-48G");

    let volume_0 = &serialized["volumes"]["0"];
    assert_eq!(
        volume_0["base_snapshot"],
        "1b552e81-401d-4c15-b0b2-3c89e2d46c28"
    );
    assert_eq!(volume_0["name"], "test-server-root");
    assert_eq!(volume_0["volume_type"], "sbs_volume");
    assert_eq!(volume_0["boot"], true);
    assert!(volume_0.get("id").is_none());
    assert!(volume_0.get("image").is_none());
}

fn mock_config(url: &str) -> Config {
    let port_str = url.split(':').next_back().unwrap_or("8330");
    let clean_port: String = port_str.chars().filter(|c| c.is_ascii_digit()).collect();
    let port = clean_port.parse::<u16>().unwrap_or(8330);
    Config {
        scaleway: scaleway_chat::config::ScalewayConfig {
            access_key: "SCW123".to_string(),
            secret_key: SecretString::new("secret".to_string()),
            project_id: "00000000-0000-0000-0000-000000000000".to_string(),
            organization_id: "00000000-0000-0000-0000-000000000000".to_string(),
            zone: "fr-par-2".to_string(),
        },
        instance: scaleway_chat::config::InstanceConfig {
            name: "nemotron-l40s".to_string(),
            instance_type: "L40S-1-48G".to_string(),
            snapshot_id: "1b552e81-401d-4c15-b0b2-3c89e2d46c28".to_string(),
            public_ip: "new".to_string(),
            gpu_types: None,
        },
        nemotron: scaleway_chat::config::NemotronConfig {
            port,
            api_key: SecretString::new("key".to_string()),
            model: "model".to_string(),
            max_tokens: 10,
            temperature: 0.7,
            system_prompt: "prompt".to_string(),
        },
        timeouts: scaleway_chat::config::TimeoutsConfig {
            instance_creation_seconds: 5,
            instance_poll_interval_seconds: 1,
            nemotron_startup_seconds: 5,
            nemotron_poll_interval_seconds: 1,
            cleanup_timeout_seconds: 5,
            cleanup_poll_interval_seconds: 1,
            inference_timeout_seconds: 300,
        },
        logging: scaleway_chat::config::LoggingConfig { verbose: false },
    }
}

#[tokio::test]
async fn test_ip_attachment_success() {
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let ip_id = "eb41297e-e814-4887-a284-d88509b06318";
    let server_id = "17de9180-4edf-4fc4-8084-90e2e7b31c8c";

    // 1. Mock PATCH /ips/{ip_id} (attaching to server_id)
    Mock::given(method("PATCH"))
        .and(path(format!("/instance/v1/zones/fr-par-2/ips/{}", ip_id)))
        .and(body_json(serde_json::json!({
            "server": server_id
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": ip_id,
                "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000",
                "zone": "fr-par-2",
                "server": {
                    "id": server_id,
                    "name": "nemotron-l40s"
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // 2. Mock verification GET /ips/{ip_id} (returns attached)
    Mock::given(method("GET"))
        .and(path(format!("/instance/v1/zones/fr-par-2/ips/{}", ip_id)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": ip_id,
                "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000",
                "zone": "fr-par-2",
                "server": {
                    "id": server_id,
                    "name": "nemotron-l40s"
                }
            }
        })))
        .mount(&mock_server)
        .await;

    let attach_res = client.attach_ip_to_server(ip_id, server_id).await;
    assert!(attach_res.is_ok());

    let verified_ip = client.get_public_ip(ip_id).await.unwrap();
    println!(
        "DEBUG test_ip_attachment_success verified_ip: {:?}",
        verified_ip
    );
    assert!(verified_ip.server.is_some());
    assert_eq!(verified_ip.server.unwrap().id, server_id);
}

#[tokio::test]
async fn test_provision_resume_no_duplicate_resources() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));

    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());
    state.volume_id = Some("4659a41e-d227-4de5-9d01-99db0a579d8b".to_string());
    state.public_ip_id = Some("eb41297e-e814-4887-a284-d88509b06318".to_string());
    state.instance_id = Some("17de9180-4edf-4fc4-8084-90e2e7b31c8c".to_string());

    // 1. Mock validate_auth_and_project
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": []
        })))
        .mount(&mock_server)
        .await;

    // 2. Mock get_snapshot
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "name": "snapshot-1",
            "status": "ready",
            "size": 100000000000u64,
            "project_id": "00000000-0000-0000-0000-000000000000",
            "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    // 3. Mock validate_instance_type_available
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": {
                "L40S-1-48G": {
                    "volumes_constraint": {
                        "min_size": 1000000000,
                        "max_size": 1000000000000u64
                    }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // --- CLEANUP MOCKS FOR OLD RESOURCES ---

    // Old server GET verification (returns 404 for deleted)
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/17de9180-4edf-4fc4-8084-90e2e7b31c8c",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found",
            "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Old server GET (returns stopped during reconcile and initial cleanup)
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/17de9180-4edf-4fc4-8084-90e2e7b31c8c",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "17de9180-4edf-4fc4-8084-90e2e7b31c8c",
                "name": "nemotron-l40s",
                "state": "stopped",
                "public_ip": null,
                "volumes": {
                    "0": {
                        "id": "4659a41e-d227-4de5-9d01-99db0a579d8b",
                        "name": "volume-1",
                        "volume_type": "sbs_volume"
                    }
                }
            }
        })))
        .up_to_n_times(2)
        .mount(&mock_server)
        .await;

    // Old server power_off
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/17de9180-4edf-4fc4-8084-90e2e7b31c8c/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // Old server DELETE
    Mock::given(method("DELETE"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/17de9180-4edf-4fc4-8084-90e2e7b31c8c",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // Old volume GET verification (returns 404 for deleted)
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/volumes/4659a41e-d227-4de5-9d01-99db0a579d8b",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found",
            "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Old volume DELETE
    Mock::given(method("DELETE"))
        .and(path(
            "/block/v1/zones/fr-par-2/volumes/4659a41e-d227-4de5-9d01-99db0a579d8b",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // Old IP GET verification (returns 404 for deleted)
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/ips/eb41297e-e814-4887-a284-d88509b06318",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found",
            "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Old IP DELETE
    Mock::given(method("DELETE"))
        .and(path(
            "/instance/v1/zones/fr-par-2/ips/eb41297e-e814-4887-a284-d88509b06318",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // --- PROVISIONING MOCKS FOR NEW RESOURCE ATTEMPT ---

    // New server POST creation
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "99999999-4edf-4fc4-8084-90e2e7b31c8c",
                "name": "nemotron-l40s-new",
                "state": "stopped",
                "public_ip": null,
                "volumes": {
                    "0": {
                        "id": "88888888-d227-4de5-9d01-99db0a579d8b",
                        "name": "nemotron-l40s-new-root",
                        "volume_type": "sbs_volume"
                    }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // New volume GET
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/volumes/88888888-d227-4de5-9d01-99db0a579d8b",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "88888888-d227-4de5-9d01-99db0a579d8b",
            "name": "nemotron-l40s-new-root",
            "status": "available",
            "project_id": "00000000-0000-0000-0000-000000000000",
            "zone": "fr-par-2",
            "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28"
        })))
        .mount(&mock_server)
        .await;

    // New server poweron action
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/99999999-4edf-4fc4-8084-90e2e7b31c8c/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // New server wait_for_instance_running GET (polling)
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/99999999-4edf-4fc4-8084-90e2e7b31c8c",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "99999999-4edf-4fc4-8084-90e2e7b31c8c",
                "name": "nemotron-l40s-new",
                "state": "running",
                "public_ip": null,
                "volumes": {
                    "0": {
                        "id": "88888888-d227-4de5-9d01-99db0a579d8b",
                        "name": "nemotron-l40s-new-root",
                        "volume_type": "sbs_volume"
                    }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // New IP POST allocate
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/ips"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "77777777-e814-4887-a284-d88509b06318",
                "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000",
                "zone": "fr-par-2",
                "server": null
            }
        })))
        .mount(&mock_server)
        .await;

    // New IP PATCH attach
    Mock::given(method("PATCH"))
        .and(path(
            "/instance/v1/zones/fr-par-2/ips/77777777-e814-4887-a284-d88509b06318",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "77777777-e814-4887-a284-d88509b06318",
                "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000",
                "zone": "fr-par-2",
                "server": {
                    "id": "99999999-4edf-4fc4-8084-90e2e7b31c8c",
                    "name": "nemotron-l40s-new"
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // New IP GET verification
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/ips/77777777-e814-4887-a284-d88509b06318",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "77777777-e814-4887-a284-d88509b06318",
                "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000",
                "zone": "fr-par-2",
                "server": {
                    "id": "99999999-4edf-4fc4-8084-90e2e7b31c8c",
                    "name": "nemotron-l40s-new"
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // Mock Nemotron readiness check
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "object": "list",
            "data": [
                {
                    "id": "model",
                    "object": "model",
                    "created": 1686880000,
                    "owned_by": "organization"
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    if let Err(ref e) = res {
        println!(
            "DEBUG test_provision_resume_no_duplicate_resources error: {:?}",
            e
        );
    }
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "127.0.0.1");
    assert_eq!(
        state.volume_id.unwrap(),
        "88888888-d227-4de5-9d01-99db0a579d8b"
    );
    assert_eq!(
        state.public_ip_id.unwrap(),
        "77777777-e814-4887-a284-d88509b06318"
    );
    assert_eq!(
        state.instance_id.unwrap(),
        "99999999-4edf-4fc4-8084-90e2e7b31c8c"
    );

    let _ = std::fs::remove_file(state_file);
}

#[tokio::test]
async fn test_ip_already_attached_to_target() {
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));

    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());
    state.volume_id = Some("4659a41e-d227-4de5-9d01-99db0a579d8b".to_string());
    state.public_ip_id = Some("eb41297e-e814-4887-a284-d88509b06318".to_string());
    state.public_ip_address = Some("127.0.0.1".to_string());
    state.instance_id = Some("17de9180-4edf-4fc4-8084-90e2e7b31c8c".to_string());
    state.phase = ProvisioningPhase::Ready;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28", "name": "snap", "status": "ready",
            "size": 1000, "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": { "L40S-1-48G": {} }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/volumes/4659a41e-d227-4de5-9d01-99db0a579d8b",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "4659a41e-d227-4de5-9d01-99db0a579d8b",
            "name": "volume-1",
            "status": "available",
            "project_id": "00000000-0000-0000-0000-000000000000",
            "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/17de9180-4edf-4fc4-8084-90e2e7b31c8c",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "17de9180-4edf-4fc4-8084-90e2e7b31c8c", "name": "nemotron-l40s",
                "state": "running", "public_ip": {
                    "id": "eb41297e-e814-4887-a284-d88509b06318", "address": "127.0.0.1"
                },
                "volumes": {
                    "0": {
                        "id": "4659a41e-d227-4de5-9d01-99db0a579d8b",
                        "name": "volume-1",
                        "volume_type": "sbs_volume"
                    }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/ips/eb41297e-e814-4887-a284-d88509b06318",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "eb41297e-e814-4887-a284-d88509b06318", "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2",
                "server": {
                    "id": "17de9180-4edf-4fc4-8084-90e2e7b31c8c", "name": "nemotron-l40s"
                }
            }
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    if let Err(ref e) = res {
        println!("DEBUG test_ip_already_attached_to_target error: {:?}", e);
    }
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "127.0.0.1");

    let _ = std::fs::remove_file(state_file);
}

#[tokio::test]
async fn test_ip_attached_to_another_instance() {
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));

    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());
    state.volume_id = Some("4659a41e-d227-4de5-9d01-99db0a579d8b".to_string());
    state.public_ip_id = Some("eb41297e-e814-4887-a284-d88509b06318".to_string());
    state.public_ip_address = Some("127.0.0.1".to_string());
    state.instance_id = Some("17de9180-4edf-4fc4-8084-90e2e7b31c8c".to_string());
    state.phase = ProvisioningPhase::Ready;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28", "name": "snap", "status": "ready",
            "size": 1000, "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": { "L40S-1-48G": {} }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/volumes/4659a41e-d227-4de5-9d01-99db0a579d8b",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "4659a41e-d227-4de5-9d01-99db0a579d8b",
            "name": "volume-1",
            "status": "available",
            "project_id": "00000000-0000-0000-0000-000000000000",
            "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/17de9180-4edf-4fc4-8084-90e2e7b31c8c",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "17de9180-4edf-4fc4-8084-90e2e7b31c8c", "name": "nemotron-l40s",
                "state": "running", "public_ip": null,
                "volumes": {
                    "0": {
                        "id": "4659a41e-d227-4de5-9d01-99db0a579d8b",
                        "name": "volume-1",
                        "volume_type": "sbs_volume"
                    }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/ips/eb41297e-e814-4887-a284-d88509b06318",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "eb41297e-e814-4887-a284-d88509b06318", "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2",
                "server": {
                    "id": "different-server-uuid", "name": "other-server"
                }
            }
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    println!(
        "DEBUG test_ip_attached_to_another_instance error: {}",
        err_msg
    );
    assert!(err_msg.contains("Conflicting attachment"));

    let _ = std::fs::remove_file(state_file);
}

#[tokio::test]
async fn test_ip_missing_404() {
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let ip_id = "eb41297e-e814-4887-a284-d88509b06318";

    Mock::given(method("GET"))
        .and(path(format!("/instance/v1/zones/fr-par-2/ips/{}", ip_id)))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "IP not found",
            "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    let res = client.get_public_ip(ip_id).await;
    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(err_msg.contains("resource_not_found"));
}

#[tokio::test]
async fn test_instance_missing_404() {
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let server_id = "17de9180-4edf-4fc4-8084-90e2e7b31c8c";

    Mock::given(method("GET"))
        .and(path(format!(
            "/instance/v1/zones/fr-par-2/servers/{}",
            server_id
        )))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "Server not found",
            "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    let res = client.get_server(server_id).await;
    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(err_msg.contains("resource_not_found"));
}

#[tokio::test]
async fn test_ip_attachment_http_400_malformed() {
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let ip_id = "eb41297e-e814-4887-a284-d88509b06318";
    let server_id = "17de9180-4edf-4fc4-8084-90e2e7b31c8c";

    Mock::given(method("PATCH"))
        .and(path(format!("/instance/v1/zones/fr-par-2/ips/{}", ip_id)))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "message": "Validation error",
            "type": "invalid_arguments",
            "details": [
                {
                    "argument_name": "address",
                    "reason": "required",
                    "help_message": "Address is a required field"
                },
                {
                    "argument_name": "id",
                    "reason": "required"
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    let res = client.attach_ip_to_server(ip_id, server_id).await;
    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(err_msg.contains("invalid_arguments:"));
    assert!(err_msg.contains(
        "- argument_name: address\n  reason: required\n  help_message: Address is a required field"
    ));
    assert!(err_msg.contains("- argument_name: id\n  reason: required"));
}

#[tokio::test]
async fn test_ip_attachment_http_409_conflict() {
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let ip_id = "eb41297e-e814-4887-a284-d88509b06318";
    let server_id = "17de9180-4edf-4fc4-8084-90e2e7b31c8c";

    Mock::given(method("PATCH"))
        .and(path(format!("/instance/v1/zones/fr-par-2/ips/{}", ip_id)))
        .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
            "message": "IP is already attached to another server",
            "type": "conflict"
        })))
        .mount(&mock_server)
        .await;

    let res = client.attach_ip_to_server(ip_id, server_id).await;
    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(err_msg.contains("API Error (status 409 Conflict)"));
}

#[tokio::test]
async fn test_direct_snapshot_provisioning_flow() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));

    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // Mock Nemotron readiness check
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "object": "list",
            "data": [
                {
                    "id": "model",
                    "object": "model",
                    "created": 1686880000,
                    "owned_by": "organization"
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    // 1. Mock validate_auth_and_project
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    // 2. Mock get_snapshot
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "name": "snap",
            "status": "ready",
            "size": 1000,
            "project_id": "00000000-0000-0000-0000-000000000000",
            "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    // 3. Mock validate_instance_type_available
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": { "L40S-1-48G": {} }
        })))
        .mount(&mock_server)
        .await;

    // 4. Mock POST create instance
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "17de9180-4edf-4fc4-8084-90e2e7b31c8c",
                "name": "nemotron-l40s",
                "state": "stopped",
                "public_ip": null,
                "volumes": {
                    "0": {
                        "id": "4659a41e-d227-4de5-9d01-99db0a579d8b",
                        "name": "nemotron-l40s-root",
                        "volume_type": "sbs_volume"
                    }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // 5. Mock get_volume
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/volumes/4659a41e-d227-4de5-9d01-99db0a579d8b",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "4659a41e-d227-4de5-9d01-99db0a579d8b",
            "name": "nemotron-l40s-root",
            "status": "available",
            "project_id": "00000000-0000-0000-0000-000000000000",
            "zone": "fr-par-2",
            "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28"
        })))
        .mount(&mock_server)
        .await;

    // 6. Mock allocate_public_ip
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/ips"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "eb41297e-e814-4887-a284-d88509b06318",
                "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000",
                "zone": "fr-par-2",
                "server": null
            }
        })))
        .mount(&mock_server)
        .await;

    // 8. Mock PATCH IP attach
    Mock::given(method("PATCH"))
        .and(path(
            "/instance/v1/zones/fr-par-2/ips/eb41297e-e814-4887-a284-d88509b06318",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "eb41297e-e814-4887-a284-d88509b06318",
                "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000",
                "zone": "fr-par-2",
                "server": {
                    "id": "17de9180-4edf-4fc4-8084-90e2e7b31c8c",
                    "name": "nemotron-l40s"
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // 9. Mock get_public_ip (attached, verification)
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/ips/eb41297e-e814-4887-a284-d88509b06318",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "eb41297e-e814-4887-a284-d88509b06318",
                "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000",
                "zone": "fr-par-2",
                "server": {
                    "id": "17de9180-4edf-4fc4-8084-90e2e7b31c8c",
                    "name": "nemotron-l40s"
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // 10. Mock poweron action
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/17de9180-4edf-4fc4-8084-90e2e7b31c8c/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // 11. Mock wait_for_instance_running (polling GET)
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/17de9180-4edf-4fc4-8084-90e2e7b31c8c",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "17de9180-4edf-4fc4-8084-90e2e7b31c8c",
                "name": "nemotron-l40s",
                "state": "running",
                "public_ip": {
                    "id": "eb41297e-e814-4887-a284-d88509b06318",
                    "address": "127.0.0.1"
                },
                "volumes": {
                    "0": {
                        "id": "4659a41e-d227-4de5-9d01-99db0a579d8b",
                        "name": "volume-1",
                        "volume_type": "sbs_volume"
                    }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    let mut config = mock_config(&mock_server.uri());
    config.timeouts.nemotron_startup_seconds = 2;
    config.timeouts.instance_creation_seconds = 2;
    config.timeouts.cleanup_timeout_seconds = 2;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    if let Err(ref e) = res {
        println!(
            "DEBUG test_direct_snapshot_provisioning_flow error: {:?}",
            e
        );
    }
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "127.0.0.1");

    // Assert that local state is fully updated to version 4 and contains volume ID
    assert_eq!(state.version, 4);
    assert_eq!(state.creation_mode, Some("snapshot_direct".to_string()));
    assert_eq!(
        state.volume_id,
        Some("4659a41e-d227-4de5-9d01-99db0a579d8b".to_string())
    );
    assert_eq!(
        state.instance_id,
        Some("17de9180-4edf-4fc4-8084-90e2e7b31c8c".to_string())
    );
    assert_eq!(
        state.public_ip_id,
        Some("eb41297e-e814-4887-a284-d88509b06318".to_string())
    );

    let _ = std::fs::remove_file(state_file);
}

#[tokio::test]
async fn test_legacy_state_migration_success() {
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));

    // Construct a legacy state (version 1, creation_mode None, instance_id set)
    let mut state = State {
        version: 1,
        phase: ProvisioningPhase::Ready,
        creation_mode: None,
        attempt_id: "eb41297e-e814-4887-a284-d88509b06318".to_string(),
        selected_gpu_type: "L40S-1-48G".to_string(),
        instance_id: Some("17de9180-4edf-4fc4-8084-90e2e7b31c8c".to_string()),
        volume_id: None,
        public_ip_id: Some("eb41297e-e814-4887-a284-d88509b06318".to_string()),
        public_ip_address: Some("127.0.0.1".to_string()),
        snapshot_id: config.instance.snapshot_id.clone(),
        zone: config.scaleway.zone.clone(),
        attempted_gpu_types: vec!["L40S-1-48G".to_string()],
        created_at: chrono::Utc::now(),
        path: Some(state_file.clone()),
    };

    // Mock validate auth and snapshot checks
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28", "name": "snap", "status": "ready",
            "size": 1000, "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": { "L40S-1-48G": {} }
        })))
        .mount(&mock_server)
        .await;

    // GET server (exists, returns volumes)
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/17de9180-4edf-4fc4-8084-90e2e7b31c8c",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "17de9180-4edf-4fc4-8084-90e2e7b31c8c",
                "name": "nemotron-l40s",
                "state": "running",
                "public_ip": {
                    "id": "eb41297e-e814-4887-a284-d88509b06318",
                    "address": "127.0.0.1"
                },
                "volumes": {
                    "0": {
                        "id": "4659a41e-d227-4de5-9d01-99db0a579d8b",
                        "name": "volume-1",
                        "volume_type": "sbs_volume"
                    }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // GET public IP (already attached)
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/ips/eb41297e-e814-4887-a284-d88509b06318",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "eb41297e-e814-4887-a284-d88509b06318",
                "address": "127.0.0.1",
                "project": "00000000-0000-0000-0000-000000000000",
                "zone": "fr-par-2",
                "server": {
                    "id": "17de9180-4edf-4fc4-8084-90e2e7b31c8c",
                    "name": "nemotron-l40s"
                }
            }
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "127.0.0.1");

    // State must be migrated to version 4 and creation_mode populated
    assert_eq!(state.version, 4);
    assert_eq!(state.creation_mode, Some("snapshot_direct".to_string()));
    assert_eq!(
        state.volume_id,
        Some("4659a41e-d227-4de5-9d01-99db0a579d8b".to_string())
    );

    let _ = std::fs::remove_file(state_file);
}

#[tokio::test]
async fn test_legacy_state_rejection_no_instance() {
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));

    // Case 1: Legacy state with volume, but no instance ID
    let mut state1 = State {
        version: 1,
        phase: ProvisioningPhase::Ready,
        creation_mode: None,
        attempt_id: "eb41297e-e814-4887-a284-d88509b06318".to_string(),
        selected_gpu_type: "L40S-1-48G".to_string(),
        instance_id: None,
        volume_id: Some("4659a41e-d227-4de5-9d01-99db0a579d8b".to_string()),
        public_ip_id: None,
        public_ip_address: None,
        snapshot_id: config.instance.snapshot_id.clone(),
        zone: config.scaleway.zone.clone(),
        attempted_gpu_types: vec!["L40S-1-48G".to_string()],
        created_at: chrono::Utc::now(),
        path: Some(state_file.clone()),
    };

    // Case 2: Legacy state with instance ID set but it does not exist on Scaleway (query 404)
    let mut state2 = State {
        version: 1,
        phase: ProvisioningPhase::Ready,
        creation_mode: None,
        attempt_id: "eb41297e-e814-4887-a284-d88509b06318".to_string(),
        selected_gpu_type: "L40S-1-48G".to_string(),
        instance_id: Some("non-existent-instance-id".to_string()),
        volume_id: Some("4659a41e-d227-4de5-9d01-99db0a579d8b".to_string()),
        public_ip_id: None,
        public_ip_address: None,
        snapshot_id: config.instance.snapshot_id.clone(),
        zone: config.scaleway.zone.clone(),
        attempted_gpu_types: vec!["L40S-1-48G".to_string()],
        created_at: chrono::Utc::now(),
        path: Some(state_file.clone()),
    };

    // Mock validation endpoints
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28", "name": "snap", "status": "ready",
            "size": 1000, "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": { "L40S-1-48G": {} }
        })))
        .mount(&mock_server)
        .await;

    // GET non-existent server -> returns 404
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/non-existent-instance-id",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "Server not found",
            "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Provisioning state1 must fail because server creation is not mocked for the restart attempt,
    // but the stale legacy state must have been cleaned up and a restart initiated.
    let res1 = client
        .provision_resources(&config, &mut state1, &scaleway_chat::NoopProgress)
        .await;
    assert!(res1.is_err());
    let err1 = res1.unwrap_err().to_string();
    assert!(err1.contains("Instance creation failed") || err1.contains("API Error"));
    assert_eq!(state1.phase, ProvisioningPhase::CleaningUp);
    assert_eq!(state1.instance_id, None);
    assert_eq!(state1.volume_id, None);

    // Provisioning state2 must fail likewise, but stale state must have been cleaned.
    let res2 = client
        .provision_resources(&config, &mut state2, &scaleway_chat::NoopProgress)
        .await;
    assert!(res2.is_err());
    let err2 = res2.unwrap_err().to_string();
    assert!(err2.contains("Instance creation failed") || err2.contains("API Error"));
    assert_eq!(state2.phase, ProvisioningPhase::CleaningUp);
    assert_eq!(state2.instance_id, None);
    assert_eq!(state2.volume_id, None);

    let _ = std::fs::remove_file(state_file);
}

// =========================================================================
// HAL MODE TESTS
// =========================================================================
use scaleway_chat::hal::{escape_html, HalArguments, HalRequest, HalResponse};

#[test]
fn test_hal_html_escaping() {
    assert_eq!(escape_html("hello & <world>"), "hello &amp; &lt;world&gt;");
}

#[test]
fn test_hal_request_parsing() {
    // Valid JSON with array arguments
    let json_array =
        r#"{"request_id": "test-uuid", "command": "scaleway", "arguments": ["Explain", "Juju"]}"#;
    let req: HalRequest = serde_json::from_str(json_array).unwrap();
    assert_eq!(req.request_id, "test-uuid");
    assert_eq!(req.command, "scaleway");
    assert_eq!(req.prompt_string(), "Explain Juju");
    assert!(!req.is_kill_confirmed());

    // Valid JSON with string arguments
    let json_string =
        r#"{"request_id": "test-uuid", "command": "scaleway", "arguments": "Explain Juju"}"#;
    let req: HalRequest = serde_json::from_str(json_string).unwrap();
    assert_eq!(req.prompt_string(), "Explain Juju");

    // Invalid JSON
    let invalid_json = r#"{"request_id": "test-uuid", "command": "scaleway", "arguments":}"#;
    let req_res: std::result::Result<HalRequest, _> = serde_json::from_str(invalid_json);
    assert!(req_res.is_err());

    // Kill confirmation tests
    let kill_no = r#"{"request_id": "id", "command": "scaleway_kill", "arguments": ["NO"]}"#;
    let req_kill_no: HalRequest = serde_json::from_str(kill_no).unwrap();
    assert!(!req_kill_no.is_kill_confirmed());

    let kill_yes = r#"{"request_id": "id", "command": "scaleway_kill", "arguments": ["KILL"]}"#;
    let req_kill_yes: HalRequest = serde_json::from_str(kill_yes).unwrap();
    assert!(req_kill_yes.is_kill_confirmed());

    let kill_yes_str = r#"{"request_id": "id", "command": "scaleway_kill", "arguments": "KILL"}"#;
    let req_kill_yes_str: HalRequest = serde_json::from_str(kill_yes_str).unwrap();
    assert!(req_kill_yes_str.is_kill_confirmed());
}

#[test]
fn test_hal_response_serialization() {
    let prog = HalResponse::Progress {
        request_id: "uuid1",
        percent: 25,
        message: "Doing stuff",
        format: "html",
    };
    let prog_str = serde_json::to_string(&prog).unwrap();
    assert!(prog_str.contains(r#""type":"progress""#));
    assert!(prog_str.contains(r#""request_id":"uuid1""#));
    assert!(prog_str.contains(r#""percent":25"#));

    let fin = HalResponse::Final {
        request_id: "uuid2",
        format: "html",
        message: "Done",
        trusted_html: false,
    };
    let fin_str = serde_json::to_string(&fin).unwrap();
    assert!(fin_str.contains(r#""type":"final""#));
    assert!(fin_str.contains(r#""request_id":"uuid2""#));
    assert!(fin_str.contains(r#""trusted_html":false"#));

    let err = HalResponse::Error {
        request_id: "uuid3",
        reason: "Failed",
        technical_details: "Detailed logs",
        suggested_action: "Retry",
        format: "html",
    };
    let err_str = serde_json::to_string(&err).unwrap();
    assert!(err_str.contains(r#""type":"error""#));
    assert!(err_str.contains(r#""request_id":"uuid3""#));
    assert!(err_str.contains(r#""format":"html""#));
}

#[tokio::test]
async fn test_hal_subprocess_integration() {
    let bin_path = match std::env::var("CARGO_BIN_EXE_scaleway-chat") {
        Ok(path) => path,
        Err(_) => {
            println!("Skipping subprocess test: CARGO_BIN_EXE_scaleway-chat not set.");
            return;
        }
    };

    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::Command;

    let temp_dir =
        std::env::temp_dir().join(format!("scaleway_chat_test_hal_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config_path = temp_dir.join("config.toml");

    let mock_config = r#"[scaleway]
access_key = "SCWXXXXXXXXXXXXXXXXX"
secret_key = "secret"
project_id = "00000000-0000-0000-0000-000000000000"
organization_id = "00000000-0000-0000-0000-000000000000"
zone = "fr-par-2"

[instance]
name = "nemotron-l40s"
instance_type = "L40S-1-48G"
snapshot_id = "00000000-0000-0000-0000-000000000000"
public_ip = "new"

[nemotron]
port = 8330
api_key = "key"
model = "model"
max_tokens = 10
temperature = 0.7
system_prompt = "system"

[timeouts]
instance_creation_seconds = 5
instance_poll_interval_seconds = 1
nemotron_startup_seconds = 5
nemotron_poll_interval_seconds = 1
cleanup_timeout_seconds = 5
cleanup_poll_interval_seconds = 1

[logging]
verbose = false
"#;
    std::fs::write(&config_path, mock_config).unwrap();

    let mut child = Command::new(&bin_path)
        .arg("--config")
        .arg(&config_path)
        .arg("hal")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn scaleway-chat hal");

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();

    let req = serde_json::json!({
        "request_id": "test-subprocess-uuid",
        "command": "scaleway_help",
        "arguments": []
    });

    let req_line = req.to_string() + "\n";
    stdin.write_all(req_line.as_bytes()).await.unwrap();
    stdin.flush().await.unwrap();
    drop(stdin);

    let mut reader = BufReader::new(stdout).lines();
    let mut lines = Vec::new();
    while let Ok(Some(line)) = reader.next_line().await {
        lines.push(line);
    }

    assert_eq!(lines.len(), 1);
    let resp: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    assert_eq!(resp["type"], "final");
    assert_eq!(resp["request_id"], "test-subprocess-uuid");
    assert!(resp["message"]
        .as_str()
        .unwrap()
        .contains("Available Scaleway GPU Commands"));

    let status = child.wait().await.unwrap();
    assert!(status.success());

    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_hal_subprocess_invalid_command() {
    let bin_path = match std::env::var("CARGO_BIN_EXE_scaleway-chat") {
        Ok(path) => path,
        Err(_) => return,
    };

    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::Command;

    let temp_dir = std::env::temp_dir().join(format!(
        "scaleway_chat_test_hal_invalid_{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config_path = temp_dir.join("config.toml");

    let mock_config = r#"[scaleway]
access_key = "SCW1"
secret_key = "secret"
project_id = "00000000-0000-0000-0000-000000000000"
organization_id = "00000000-0000-0000-0000-000000000000"
zone = "fr-par-2"
[instance]
name = "name"
instance_type = "type"
snapshot_id = "00000000-0000-0000-0000-000000000000"
public_ip = "new"
[nemotron]
port = 123
api_key = "key"
model = "model"
max_tokens = 1
temperature = 0.5
system_prompt = "sys"
[timeouts]
instance_creation_seconds = 1
instance_poll_interval_seconds = 1
nemotron_startup_seconds = 1
nemotron_poll_interval_seconds = 1
cleanup_timeout_seconds = 5
cleanup_poll_interval_seconds = 1
[logging]
verbose = false
"#;
    std::fs::write(&config_path, mock_config).unwrap();

    let mut child = Command::new(&bin_path)
        .arg("--config")
        .arg(&config_path)
        .arg("hal")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();

    let req = serde_json::json!({
        "request_id": "test-invalid-cmd-uuid",
        "command": "scaleway_invalid_cmd_xyz",
        "arguments": []
    });

    let req_line = req.to_string() + "\n";
    stdin.write_all(req_line.as_bytes()).await.unwrap();
    stdin.flush().await.unwrap();
    drop(stdin);

    let mut reader = BufReader::new(stdout).lines();
    let mut lines = Vec::new();
    while let Ok(Some(line)) = reader.next_line().await {
        lines.push(line);
    }

    assert_eq!(lines.len(), 1);
    let resp: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    assert_eq!(resp["type"], "error");
    assert_eq!(resp["request_id"], "test-invalid-cmd-uuid");
    assert!(resp["reason"]
        .as_str()
        .unwrap()
        .contains("Unsupported command"));

    let status = child.wait().await.unwrap();
    assert!(!status.success());
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_hal_scaleway_chat_e2e_integration() {
    let bin_path = match std::env::var("CARGO_BIN_EXE_scaleway-chat") {
        Ok(path) => path,
        Err(_) => return,
    };

    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::Command;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;

    // 1. Setup mock server expectations
    // validate_auth_and_project
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    // get_snapshot
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "name": "snap",
            "status": "ready",
            "size": 1000,
            "project_id": "00000000-0000-0000-0000-000000000000",
            "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    // products metadata
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": {
                "L40S-1-48G": {
                    "volumes_constraint": { "min_size": 500, "max_size": 2000 }
                },
                "L40S-2-48G": {
                    "volumes_constraint": { "min_size": 500, "max_size": 2000 }
                },
                "H100-1-80G": {
                    "volumes_constraint": { "min_size": 500, "max_size": 2000 }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // POST create server
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "inst-e2e-123",
                "name": "nemotron-l40s-l40s-1-48g-e2e",
                "state": "stopped",
                "volumes": {
                    "0": { "id": "vol-e2e-123", "volume_type": "sbs_volume" }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // GET volume status
    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-e2e-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "vol-e2e-123", "name": "vol-root", "status": "available", "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // POST action power on
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-e2e-123/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // GET server status
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-e2e-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "inst-e2e-123", "name": "nemotron-l40s", "state": "running", "public_ip": null,
                "volumes": { "0": { "id": "vol-e2e-123", "volume_type": "sbs_volume" } }
            }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // POST allocate IP
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/ips"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-e2e-123", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": null }
        })))
        .mount(&mock_server)
        .await;

    // PATCH attach IP
    Mock::given(method("PATCH"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-e2e-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-e2e-123", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "inst-e2e-123", "name": "nemotron-l40s" } }
        })))
        .mount(&mock_server)
        .await;

    // GET attached IP to verify
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-e2e-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "ip-e2e-123", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "inst-e2e-123", "name": "nemotron-l40s" }
            }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Mock Nemotron readiness check (GET /v1/models)
    let mock_uri = mock_server.uri();
    let port_str = mock_uri.split(':').next_back().unwrap_or("8330");
    let clean_port: String = port_str.chars().filter(|c| c.is_ascii_digit()).collect();
    let port = clean_port.parse::<u16>().unwrap_or(8330);

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "object": "list",
            "data": [{"id": "model", "object": "model", "created": 1686880000, "owned_by": "org"}]
        })))
        .mount(&mock_server)
        .await;

    // Setup mock config file
    let temp_dir = std::env::temp_dir().join(format!("hal_e2e_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config_path = temp_dir.join("config.toml");

    let mock_config = format!(
        r#"[scaleway]
access_key = "SCW123"
secret_key = "secret"
project_id = "00000000-0000-0000-0000-000000000000"
organization_id = "00000000-0000-0000-0000-000000000000"
zone = "fr-par-2"

[instance]
name = "nemotron-l40s"
snapshot_id = "1b552e81-401d-4c15-b0b2-3c89e2d46c28"
public_ip = "new"
gpu_types = ["L40S-1-48G"]

[nemotron]
port = {}
api_key = "key"
model = "model"
max_tokens = 10
temperature = 0.7
system_prompt = "system"

[timeouts]
instance_creation_seconds = 5
instance_poll_interval_seconds = 1
nemotron_startup_seconds = 5
nemotron_poll_interval_seconds = 1
cleanup_timeout_seconds = 5
cleanup_poll_interval_seconds = 1

[logging]
verbose = false
"#,
        port
    );
    std::fs::write(&config_path, mock_config).unwrap();

    // 2. Request to start the instance received by HAL
    let mut child = Command::new(&bin_path)
        .arg("--config")
        .arg(&config_path)
        .arg("hal")
        .env("HOME", temp_dir.to_str().unwrap())
        .env("SCW_API_URL", &mock_uri)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn scaleway-chat hal");

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();

    // Send scaleway_start request command
    let start_req = serde_json::json!({
        "request_id": "e2e-start-req",
        "command": "scaleway_start",
        "arguments": []
    });

    let req_line = start_req.to_string() + "\n";
    stdin.write_all(req_line.as_bytes()).await.unwrap();
    stdin.flush().await.unwrap();
    drop(stdin);

    let mut reader = BufReader::new(stdout).lines();
    let mut lines = Vec::new();
    while let Ok(Some(line)) = reader.next_line().await {
        println!("STDOUT: {}", line);
        lines.push(line);
    }

    let mut err_reader = BufReader::new(child.stderr.take().unwrap()).lines();
    while let Ok(Some(err_line)) = err_reader.next_line().await {
        println!("STDERR: {}", err_line);
    }

    // 3. Validation that the instance is created
    assert!(!lines.is_empty(), "No output received from scaleway_start");
    let final_line = lines.last().unwrap();
    let start_resp: serde_json::Value = serde_json::from_str(final_line).unwrap();
    assert_eq!(start_resp["type"], "final");
    assert_eq!(start_resp["request_id"], "e2e-start-req");
    assert!(start_resp["message"].as_str().unwrap().contains("Running"));
    assert!(start_resp["message"]
        .as_str()
        .unwrap()
        .contains("inst-e2e-123"));

    let _ = child.wait().await;

    // Setup mock endpoints for the KILL phase
    // GET instance-e2e-123 for stop check -> returns stopped (so it doesn't wait)
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-e2e-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": { "id": "inst-e2e-123", "name": "inst", "state": "stopped", "volumes": {} }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // POST power off
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-e2e-123/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // DELETE instance
    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-e2e-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // DELETE volume
    Mock::given(method("DELETE"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-e2e-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // DELETE IP
    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-e2e-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // GET checks during verified cleanup -> return 404 to verify deleted
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-e2e-123"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-e2e-123"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-e2e-123"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    // 4. Request to kill the instance received by HAL
    let mut kill_child = Command::new(&bin_path)
        .arg("--config")
        .arg(&config_path)
        .arg("hal")
        .env("HOME", temp_dir.to_str().unwrap())
        .env("SCW_API_URL", &mock_uri)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn scaleway-chat hal for kill");

    let mut kill_stdin = kill_child.stdin.take().unwrap();
    let kill_stdout = kill_child.stdout.take().unwrap();

    let kill_req = serde_json::json!({
        "request_id": "e2e-kill-req",
        "command": "scaleway_kill",
        "arguments": ["KILL"]
    });

    let kill_req_line = kill_req.to_string() + "\n";
    kill_stdin
        .write_all(kill_req_line.as_bytes())
        .await
        .unwrap();
    kill_stdin.flush().await.unwrap();
    drop(kill_stdin);

    let mut kill_reader = BufReader::new(kill_stdout).lines();
    let mut kill_lines = Vec::new();
    while let Ok(Some(line)) = kill_reader.next_line().await {
        kill_lines.push(line);
    }

    // 5. Validation that the instance is correctly killed
    assert!(
        !kill_lines.is_empty(),
        "No output received from scaleway_kill"
    );
    let kill_final_line = kill_lines.last().unwrap();
    let kill_resp: serde_json::Value = serde_json::from_str(kill_final_line).unwrap();
    assert_eq!(kill_resp["type"], "final");
    assert_eq!(kill_resp["request_id"], "e2e-kill-req");
    assert!(kill_resp["message"]
        .as_str()
        .unwrap()
        .contains("Teardown complete"));

    let _ = kill_child.wait().await;
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_hal_scaleway_chat_e2e_full_chat() {
    let bin_path = match std::env::var("CARGO_BIN_EXE_scaleway-chat") {
        Ok(path) => path,
        Err(_) => return,
    };

    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::Command;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;

    // validate_auth_and_project
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    // get_snapshot
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "name": "snap",
            "status": "ready",
            "size": 1000,
            "project_id": "00000000-0000-0000-0000-000000000000",
            "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    // products metadata
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": {
                "L40S-1-48G": {
                    "volumes_constraint": { "min_size": 500, "max_size": 2000 }
                },
                "L40S-2-48G": {
                    "volumes_constraint": { "min_size": 500, "max_size": 2000 }
                },
                "H100-1-80G": {
                    "volumes_constraint": { "min_size": 500, "max_size": 2000 }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // POST create server
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "inst-e2e-chat",
                "name": "nemotron-l40s-l40s-1-48g-chat-e2e",
                "state": "stopped",
                "volumes": {
                    "0": { "id": "vol-e2e-chat", "volume_type": "sbs_volume" }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    // GET volume status
    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-e2e-chat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "vol-e2e-chat", "name": "vol-root", "status": "available", "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // POST action power on
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-e2e-chat/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // GET server status
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-e2e-chat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "inst-e2e-chat", "name": "nemotron-l40s", "state": "running", "public_ip": null,
                "volumes": { "0": { "id": "vol-e2e-chat", "volume_type": "sbs_volume" } }
            }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // POST allocate IP
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/ips"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-e2e-chat", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": null }
        })))
        .mount(&mock_server)
        .await;

    // PATCH attach IP
    Mock::given(method("PATCH"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-e2e-chat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-e2e-chat", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "inst-e2e-chat", "name": "nemotron-l40s" } }
        })))
        .mount(&mock_server)
        .await;

    // GET attached IP to verify
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-e2e-chat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": {
                "id": "ip-e2e-chat", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "inst-e2e-chat", "name": "nemotron-l40s" }
            }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Mock Nemotron readiness check (GET /v1/models)
    let mock_uri = mock_server.uri();
    let port_str = mock_uri.split(':').next_back().unwrap_or("8330");
    let clean_port: String = port_str.chars().filter(|c| c.is_ascii_digit()).collect();
    let port = clean_port.parse::<u16>().unwrap_or(8330);

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "object": "list",
            "data": [{"id": "model", "object": "model", "created": 1686880000, "owned_by": "org"}]
        })))
        .mount(&mock_server)
        .await;

    // Mock Nemotron chat completions stream endpoint: POST /v1/chat/completions
    let response_body =
        "data: {\"choices\": [{\"delta\": {\"content\": \"2\"}}]}\n\ndata: [DONE]\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(response_body)
                .append_header("content-type", "text/event-stream"),
        )
        .mount(&mock_server)
        .await;

    // Setup mock config file
    let temp_dir = std::env::temp_dir().join(format!("hal_e2e_chat_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config_path = temp_dir.join("config.toml");

    let mock_config = format!(
        r#"[scaleway]
access_key = "SCW123"
secret_key = "secret"
project_id = "00000000-0000-0000-0000-000000000000"
organization_id = "00000000-0000-0000-0000-000000000000"
zone = "fr-par-2"

[instance]
name = "nemotron-l40s"
snapshot_id = "1b552e81-401d-4c15-b0b2-3c89e2d46c28"
public_ip = "new"
gpu_types = ["L40S-1-48G"]

[nemotron]
port = {}
api_key = "key"
model = "model"
max_tokens = 10
temperature = 0.7
system_prompt = "system"

[timeouts]
instance_creation_seconds = 5
instance_poll_interval_seconds = 1
nemotron_startup_seconds = 5
nemotron_poll_interval_seconds = 1
cleanup_timeout_seconds = 5
cleanup_poll_interval_seconds = 1

[logging]
verbose = false
"#,
        port
    );
    std::fs::write(&config_path, mock_config).unwrap();

    // 1 & 2. Create Instance & run successful chat completion ("1+1" -> "2")
    let mut child = Command::new(&bin_path)
        .arg("--config")
        .arg(&config_path)
        .arg("hal")
        .env("HOME", temp_dir.to_str().unwrap())
        .env("SCW_API_URL", &mock_uri)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn scaleway-chat hal");

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();

    // Send scaleway command with "1+1" arguments
    let chat_req = serde_json::json!({
        "request_id": "e2e-chat-req",
        "command": "scaleway",
        "arguments": ["1+1"]
    });

    let req_line = chat_req.to_string() + "\n";
    stdin.write_all(req_line.as_bytes()).await.unwrap();
    stdin.flush().await.unwrap();
    drop(stdin);

    let mut reader = BufReader::new(stdout).lines();
    let mut lines = Vec::new();
    while let Ok(Some(line)) = reader.next_line().await {
        lines.push(line);
    }

    assert!(
        !lines.is_empty(),
        "No output received from scaleway chat command"
    );
    let final_line = lines.last().unwrap();
    let chat_resp: serde_json::Value = serde_json::from_str(final_line).unwrap();
    assert_eq!(chat_resp["type"], "final");
    assert_eq!(chat_resp["request_id"], "e2e-chat-req");

    // Check that the returned message contains the mathematical result "2"
    let final_message = chat_resp["message"].as_str().unwrap();
    assert!(
        final_message.contains("2"),
        "Response does not contain math answer '2': {}",
        final_message
    );

    let _ = child.wait().await;

    // 3 & 4. Setup mock endpoints for the KILL phase and verify resource deletion
    // GET instance-e2e-chat for stop check -> returns stopped (so it doesn't wait)
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-e2e-chat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": { "id": "inst-e2e-chat", "name": "inst", "state": "stopped", "volumes": {} }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // POST power off
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-e2e-chat/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // DELETE instance
    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-e2e-chat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // DELETE volume
    Mock::given(method("DELETE"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-e2e-chat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // DELETE IP
    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-e2e-chat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // GET checks during verified cleanup -> return 404 to verify deleted
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-e2e-chat"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-e2e-chat"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-e2e-chat"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    // Spawn kill command to verify resources teardown
    let mut kill_child = Command::new(&bin_path)
        .arg("--config")
        .arg(&config_path)
        .arg("hal")
        .env("HOME", temp_dir.to_str().unwrap())
        .env("SCW_API_URL", &mock_uri)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn scaleway-chat hal for kill");

    let mut kill_stdin = kill_child.stdin.take().unwrap();
    let kill_stdout = kill_child.stdout.take().unwrap();

    let kill_req = serde_json::json!({
        "request_id": "e2e-kill-req-2",
        "command": "scaleway_kill",
        "arguments": ["KILL"]
    });

    let kill_req_line = kill_req.to_string() + "\n";
    kill_stdin
        .write_all(kill_req_line.as_bytes())
        .await
        .unwrap();
    kill_stdin.flush().await.unwrap();
    drop(kill_stdin);

    let mut kill_reader = BufReader::new(kill_stdout).lines();
    let mut kill_lines = Vec::new();
    while let Ok(Some(line)) = kill_reader.next_line().await {
        kill_lines.push(line);
    }

    assert!(
        !kill_lines.is_empty(),
        "No output received from scaleway_kill"
    );
    let kill_final_line = kill_lines.last().unwrap();
    let kill_resp: serde_json::Value = serde_json::from_str(kill_final_line).unwrap();
    assert_eq!(kill_resp["type"], "final");
    assert_eq!(kill_resp["request_id"], "e2e-kill-req-2");
    assert!(kill_resp["message"]
        .as_str()
        .unwrap()
        .contains("Teardown complete"));

    let _ = kill_child.wait().await;
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_hal_compliance_html_rules() {
    let bin_path = match std::env::var("CARGO_BIN_EXE_scaleway-chat") {
        Ok(path) => path,
        Err(_) => return,
    };

    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::Command;

    let temp_dir =
        std::env::temp_dir().join(format!("scaleway_chat_test_hal_compliance_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config_path = temp_dir.join("config.toml");

    let mock_config = r#"[scaleway]
access_key = "SCWXXXXXXXXXXXXXXXXX"
secret_key = "secret"
project_id = "00000000-0000-0000-0000-000000000000"
organization_id = "00000000-0000-0000-0000-000000000000"
zone = "fr-par-2"
[instance]
name = "name"
instance_type = "type"
snapshot_id = "00000000-0000-0000-0000-000000000000"
public_ip = "new"
[nemotron]
port = 123
api_key = "key"
model = "model"
max_tokens = 1
temperature = 0.5
system_prompt = "sys"
[timeouts]
instance_creation_seconds = 1
instance_poll_interval_seconds = 1
nemotron_startup_seconds = 1
nemotron_poll_interval_seconds = 1
cleanup_timeout_seconds = 5
cleanup_poll_interval_seconds = 1
[logging]
verbose = false
"#;
    std::fs::write(&config_path, mock_config).unwrap();

    let mut child = Command::new(&bin_path)
        .arg("--config")
        .arg(&config_path)
        .arg("hal")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();

    let req = serde_json::json!({
        "request_id": "test-compliance-uuid",
        "command": "scaleway_help",
        "arguments": []
    });

    let req_line = req.to_string() + "\n";
    stdin.write_all(req_line.as_bytes()).await.unwrap();
    stdin.flush().await.unwrap();
    drop(stdin);

    let mut reader = BufReader::new(stdout).lines();
    let mut lines = Vec::new();
    while let Ok(Some(line)) = reader.next_line().await {
        lines.push(line);
    }

    assert_eq!(lines.len(), 1);
    let resp: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    assert_eq!(resp["type"], "final");
    let msg = resp["message"].as_str().unwrap();

    // Verify compliance rules for output HTML:
    // 1. Never use <br>, <p>, <div>, <h1>, <ul>, or <li>.
    assert!(!msg.contains("<br"), "HTML message should not contain <br> tags: {}", msg);
    assert!(!msg.contains("<p"), "HTML message should not contain <p> tags: {}", msg);
    assert!(!msg.contains("<div"), "HTML message should not contain <div> tags: {}", msg);
    assert!(!msg.contains("<h1"), "HTML message should not contain <h1> tags: {}", msg);
    assert!(!msg.contains("<ul"), "HTML message should not contain <ul> tags: {}", msg);
    assert!(!msg.contains("<li"), "HTML message should not contain <li> tags: {}", msg);

    // 2. Bullets should be Unicode bullets, not markdown hyphens/asterisks
    assert!(msg.contains("•"), "HTML message should contain Unicode bullet points: {}", msg);
    assert!(!msg.contains(" - "), "HTML message should not contain raw Markdown hyphens: {}", msg);

    let status = child.wait().await.unwrap();
    assert!(status.success());
    let _ = std::fs::remove_dir_all(temp_dir);
}
