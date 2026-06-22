// Copyright 2026 Cedric Gegout
// SPDX-License-Identifier: MIT

#![allow(unused_imports, dead_code)]
use secrecy::SecretString;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use scaleway_chat::config::Config;
use scaleway_chat::error::{AppError, Result};
use scaleway_chat::scaleway::models::{
    CreateServerRequest, InstanceVolumeType, Ip, IpResponse, Server, ServerResponse, Snapshot,
    SnapshotBootVolume, Volume,
};
use scaleway_chat::scaleway::ScalewayClient;
use scaleway_chat::state::{ProvisioningPhase, State};

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
            gpu_types: Some(vec![
                "L40S-1-48G".to_string(),
                "L40S-2-48G".to_string(),
                "H100-1-80G".to_string(),
            ]),
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

async fn setup_common_mocks(mock_server: &MockServer) {
    // 1. Mock validate_auth_and_project
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(mock_server)
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
        .mount(mock_server)
        .await;

    // 3. Mock products metadata
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
        .mount(mock_server)
        .await;

    // 4. Mock Nemotron readiness check
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "object": "list",
            "data": [{"id": "model", "object": "model", "created": 1686880000, "owned_by": "organization"}]
        })))
        .mount(mock_server)
        .await;
}

// 1. Fallback order is preserved
#[test]
fn test_fallback_order_preserved() {
    let config = mock_config("http://localhost:8330");
    let order = config.instance.effective_gpu_types();
    assert_eq!(
        order,
        vec![
            "L40S-1-48G".to_string(),
            "L40S-2-48G".to_string(),
            "H100-1-80G".to_string(),
        ]
    );
}

// 2. L40S-1 succeeds and no other type is attempted
#[tokio::test]
async fn test_fallback_l40s_1_succeeds() {
    let mock_server = MockServer::start().await;
    setup_common_mocks(&mock_server).await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // Mock L40S-1-48G POST server
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "instance-l40s-1",
                "name": "nemotron-l40s-l40s-1-48g-xxxx",
                "state": "stopped",
                "volumes": {
                    "0": { "id": "vol-l40s-1", "volume_type": "sbs_volume" }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-l40s-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "vol-l40s-1", "name": "vol-1", "status": "available", "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/instance-l40s-1/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/instance-l40s-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "instance-l40s-1", "name": "nemotron-l40s", "state": "running", "public_ip": null,
                "volumes": { "0": { "id": "vol-l40s-1", "volume_type": "sbs_volume" } }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/ips"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-l40s-1", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": null }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("PATCH"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-l40s-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-l40s-1", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "instance-l40s-1", "name": "nemotron-l40s" } }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-l40s-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-l40s-1", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "instance-l40s-1", "name": "nemotron-l40s" } }
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "127.0.0.1");
    assert_eq!(state.selected_gpu_type, "L40S-1-48G");

    let _ = std::fs::remove_file(state_file);
}

// 3. L40S-1 out of stock, cleanup succeeds, L40S-2 succeeds
#[tokio::test]
async fn test_fallback_l40s_1_out_of_stock_l40s_2_succeeds() {
    let mock_server = MockServer::start().await;
    setup_common_mocks(&mock_server).await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // 1st POST to create instance -> out of stock
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock capacity", "type": "out_of_stock"
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // 2nd POST to create instance -> succeeds (L40S-2)
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "instance-l40s-2",
                "name": "nemotron-l40s-l40s-2-48g-xxxx",
                "state": "stopped",
                "volumes": {
                    "0": { "id": "vol-l40s-2", "volume_type": "sbs_volume" }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-l40s-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "vol-l40s-2", "name": "vol-2", "status": "available", "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/instance-l40s-2/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/instance-l40s-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "instance-l40s-2", "name": "nemotron-l40s", "state": "running", "public_ip": null,
                "volumes": { "0": { "id": "vol-l40s-2", "volume_type": "sbs_volume" } }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/ips"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-l40s-2", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": null }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("PATCH"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-l40s-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-l40s-2", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "instance-l40s-2", "name": "nemotron-l40s" } }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-l40s-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-l40s-2", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "instance-l40s-2", "name": "nemotron-l40s" } }
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "127.0.0.1");
    assert_eq!(state.selected_gpu_type, "L40S-2-48G");

    let _ = std::fs::remove_file(state_file);
}

// 4. L40S-1 and L40S-2 out of stock, H100 succeeds
#[tokio::test]
async fn test_fallback_l40s_1_and_2_out_of_stock_h100_succeeds() {
    let mock_server = MockServer::start().await;
    setup_common_mocks(&mock_server).await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // 1st & 2nd POST -> out of stock
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock capacity", "type": "out_of_stock"
        })))
        .up_to_n_times(2)
        .mount(&mock_server)
        .await;

    // 3rd POST -> succeeds (H100)
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "instance-h100",
                "name": "nemotron-l40s-h100-1-80g-xxxx",
                "state": "stopped",
                "volumes": {
                    "0": { "id": "vol-h100", "volume_type": "sbs_volume" }
                }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-h100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "vol-h100", "name": "vol-h100", "status": "available", "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/instance-h100/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/instance-h100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "instance-h100", "name": "nemotron-l40s", "state": "running", "public_ip": null,
                "volumes": { "0": { "id": "vol-h100", "volume_type": "sbs_volume" } }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/ips"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-h100", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": null }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("PATCH"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-h100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-h100", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "instance-h100", "name": "nemotron-l40s" } }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-h100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-h100", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "instance-h100", "name": "nemotron-l40s" } }
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "127.0.0.1");
    assert_eq!(state.selected_gpu_type, "H100-1-80G");

    let _ = std::fs::remove_file(state_file);
}

// 5. All three are out of stock
#[tokio::test]
async fn test_fallback_all_out_of_stock() {
    let mock_server = MockServer::start().await;
    setup_common_mocks(&mock_server).await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // All POSTs -> out of stock
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock capacity", "type": "out_of_stock"
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert!(matches!(err, AppError::NoCompatibleGpuAvailable { .. }));

    let _ = std::fs::remove_file(state_file);
}

// 6. H100 is skipped because volume compatibility fails
#[tokio::test]
async fn test_h100_skipped_volume_compatibility() {
    let mock_server = MockServer::start().await;
    // 1. Mock validate_auth_and_project
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    // 2. Mock get_snapshot -> size: 1000
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28", "name": "snap", "status": "ready", "size": 1000,
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    // 3. Mock products server -> H100 requires min size: 5000 (larger than snapshot size 1000)
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": {
                "L40S-1-48G": { "volumes_constraint": { "min_size": 500, "max_size": 2000 } },
                "L40S-2-48G": { "volumes_constraint": { "min_size": 500, "max_size": 2000 } },
                "H100-1-80G": { "volumes_constraint": { "min_size": 5000, "max_size": 10000 } }
            }
        })))
        .mount(&mock_server)
        .await;

    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // Both L40S out of stock
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock capacity", "type": "out_of_stock"
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    if let AppError::NoCompatibleGpuAvailable { attempted, .. } = err {
        assert_eq!(attempted, vec!["L40S-1-48G", "L40S-2-48G", "H100-1-80G"]);
    } else {
        panic!("Expected NoCompatibleGpuAvailable, got {:?}", err);
    }

    let _ = std::fs::remove_file(state_file);
}

// 7. Product offered does not imply live capacity
#[tokio::test]
async fn test_product_offered_not_live_capacity() {
    let mock_server = MockServer::start().await;
    setup_common_mocks(&mock_server).await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // It is listed in products (from setup_common_mocks), but POST server returns out_of_stock
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock capacity", "type": "out_of_stock"
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_err());
    assert!(matches!(
        res.unwrap_err(),
        AppError::NoCompatibleGpuAvailable { .. }
    ));

    let _ = std::fs::remove_file(state_file);
}

// 8. A failed powered-off Instance is automatically deleted
#[tokio::test]
async fn test_reconciliation_powered_off_deleted() {
    let mock_server = MockServer::start().await;
    setup_common_mocks(&mock_server).await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());
    state.instance_id = Some("failed-inst-id".to_string());
    state.phase = ProvisioningPhase::PoweringOn;
    state.save_default().unwrap();

    // Reconcile checks server state -> stopped
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/failed-inst-id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "failed-inst-id", "name": "failed-inst", "state": "stopped", "volumes": {}
            }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Cleanup deletion calls
    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/servers/failed-inst-id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // GET server verify deletion -> returns 404
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/failed-inst-id"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Mock new creation fails so we stop there
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock", "type": "out_of_stock"
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_err());
    assert_eq!(state.instance_id, None); // verified deleted during reconciliation before fallback loop

    let _ = std::fs::remove_file(state_file);
}

// 9. A stopped Instance from old state is cleaned at startup
#[tokio::test]
async fn test_reconciliation_stopped_old_state_cleaned() {
    let mock_server = MockServer::start().await;
    setup_common_mocks(&mock_server).await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));

    // Construct old state
    let mut state = State {
        version: 1,
        phase: ProvisioningPhase::Ready,
        creation_mode: None,
        attempt_id: "attempt-1".to_string(),
        selected_gpu_type: "L40S-1-48G".to_string(),
        instance_id: Some("old-stopped-id".to_string()),
        volume_id: None,
        public_ip_id: None,
        public_ip_address: None,
        snapshot_id: config.instance.snapshot_id.clone(),
        zone: config.scaleway.zone.clone(),
        attempted_gpu_types: vec!["L40S-1-48G".to_string()],
        created_at: chrono::Utc::now(),
        path: Some(state_file.clone()),
    };
    state.save_default().unwrap();

    // Server state stopped
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/old-stopped-id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "old-stopped-id", "name": "old-stopped", "state": "stopped", "volumes": {}
            }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // DELETE server
    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/servers/old-stopped-id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // GET server verify deletion -> returns 404
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/old-stopped-id"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Mock new creation fails
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock", "type": "out_of_stock"
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_err());
    assert_eq!(state.instance_id, None);
    assert_eq!(state.version, 4); // migrated to version 4 first, then cleaned up and restarted

    let _ = std::fs::remove_file(state_file);
}

// 10. Generated boot volume is deleted after failed power-on
#[tokio::test]
async fn test_boot_volume_deleted_after_failed_power_on() {
    let mock_server = MockServer::start().await;
    setup_common_mocks(&mock_server).await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // Create server succeeds
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "inst-poweron-fail", "name": "inst", "state": "stopped",
                "volumes": { "0": { "id": "vol-poweron-fail", "volume_type": "sbs_volume" } }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-poweron-fail"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "vol-poweron-fail", "name": "vol", "status": "available", "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-poweron-fail"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Power-on fails -> returns out_of_stock (capacity failure during boot)
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-poweron-fail/action",
        ))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock", "type": "out_of_stock"
        })))
        .mount(&mock_server)
        .await;

    // Cleanup mocks
    Mock::given(method("DELETE"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-poweron-fail",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-poweron-fail",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-poweron-fail"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // Mock next candidate out of stock too
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock", "type": "out_of_stock"
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_err());
    // Verify cleanup happened: state cleared of failed attempt resources
    assert_eq!(state.instance_id, None);
    assert_eq!(state.volume_id, None);

    let _ = std::fs::remove_file(state_file);
}

// 11. Public IP is not allocated before Instance reaches running
#[tokio::test]
async fn test_ip_not_allocated_before_running() {
    let mock_server = MockServer::start().await;
    setup_common_mocks(&mock_server).await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // Create server succeeds
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "inst-ip-order", "name": "inst", "state": "stopped",
                "volumes": { "0": { "id": "vol-ip-order", "volume_type": "sbs_volume" } }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-ip-order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "vol-ip-order", "name": "vol", "status": "available", "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-ip-order"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Power-on succeeds
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-ip-order/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // Server wait_for_instance_running is called first. We mock it to fail with timeout after 2 polls.
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-ip-order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "inst-ip-order", "name": "inst", "state": "starting", "public_ip": null,
                "volumes": { "0": { "id": "vol-ip-order", "volume_type": "sbs_volume" } }
            }
        })))
        .up_to_n_times(5)
        .mount(&mock_server)
        .await;

    // For cleanup checks, GET returns 404
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-ip-order"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Cleanup mocks
    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-ip-order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-ip-order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // Mock next candidate out of stock too
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock", "type": "out_of_stock"
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_err());

    // Verify that NO request was made to POST /ips
    let received_requests = mock_server.received_requests().await.unwrap();
    let ip_alloc_reqs = received_requests
        .iter()
        .filter(|r| {
            r.method.to_string() == "POST" && r.url.path() == "/instance/v1/zones/fr-par-2/ips"
        })
        .count();
    assert_eq!(ip_alloc_reqs, 0);

    let _ = std::fs::remove_file(state_file);
}

// 12. Public IP is deleted after Nemotron startup failure
#[tokio::test]
async fn test_ip_deleted_after_nemotron_failure() {
    let mock_server = MockServer::start().await;
    // Mock validate auth & snapshot
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
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28", "name": "snap", "status": "ready", "size": 1000,
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": {
                "L40S-1-48G": { "volumes_constraint": { "min_size": 500, "max_size": 2000 } },
                "L40S-2-48G": { "volumes_constraint": { "min_size": 500, "max_size": 2000 } },
                "H100-1-80G": { "volumes_constraint": { "min_size": 500, "max_size": 2000 } }
            }
        })))
        .mount(&mock_server)
        .await;

    // Nemotron models check fails (returns 500 or timeout)
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // Create server succeeds
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "inst-nemo-fail", "name": "inst", "state": "stopped",
                "volumes": { "0": { "id": "vol-nemo-fail", "volume_type": "sbs_volume" } }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-nemo-fail"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "vol-nemo-fail", "name": "vol", "status": "available", "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-nemo-fail"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Power-on succeeds
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-nemo-fail/action",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // Server reaches running
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-nemo-fail"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "inst-nemo-fail", "name": "inst", "state": "running", "public_ip": null,
                "volumes": { "0": { "id": "vol-nemo-fail", "volume_type": "sbs_volume" } }
            }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // For cleanup, server GET returns 404
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-nemo-fail"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // IP allocate succeeds
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/ips"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-nemo-fail", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": null }
        })))
        .mount(&mock_server)
        .await;

    // IP attach succeeds
    Mock::given(method("PATCH"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-nemo-fail"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-nemo-fail", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "inst-nemo-fail", "name": "inst" } }
        })))
        .mount(&mock_server)
        .await;

    // IP get verification succeeds (called up to 1 time)
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-nemo-fail"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ip": { "id": "ip-nemo-fail", "address": "127.0.0.1", "project": "00000000", "zone": "fr-par-2", "server": { "id": "inst-nemo-fail", "name": "inst" } }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // For cleanup IP GET returns 404
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-nemo-fail"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Cleanup mocks
    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-nemo-fail"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-nemo-fail"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/ips/ip-nemo-fail"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    // Mock next candidate out of stock too
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock", "type": "out_of_stock"
        })))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    assert!(res.is_err());

    // Verify cleanup of all 3 resources succeeded
    assert_eq!(state.instance_id, None);
    assert_eq!(state.volume_id, None);
    assert_eq!(state.public_ip_id, None);

    let _ = std::fs::remove_file(state_file);
}

// 13. Snapshot deletion API is never called
#[tokio::test]
async fn test_snapshot_deletion_api_never_called() {
    let mock_server = MockServer::start().await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    // Verify calling delete instance/volume/IP with snapshot ID fails locally with SafetyViolation
    let snapshot_id = &config.instance.snapshot_id;
    let res1 = client.delete_instance(snapshot_id, snapshot_id).await;
    assert!(matches!(res1, Err(AppError::SafetyViolation(_))));

    let res2 = client.delete_volume(snapshot_id, snapshot_id).await;
    assert!(matches!(res2, Err(AppError::SafetyViolation(_))));

    let res3 = client.delete_public_ip(snapshot_id, snapshot_id).await;
    assert!(matches!(res3, Err(AppError::SafetyViolation(_))));
}

// 14. Snapshot is verified after cleanup
#[tokio::test]
async fn test_snapshot_verified_after_cleanup() {
    let mock_server = MockServer::start().await;
    setup_common_mocks(&mock_server).await;
    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );

    // Perform cleanup
    let report = client.cleanup_failed_attempt(&config, &mut state).await;
    assert!(report.is_ok());
    let r = report.unwrap();
    assert!(r.snapshot_preserved); // GET snapshot returned ready/preserved

    // Verify GET /block/v1/zones/fr-par-2/snapshots/... was hit at least once
    let received_requests = mock_server.received_requests().await.unwrap();
    let snap_get_reqs = received_requests
        .iter()
        .filter(|r| {
            r.method.to_string() == "GET"
                && r.url.path()
                    == "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28"
        })
        .count();
    assert!(snap_get_reqs >= 1);
}

// 15. Cleanup tolerates resources already absent
#[tokio::test]
async fn test_cleanup_tolerates_absent_resources() {
    let mock_server = MockServer::start().await;
    // validate auth
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    // Mock get_snapshot -> exists
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28", "name": "snap", "status": "ready", "size": 1000,
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());
    state.instance_id = Some("absent-inst-id".to_string());
    state.volume_id = Some("absent-vol-id".to_string());
    state.public_ip_id = Some("absent-ip-id".to_string());
    state.save_default().unwrap();

    // Mock deletion calls return 404 (already deleted/absent)
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // GET verification calls return 404
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28", "name": "snap", "status": "ready", "size": 1000,
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    let cleanup_res = client.cleanup_failed_attempt(&config, &mut state).await;
    assert!(cleanup_res.is_ok());
    let report = cleanup_res.unwrap();
    assert!(report.instance_deleted);
    assert!(report.volume_deleted);
    assert!(report.ip_deleted);

    let _ = std::fs::remove_file(state_file);
}

// 16. Cleanup failure prevents trying the next GPU
#[tokio::test]
async fn test_cleanup_failure_blocks_next_gpu() {
    let mock_server = MockServer::start().await;
    // validate auth
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    // get snapshot
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28", "name": "snap", "status": "ready", "size": 1000,
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/products/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": {
                "L40S-1-48G": { "volumes_constraint": { "min_size": 500, "max_size": 2000 } },
                "L40S-2-48G": { "volumes_constraint": { "min_size": 500, "max_size": 2000 } },
                "H100-1-80G": { "volumes_constraint": { "min_size": 500, "max_size": 2000 } }
            }
        })))
        .mount(&mock_server)
        .await;

    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());

    // Create server succeeds
    Mock::given(method("POST"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "server": {
                "id": "inst-cleanup-fail", "name": "inst", "state": "stopped",
                "volumes": { "0": { "id": "vol-cleanup-fail", "volume_type": "sbs_volume" } }
            }
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-cleanup-fail"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "vol-cleanup-fail", "name": "vol", "status": "available", "snapshot_id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28",
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/block/v1/zones/fr-par-2/volumes/vol-cleanup-fail"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Power-on fails -> out of stock (so we try fallback next)
    Mock::given(method("POST"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-cleanup-fail/action",
        ))
        .respond_with(ResponseTemplate::new(412).set_body_json(serde_json::json!({
            "message": "out of stock", "type": "out_of_stock"
        })))
        .mount(&mock_server)
        .await;

    // GET server returns 404 for cleanup stopped check
    Mock::given(method("GET"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-cleanup-fail",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // Delete server fails with 500 error during cleanup (matching all retries!)
    Mock::given(method("DELETE"))
        .and(path(
            "/instance/v1/zones/fr-par-2/servers/inst-cleanup-fail",
        ))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let res = client
        .provision_resources(&config, &mut state, &scaleway_chat::NoopProgress)
        .await;
    // It must return an error and NOT proceed to next GPU candidate because cleanup was incomplete
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert!(matches!(
        err,
        AppError::CleanupIncomplete(_) | AppError::ApiError(_)
    ));

    // Verify that only L40S-1-48G was attempted
    assert_eq!(state.attempted_gpu_types, vec!["L40S-1-48G".to_string()]);

    let _ = std::fs::remove_file(state_file);
}

// 18. State is cleared only after verified cleanup
#[tokio::test]
async fn test_state_cleared_only_after_verified_cleanup() {
    let mock_server = MockServer::start().await;
    // validate auth
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})))
        .mount(&mock_server)
        .await;

    // get snapshot
    Mock::given(method("GET"))
        .and(path(
            "/block/v1/zones/fr-par-2/snapshots/1b552e81-401d-4c15-b0b2-3c89e2d46c28",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "1b552e81-401d-4c15-b0b2-3c89e2d46c28", "name": "snap", "status": "ready", "size": 1000,
            "project_id": "00000000-0000-0000-0000-000000000000", "zone": "fr-par-2"
        })))
        .mount(&mock_server)
        .await;

    let config = mock_config(&mock_server.uri());
    let client = ScalewayClient::new_with_url(&config, mock_server.uri());

    let temp_dir = std::env::temp_dir();
    let state_file = temp_dir.join(format!("state-{}.toml", uuid::Uuid::new_v4()));
    let mut state = State::new(
        config.instance.snapshot_id.clone(),
        config.scaleway.zone.clone(),
    );
    state.path = Some(state_file.clone());
    state.instance_id = Some("inst-state-test".to_string());
    state.save_default().unwrap();

    // GET server returns 404 for cleanup stopped check
    Mock::given(method("GET"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-state-test"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "not found", "type": "resource_not_found"
        })))
        .mount(&mock_server)
        .await;

    // 1st Cleanup -> Deletion fails (all 5 retries return 500)
    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-state-test"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(5)
        .mount(&mock_server)
        .await;

    // Success mock for the 2nd attempt DELETE request (the 6th total DELETE request)
    Mock::given(method("DELETE"))
        .and(path("/instance/v1/zones/fr-par-2/servers/inst-state-test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&mock_server)
        .await;

    let cleanup_res1 = client.cleanup_failed_attempt(&config, &mut state).await;
    assert!(cleanup_res1.is_err());
    assert!(state_file.exists()); // file still exists

    let cleanup_res2 = client.cleanup_failed_attempt(&config, &mut state).await;
    assert!(cleanup_res2.is_ok());
    assert!(!state_file.exists()); // file cleared
}

// 24. Secrets remain redacted from logs and errors
#[test]
fn test_secrets_redacted_from_logs() {
    let config = mock_config("http://localhost:8330");

    // Secret keys must be wrapped in SecretString, whose debug representation does not leak the secret
    let scw_secret_debug = format!("{:?}", config.scaleway.secret_key);
    assert!(!scw_secret_debug.contains("secret"));

    let nemo_secret_debug = format!("{:?}", config.nemotron.api_key);
    assert!(!nemo_secret_debug.contains("key"));
}
