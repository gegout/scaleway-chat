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

use clap::Parser;
use std::io::Write;
use tracing::{error, info, warn};

use scaleway_chat::cli::{Cli, Commands};
use scaleway_chat::config::Config;
use scaleway_chat::error::Result;
use scaleway_chat::logging;
use scaleway_chat::nemotron::{chat::ChatAction, NemotronClient};
use scaleway_chat::scaleway::ScalewayClient;
use scaleway_chat::state::State;

#[tokio::main]
async fn main() {
    // Parse command line arguments using clap
    let args = Cli::parse();

    // Initialize the console logger. If verbose is enabled, debug-level logs will print.
    logging::init(args.verbose);

    // Load the configuration file. If a path is provided with --config, use it;
    // otherwise, fallback to the default location (~/.config/scaleway-chat/config.toml).
    let config_res = match args.config {
        Some(path) => Config::load_from_path(&path).map(|c| (c, path)),
        None => Config::load_default(),
    };

    let (config, config_path) = match config_res {
        Ok(c) => c,
        Err(e) => {
            error!("Configuration validation failed: {}", e);
            std::process::exit(1);
        }
    };

    info!("[Config] Reading {}", config_path.to_string_lossy());
    info!("[Config] Configuration valid");

    // Initialize the Scaleway API client with the loaded credentials
    let client = ScalewayClient::new(&config);

    // Default to the "run" subcommand if no command is specified on the command-line
    let cmd = args.command.unwrap_or(Commands::Run);

    // Execute the corresponding subcommand handler
    let run_result = match cmd {
        Commands::ValidateConfig => validate_config(&client, &config).await,
        Commands::TestIntegration => run_live_integration_test(&client, &config).await,
        Commands::Status => show_status(&client, &config).await,
        Commands::Kill => run_kill(&client, &config).await,
        Commands::Run => run_flow(&client, &config).await,
    };

    if let Err(e) = run_result {
        error!("Execution failed: {}", e);
        std::process::exit(1);
    }
}

/// Validates configuration parameters and tests remote Scaleway API connectivity.
/// This verifies project credentials, checks snapshot presence, and confirms GPU instance type support.
async fn validate_config(client: &ScalewayClient, config: &Config) -> Result<()> {
    info!("[Scaleway] Authenticating...");
    client.validate_auth_and_project().await?;

    info!("[Scaleway] Validating snapshot...");
    let snap = client.get_snapshot(&config.instance.snapshot_id).await?;
    info!(
        "[Scaleway] Snapshot is ready (size: {} GB).",
        snap.size / 1_000_000_000
    );

    client
        .validate_instance_type_available(&config.instance.instance_type)
        .await?;

    info!("Configuration and remote connection checks validated successfully.");
    Ok(())
}

async fn run_live_integration_test(client: &ScalewayClient, config: &Config) -> Result<()> {
    info!("[Integration Test] Starting live Scaleway integration checks...");

    info!("[Integration Test] 1. Validating authentication and project access...");
    client.validate_auth_and_project().await?;
    info!("[Integration Test] Authentication and project access are valid!");

    info!(
        "[Integration Test] 2. Checking instance type '{}' availability...",
        config.instance.instance_type
    );
    client
        .validate_instance_type_available(&config.instance.instance_type)
        .await?;
    info!("[Integration Test] Instance type is supported in zone!");

    info!(
        "[Integration Test] 3. Fetching and validating snapshot '{}'...",
        config.instance.snapshot_id
    );
    let snapshot = client.get_snapshot(&config.instance.snapshot_id).await?;
    info!(
        "[Integration Test] Snapshot is found! Status: '{}', Size: {} bytes, Zone: '{}'",
        snapshot.status, snapshot.size, snapshot.zone
    );

    info!("[Integration Test] Live integration checks completed successfully!");
    Ok(())
}

async fn show_status(client: &ScalewayClient, config: &Config) -> Result<()> {
    let state_opt = State::load_default()?;

    println!("\n--- Local Configuration ---");
    println!(
        "Config file: {:?}",
        Config::load_default().map(|(_, p)| p).unwrap_or_default()
    );
    println!("Zone: {}", config.scaleway.zone);
    println!("Project ID: {}", config.scaleway.project_id);
    println!("Target Instance Name: {}", config.instance.name);
    println!("Target Instance Type: {}", config.instance.instance_type);
    println!("Source Snapshot ID: {}", config.instance.snapshot_id);

    println!("\n--- Local State File ---");
    let state = match state_opt {
        Some(s) => s,
        None => {
            println!("No local state file found. No resources are currently tracked.");
            return Ok(());
        }
    };

    let state_path = State::default_path()?;
    println!("State file path: {}", state_path.to_string_lossy());
    println!("Tracked Instance ID: {:?}", state.instance_id);
    println!("Tracked Volume ID: {:?}", state.volume_id);
    println!("Tracked IP ID: {:?}", state.public_ip_id);
    println!("Tracked IP Address: {:?}", state.public_ip_address);
    println!("Created At: {}", state.created_at);

    println!("\n--- Remote Scaleway Status ---");

    if let Some(ref server_id) = state.instance_id {
        let path = format!("/instance/v1/zones/{}/servers/{}", client.zone, server_id);
        match client
            .request::<serde_json::Value, _>(reqwest::Method::GET, &path, |req| req)
            .await
        {
            Ok(v) => println!(
                "Instance ({}): state is '{}'",
                server_id,
                v["server"]["state"].as_str().unwrap_or("unknown")
            ),
            Err(_) => println!("Instance ({}): NOT found on Scaleway", server_id),
        }
    } else {
        println!("Instance: None");
    }

    if let Some(ref volume_id) = state.volume_id {
        let path = format!("/block/v1/zones/{}/volumes/{}", client.zone, volume_id);
        match client
            .request::<serde_json::Value, _>(reqwest::Method::GET, &path, |req| req)
            .await
        {
            Ok(v) => println!(
                "Volume ({}): status is '{}'",
                volume_id,
                v["status"].as_str().unwrap_or("unknown")
            ),
            Err(_) => println!("Volume ({}): NOT found on Scaleway", volume_id),
        }
    } else {
        println!("Volume: None");
    }

    if let Some(ref ip_id) = state.public_ip_id {
        let path = format!("/instance/v1/zones/{}/ips/{}", client.zone, ip_id);
        match client
            .request::<serde_json::Value, _>(reqwest::Method::GET, &path, |req| req)
            .await
        {
            Ok(v) => println!(
                "IP ({}): address is '{}'",
                ip_id,
                v["ip"]["address"].as_str().unwrap_or("unknown")
            ),
            Err(_) => println!("IP ({}): NOT found on Scaleway", ip_id),
        }
    } else {
        println!("Public IP: None");
    }

    Ok(())
}

async fn run_kill(client: &ScalewayClient, config: &Config) -> Result<()> {
    let state_opt = State::load_default()?;
    let state = match state_opt {
        Some(s) => s,
        None => {
            warn!("No active state file found. Nothing to clean up.");
            return Ok(());
        }
    };

    if confirm_kill() {
        client.perform_cleanup(config, state).await?;
    } else {
        println!("Cleanup cancelled.");
    }
    Ok(())
}

fn confirm_kill() -> bool {
    println!("\nThis will power off and permanently delete the GPU Instance,");
    println!("its restored Block Storage volume, and its allocated public IP.");
    println!("\nThe source snapshot will be preserved.");
    print!("\nType KILL to continue: ");
    let _ = std::io::stdout().flush();

    let mut confirmation = String::new();
    if std::io::stdin().read_line(&mut confirmation).is_ok() {
        confirmation.trim() == "KILL"
    } else {
        false
    }
}

// Local perform_cleanup removed, using ScalewayClient method instead.

/// Core orchestrator flow: loads or resumes state, provisions Scaleway GPU resources,
/// polls the inference endpoint until model loading is complete, and boots the REPL interface.
async fn run_flow(client: &ScalewayClient, config: &Config) -> Result<()> {
    // 1. Attempt to load an existing local state file (~/.local/state/scaleway-chat/state.toml)
    // to resume provisioning or connect to already active resources.
    let mut state = match State::load_default()? {
        Some(s) => {
            info!("[State] Found existing local state. Resuming or verifying resources...");
            s
        }
        None => State::new(
            config.instance.snapshot_id.clone(),
            config.scaleway.zone.clone(),
        ),
    };

    // Capture Ctrl-C signal to safely output tracked resource IDs on sudden exit
    let ctrl_c_signal = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c_signal);

    // 2. Provision resources (Server, boot volume, allocated IP, IP attachment, power on)
    // while listening for Ctrl-C termination.
    let provision_res = tokio::select! {
        res = client.provision_resources(config, &mut state) => res,
        _ = &mut ctrl_c_signal => {
            eprintln!("\n[Signal] Provisioning interrupted by Ctrl+C.");
            report_remaining_resources(&state);
            std::process::exit(130);
        }
    };

    let server_ip = provision_res?;

    // 3. Initialize Nemotron API client targeting the server's public IP on port 8330
    let nemotron_client = NemotronClient::new(
        server_ip.clone(),
        config.nemotron.port,
        config.nemotron.api_key.clone(),
        config.nemotron.model.clone(),
    );

    // 4. Poll Nemotron /v1/models endpoint until the inference server returns HTTP 200,
    // indicating that model weight loading is complete and the model is ready.
    let nemotron_wait_res = tokio::select! {
        res = nemotron_client.wait_for_ready(
            config.timeouts.nemotron_startup_seconds,
            config.timeouts.nemotron_poll_interval_seconds,
        ) => res,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("\n[Signal] Startup wait interrupted by Ctrl+C.");
            report_remaining_resources(&state);
            std::process::exit(130);
        }
    };

    nemotron_wait_res?;

    let state_report = format!(
        "Instance ID: {:?}\nVolume ID: {:?}\nPublic IP: {}",
        state.instance_id, state.volume_id, server_ip
    );

    // 5. Run the interactive chat REPL.
    // Handles /clear, /status, /exit (leave running), and /kill (destroy resources).
    match scaleway_chat::nemotron::chat::start_chat(&nemotron_client, config, &state_report).await?
    {
        ChatAction::Exit => {
            info!("Chat session ended. Resources are left running.");
            Ok(())
        }
        ChatAction::Kill => {
            // Initiate full teardown on user request
            client.perform_cleanup(config, state).await?;
            Ok(())
        }
    }
}

fn report_remaining_resources(state: &State) {
    eprintln!("\nTracked resources remaining active:");
    if let Some(ref vid) = state.volume_id {
        eprintln!("  - Volume: {}", vid);
    }
    if let Some(ref ipid) = state.public_ip_id {
        eprintln!("  - Public IP: {} ({:?})", ipid, state.public_ip_address);
    }
    if let Some(ref iid) = state.instance_id {
        eprintln!("  - Instance: {}", iid);
    }
    eprintln!("State file preserved for recovery. Run scaleway-chat status or kill to manage.");
}

// Local provision_resources removed, using ScalewayClient method instead.
