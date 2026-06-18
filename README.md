<!--
Copyright 2026 Cedric Gegout
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
-->

# scaleway-chat

`scaleway-chat` is a production-quality, high-reliability Rust CLI tool designed to provision, orchestrate, and clean up high-performance GPU resources on Scaleway for interactive, streamed LLM inference sessions using NVIDIA Nemotron. 

The application assumes that the booted GPU instance utilizes the **Canonical Inference Snap** to host the Nemotron LLM. This snap automatically starts up on boot and exposes an OpenAI-compatible API endpoint over HTTP (on port `8330` by default), which `scaleway-chat` connects to for streaming conversation completions.

---

## 💡 Motivations & Design Philosophy

High-performance GPU instances like Scaleway’s `L40S-1-48G` are powerful machines that command significant hourly costs. Spinning up these resources manually through the console is time-consuming and error-prone, involving:
1. Allocating a flexible IP address.
2. Restoring a large boot volume from a configured golden snapshot.
3. Creating the virtual server with correct specs.
4. Hooking up the IP, server, and volume.
5. Waiting for the operating system and LLM server to initialize.
6. Remembering to delete all three individual resources (IP, Volume, Server) upon termination to prevent stray hourly costs.

`scaleway-chat` was created to solve these exact problems. It acts as an automated orchestrator and an interactive chat terminal:
- **Cost Minimization**: Automates resource allocation only when you need it and guarantees deep-cleaning teardown.
- **State Hardening & Recovery**: Uses atomic local state files to preserve session progress. If your network fails, your computer reboots, or a crash occurs, relaunching the tool resumes exactly where it left off, avoiding resource duplication or orphans.
- **Safety Invariant**: Strictly guarantees that your original "Golden Snapshot" remains untouched, with absolute runtime barriers protecting it from deletion.

---

## 🛠 Architecture & Implementation

`scaleway-chat` is designed with a modular, library-first architecture (`src/lib.rs`), splitting the core orchestration engine from the interactive CLI entrypoint (`src/main.rs`).

```
Read & Validate Config
         ↓
Verify IAM Permissions & Project ID
         ↓
Verify Golden Snapshot Existence & Capacity
         ↓
Restore Volume from Snapshot (or adopt existing)
         ↓
Allocate Public IP (or adopt existing)
         ↓
Create & Power On GPU Server (or adopt existing)
         ↓
Wait for System Ready & Poll Nemotron API Status
         ↓
Interactive CLI Streamed Chat REPL
```

### Module Layout & Roles

- **[`src/config.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/config.rs)**:
  - Parses configuration from TOML files.
  - Enforces restrictive file permissions, printing warnings if the config file's mode is broader than `0600` (readable by owner only).
  - Performs structural and value validation: checks UUIDs, string constraints, positive timeouts, port boundaries, and temperature ranges (`0.0` to `2.0`).
- **[`src/state.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/state.rs)**:
  - Implements atomic writes to track runtime resource IDs. Saves to a temporary file, calls `sync_all` to flush writes, sets permissions to `0600`, and performs an atomic rename swap.
  - Manages crash-recovery state-adopting: on startup, adopts any resources created in previous runs and checks if they still exist on Scaleway.
- **[`src/scaleway/`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/scaleway/)**:
  - **[`client.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/scaleway/client.rs)**: Holds the `reqwest` client, project IDs, credentials, and implements HTTP request retries using **bounded exponential backoff with jitter** for transient error codes (`429 Too Many Requests`, `5xx` server issues). Maps HTTP errors to custom `AppError` types.
  - **[`auth.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/scaleway/auth.rs)**: Validates Project and Token credentials.
  - **[`snapshots.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/scaleway/snapshots.rs)**: Validates that the requested snapshot exists, is ready, and is in the configured zone.
  - **[`volumes.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/scaleway/volumes.rs)**: Handles volume creation and polling.
  - **[`ips.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/scaleway/ips.rs)**: Manages flexible IPv4 addresses.
  - **[`instances.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/scaleway/instances.rs)**: Manages server creation, actions, power status, and contains strict runtime assertions ensuring the Golden Snapshot ID is never targeted for deletion.
- **[`src/nemotron/`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/nemotron/)**:
  - **[`client.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/nemotron/client.rs)**: Polls the inference server's model list endpoint to confirm model readiness.
  - **[`stream.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/nemotron/stream.rs)**: Implements streaming Server-Sent Events (SSE) parsing. Buffers partial packet chunks, filters line comments (e.g. `: ping`), ignores empty heartbeats, and extracts model response deltas.
  - **[`chat.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/nemotron/chat.rs)**: Implements the main REPL (Read-Eval-Print Loop), managing session context and executing slash commands.
- **[`src/cli.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/cli.rs)**:
  - Parses commands and arguments using `clap` structure-derive.
- **[`src/error.rs`](file:///home/cgegout/Documents/Antigravity/scaleway-chat/src/error.rs)**:
  - Structured enum for application errors using `thiserror`.

---

## 🔒 Security: Plain HTTP Warning

> [!WARNING]
> The Nemotron API endpoint currently runs over plain HTTP (port `8330`). The Bearer API keys and conversation contents are transmitted in plain text over the network.
>
> **Recommendation**: To secure data, execute the CLI on a trusted local network, configure a VPN/VPC tunnel to the Scaleway instance, or wrap the remote port in an SSH tunnel / HTTPS reverse proxy.

---

## ⚙️ Configuration File Format

Before running the application, prepare a configuration file in `~/.config/scaleway-chat/config.toml` (or use a custom file path with `--config`).

```toml
[scaleway]
access_key = "SCWXXXXXXXXXXXXXXXXX"
secret_key = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
project_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
organization_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
zone = "fr-par-2"

[instance]
name = "nemotron-l40s"
instance_type = "L40S-1-48G"
snapshot_id = "1b552e81-401d-4c15-b0b2-3c89e2d46c28"
public_ip = "new"

[nemotron]
port = 8330
api_key = "REPLACE_WITH_NEMOTRON_API_KEY"
model = "nemotron-3-nano-30b-a3b-q4-k-m"
max_tokens = 4096
temperature = 0.7
system_prompt = "You are a helpful, concise assistant."

[timeouts]
instance_creation_seconds = 1200
instance_poll_interval_seconds = 10
nemotron_startup_seconds = 1200
nemotron_poll_interval_seconds = 10

[logging]
verbose = true
```

---

## 🚀 CLI Commands & Usage

### Global Arguments
- `--config <PATH>`: Explicit path to configuration TOML file (defaults to `~/.config/scaleway-chat/config.toml`).
- `--verbose`: Enable debug messages.
- `--no-color`: Disable styling and color codes in stdout log output.

### 1. `run` (Default)
Orchestrates or resumes the GPU infrastructure deployment, waits for completion, and enters the interactive REPL.
```bash
# Deploy or resume infrastructure and enter chat
scaleway-chat run
```
**Example output during startup:**
```text
INFO [Config] Reading /home/cgegout/.config/scaleway-chat/config.toml
INFO [Config] Configuration valid
INFO [Scaleway] Authenticating and validating project access...
INFO [Scaleway] Snapshot is ready (size: 200 GB).
INFO [Scaleway] Creating GPU Instance directly from snapshot...
INFO [Scaleway] Instance created: nemotron-l40s (36c32cb1-9ab9-4b56-ab90-4b892a88fa2e)
INFO [Scaleway] Boot volume created from snapshot.
INFO [Scaleway] Boot volume ID: d2c3049a-4c46-4f61-8ca6-f9a5cf8a4681
INFO [Scaleway] Allocating public IP...
INFO [Scaleway] Public IP allocated: 51.159.155.147
INFO [Scaleway] Attaching public IP to server...
INFO [Scaleway] Powering on Instance...
INFO [Scaleway] Instance state: starting
...
Waiting for Nemotron service to start (connection refused)
...
Nemotron is ready.

Connected to nemotron-3-nano-30b-a3b-q4-k-m
Endpoint: http://51.159.155.147:8330/v1
Commands: /clear, /status, /kill, /exit

You: 
```

### 2. `status`
Inspects currently active runtime resources, comparing your local state file with their remote status on Scaleway.
```bash
scaleway-chat status
```
**Example output:**
```text
INFO [Config] Reading /home/cgegout/.config/scaleway-chat/config.toml
INFO [Config] Configuration valid
--- Local Configuration ---
Zone: fr-par-2
Project ID: 31ad702e-94e1-47d0-b2b0-09a71c71ffdf
Target Instance Name: nemotron-l40s
Target Instance Type: L40S-1-48G
Source Snapshot ID: 1b552e81-401d-4c15-b0b2-3c89e2d46c28

--- Local State File ---
State file path: /home/cgegout/.local/state/scaleway-chat/state.toml
Tracked Instance ID: Some("36c32cb1-9ab9-4b56-ab90-4b892a88fa2e")
Tracked Volume ID: Some("d2c3049a-4c46-4f61-8ca6-f9a5cf8a4681")
Tracked IP ID: Some("72851cc0-283c-42d4-8a53-7925e8e51b29")
Tracked IP Address: Some("51.159.155.147")

--- Remote Scaleway Status ---
Instance (36c32cb1-9ab9-4b56-ab90-4b892a88fa2e): state is 'running'
Volume (d2c3049a-4c46-4f61-8ca6-f9a5cf8a4681): status is 'available'
IP (72851cc0-283c-42d4-8a53-7925e8e51b29): address is '51.159.155.147'
```

### 3. `validate-config`
Performs syntax checks, validation constraints, and calls Scaleway's server list API to verify your credentials without spinning up resources.
```bash
scaleway-chat validate-config
```
**Example output:**
```text
INFO [Config] Reading /home/cgegout/.config/scaleway-chat/config.toml
INFO [Config] Configuration valid
INFO [Scaleway] Authenticating...
INFO [Scaleway] Authentication successful. Project verified.
INFO [Scaleway] Validating snapshot...
INFO [Scaleway] Snapshot is ready (size: 200 GB).
INFO [Scaleway] Checking L40S-1-48G availability in fr-par-2...
INFO [Scaleway] Instance type L40S-1-48G is supported in zone.
Configuration and remote connection checks validated successfully.
```

### 4. `test-integration`
Runs live integration tests against the live Scaleway API, verifying authentication credentials, the availability of the `L40S-1-48G` commercial server type, and checking the status and capacity of the boot snapshot.
```bash
scaleway-chat test-integration
```
**Example output:**
```text
[Integration Test] Starting live Scaleway integration checks...
[Integration Test] 1. Validating authentication and project access...
[Integration Test] Authentication and project access are valid!
[Integration Test] 2. Checking instance type 'L40S-1-48G' availability...
[Integration Test] Instance type is supported in zone!
[Integration Test] 3. Fetching and validating snapshot '1b552e81-401d-4c15-b0b2-3c89e2d46c28'...
[Integration Test] Snapshot is found! Status: 'ready', Size: 200000000000 bytes, Zone: 'fr-par-2'
[Integration Test] Live integration checks completed successfully!
```

### 5. `kill`
Frees and deletes all provisioned resources (Server, restored Block Volume, flexible IP) tracked in the state file.
```bash
scaleway-chat kill
```
**Example output:**
```text
INFO [Config] Reading /home/cgegout/.config/scaleway-chat/config.toml
INFO [Config] Configuration valid

This will power off and permanently delete the GPU Instance,
its restored Block Storage volume, and its allocated public IP.

The source snapshot will be preserved.

Type KILL to continue: KILL
INFO [Cleanup] Stopping and deleting Instance 36c32cb1-9ab9-4b56-ab90-4b892a88fa2e...
INFO [Cleanup] Powering off Instance...
INFO [Cleanup] Power-off action sent.
INFO [Cleanup] Instance stopped.
INFO [Cleanup] Deleting Instance 36c32cb1-9ab9-4b56-ab90-4b892a88fa2e...
INFO [Cleanup] Instance deleted.
INFO [Cleanup] Deleting restored Block Storage volume d2c3049a-4c46-4f61-8ca6-f9a5cf8a4681...
INFO [Scaleway] Deleting restored volume d2c3049a-4c46-4f61-8ca6-f9a5cf8a4681...
INFO [Scaleway] Volume deleted.
INFO [Cleanup] Deleting allocated public IP 72851cc0-283c-42d4-8a53-7925e8e51b29...
INFO [Scaleway] Deleting public IP 72851cc0-283c-42d4-8a53-7925e8e51b29...
INFO [Scaleway] Public IP deleted.
INFO [Cleanup] Verifying source snapshot...
INFO [Cleanup] Snapshot preserved: 1b552e81-401d-4c15-b0b2-3c89e2d46c28
INFO [Cleanup] Complete. GPU billing resources have been removed.
```

---

## 💬 Chat Loop Slash Commands

Inside the chat prompt, you can use the following commands:
- `/status`: Displays the status of the provisioned server, volume, IP address, and model.
- `/clear`: Clears conversation history (retains only the system prompt).
- `/exit`: Exits the terminal interface immediately. **Leaves GPU resources active** so you can reconnect later, but prints a warning about continuing billing.
- `/kill`: Clears history, exits, and **terminates all Scaleway resources** (forces teardown).

---

## 🏗 How to Build

### Prerequisites
Make sure you have Rust and Cargo installed:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Build CLI
Clone the repository and run:
```bash
# Debug build
cargo build

# Optimized release build
cargo build --release
```

### Installation
Move the compiled release binary to your local bin path:
```bash
install -Dm755 target/release/scaleway-chat ~/.local/bin/scaleway-chat
install -Dm600 config.example.toml ~/.config/scaleway-chat/config.toml
```

---

## 🧪 How it is Tested

`scaleway-chat` features a comprehensive test suite covering unit validation, state persistence, streaming decoders, and API client behaviors.

We use **`wiremock`** to mock the Scaleway HTTP API, allowing us to test provisioning lifecycles, recovery mechanics, and retry backoffs without spawning real billable servers.

### Running the Tests
Execute the following cargo command:
```bash
cargo test
```

### What is Covered
1. **SSE Streaming Parser**:
   - Split network packets and chunk boundaries.
   - CRLF and LF event line terminations.
   - Comment line filtering (lines starting with `:`).
   - Structured JSON API errors (raising the correct `AppError` type).
2. **Configuration Validator**:
   - Verification of mandatory field completeness.
   - Out-of-bounds validations (e.g. port limits, invalid zones, temperature range errors, non-positive timeouts).
   - Invalid UUID format checks for snapshot, project, and organization IDs.
3. **Safety Invariants**:
   - Explicit verification that calling volume/IP/instance deletion with the golden snapshot ID raises a safety panic/error rather than making a network call.
4. **State Persistence**:
   - Atomic temporary-file swapping during saves.
   - Hardened `0600` file permission checks.
   - Deserialization of valid/invalid TOML contents.
5. **API Client & Retry Policy**:
   - Status code error mapping.
   - Bounded exponential backoff and retry triggers on `429 Too Many Requests` (respecting `Retry-After` headers) and `5xx` server-side issues.
   - Structured JSON error detail extraction (e.g. invalid arguments formatting showing `argument_name`, `reason`, and `help_message` without hiding anything).
6. **Mock Provisioning Lifecycle**:
   - Authenticating, checking capacity, validating snapshots, and resuming from a state file.
7. **Idempotent Public IP Attachment**:
   - Corrected flexible IP attachment logic targeting `/instance/v1/zones/{zone}/ips/{ip_id}` using `PATCH` and a nested JSON request body `{"server": {"id": "<server_id>"}}`.
   - Covered 11 specific scenarios using `wiremock`:
     1. Existing unattached IP.
     2. Existing IP already attached to the target Instance.
     3. Existing IP attached to another Instance (yielding safe failure).
     4. Missing IP (raising a 404 error).
     5. Missing Instance (raising a 404 error).
     6. HTTP 400 malformed payload / schema validation error details.
     7. HTTP 409 conflict handling.
     8. Successful attachment.
     9. Verification of attachment after execution.
     10. Application resume from persisted state.
     11. No duplicate resource creation.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](file:///home/cgegout/Documents/Antigravity/scaleway-chat/LICENSE) file for details.
