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

## ⚡ GPU Fallback & Transactional Cleanup Policy

To maximize provisioning success rate while strictly containing costs and preventing leaked billing resources, `scaleway-chat` implements a transactional GPU fallback orchestration algorithm.

### 1. Ordered Fallback Candidates
When starting a session in the target Availability Zone (predefined as `fr-par-2` to match the source snapshot), the application attempts to provision instances using the following ordered fallback list:
1. `L40S-1-48G`
2. `L40S-2-48G`
3. `H100-1-80G`

If the configuration specifies `instance_type` (for backward compatibility), it is internally mapped to a single-element list. If `gpu_types` is specified, it takes precedence.

### 2. Same-Zone Constraint (`fr-par-2`)
Because the source snapshot is zone-bound and cannot be automatically copied or replaced, all fallback attempts strictly run within `fr-par-2`. If no compatible or available GPU can be successfully provisioned in `fr-par-2`, the process terminates.

### 3. No-Zombie Guarantee & Verified Deletion
To prevent orphaned resources from incurring ongoing costs, the application treats every provisioning attempt as a single transaction:
* **Creation Order**: The Instance is created directly from the snapshot, the generated boot volume ID is discovered, the instance is powered on, and only *after* it reaches the `running` state is a temporary public IP allocated and attached.
* **Verified Cleanup**: If any failure occurs during this flow (e.g. `out_of_stock` errors, power-on failures, connection timeouts, driver incompatibilities, or user interruption like `Ctrl+C`), the orchestrator halts, deletes all created resources (Instance, restored Boot Volume, temporary IP), and polls their respective GET endpoints until they return HTTP 404 (Not Found).
* **Cleanup Failures**: If cleanup cannot be verified within the timeout limit (`cleanup_timeout_seconds`), the application preserves the state file, prints the remaining active resource IDs, exits with a non-zero status, and asks for manual intervention. No further GPU fallback is attempted.

### 4. Product Offerings vs. Live Capacity
Before attempting a GPU candidate, the app queries the Scaleway Instance Products API to validate compatibility (supported volume types, architecture, minimum size constraint, and zone availability). However, *offering* a product in a zone does not guarantee live capacity. Live capacity is only confirmed when the Instance is successfully created and powered on.

### 5. Multi-GPU & Runtime Compatibility Caveats
* **L40S-2-48G**: This instance type provides two physical GPUs. The booted Canonical Inference Snap might only configure and utilize a single GPU. `scaleway-chat` does not automatically modify the snap configuration.
* **H100-1-80G Guest Runtime**: Even if the H100 satisfies all infrastructure and volume checks, the NVIDIA driver or CUDA version inside the snapshot may not support the H100 GPU. The application verifies guest compatibility by checking that the Nemotron `/v1/models` endpoint becomes ready. If it fails, the instance is torn down, and the failure is classified as guest-runtime incompatible.

### 6. Startup Reconciliation
If the application starts up and finds a local state file representing an incomplete, powered-off, stopped, or failed provisioning session from a previous run, it will:
1. Identify all remaining resources in that state.
2. Trigger the transactional cleanup to destroy them.
3. Verify their deletion.
4. Restart the ordered fallback algorithm from the first GPU type.
It will **never** adopt a powered-off or stalled instance for a new session.

### 7. Predefined Snapshot Safety
The golden source snapshot is immutable. The application:
* Contains no implementation or calls to the Scaleway snapshot deletion endpoint.
* Contains runtime assertions that reject deleting any resource sharing the ID of the source snapshot.
* Re-verifies that the source snapshot exists and remains ready after every cleanup cycle.

### 8. Billing Implications
* **Failed Provisioning**: Automatically cleaned up immediately; no billing resources are left behind.
* **Exit (`/exit`)**: The running instance is preserved by choice so the user can resume later. A warning message displays active billing implications.
* **Kill (`/kill`)**: Shuts down the REPL and triggers immediate verified teardown of all active session resources.

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
secret_key = "REPLACE_WITH_SECRET"
project_id = "REPLACE_WITH_PROJECT_UUID"
organization_id = "REPLACE_WITH_ORGANIZATION_UUID"
zone = "fr-par-2"

[instance]
name = "nemotron-l40s"
snapshot_id = "1b552e81-401d-4c15-b0b2-3c89e2d46c28"
public_ip = "new"

gpu_types = [
    "L40S-1-48G",
    "L40S-2-48G",
    "H100-1-80G",
]

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
cleanup_timeout_seconds = 300
cleanup_poll_interval_seconds = 5
nemotron_startup_seconds = 1200
nemotron_poll_interval_seconds = 10
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

## 🤖 HAL Integration

`scaleway-chat` supports a non-interactive subprocess protocol specifically designed to integrate with the `hal-ecosystem` bot.

### Architecture

```
Telegram User ──> HAL Bot ──(Subprocess stdin/stdout)──> scaleway-chat hal
```

When HAL runs in stdio transport mode, it executes:
```bash
scaleway-chat hal
```
It writes exactly one JSON request line to standard input, parses progress and final responses from standard output as NDJSON, and terminates. All logging, telemetry, and error diagnostics are safely written to `stderr` to keep `stdout` pure.

### Supported HAL Commands

- **`scaleway`**:
  - Joins the arguments with spaces to construct the query prompt.
  - Resolves, provisions, or adopts the required Scaleway resources (flexible IP, instance booted directly from snapshot).
  - Waits for OS boot and model readiness (Canonical Inference Snap status).
  - Sends a single inference request to the model, streams the response internally, and outputs a single final HTML event containing the complete answer.
  - Exits *without* deleting GPU resources. Emits a billing notice that the GPU remains active.
- **`scaleway_start`**:
  - Provisions or resumes GPU resources and waits until the inference endpoint is reachable.
  - Outputs a summary of instance name, power state, public IP, model name, and a billing warning.
  - Does not send an inference prompt.
- **`scaleway_status`**:
  - Concisely queries the status of the Golden Snapshot, GPU Instance, Boot Volume, Flexible IP, and Model readiness without provisioning or deleting anything.
- **`scaleway_kill`**:
  - Requires exactly one argument: `KILL`.
  - Stops and deletes the GPU instance, restored volume, and flexible IP, clearing local session state on successful teardown.
  - Strictly preserves the Golden Snapshot.
- **`scaleway_help`**:
  - Returns concise HTML-safe Telegram help info.

### NDJSON Protocol Examples

#### Request (stdin):
```json
{"request_id": "97e68bc6-9289-4d6f-870f-90e87dcd3e44", "command": "scaleway", "arguments": ["Explain", "Juju", "controllers"]}
```

#### Progress Event (stdout):
```json
{"type":"progress","request_id":"97e68bc6-9289-4d6f-870f-90e87dcd3e44","percent":30,"message":"Validating source snapshot...","format":"html"}
```

#### Final Event (stdout):
```json
{"type":"final","request_id":"97e68bc6-9289-4d6f-870f-90e87dcd3e44","format":"html","message":"Juju controllers manage... \n\n⚠️ <b>Billing Notice:</b> The GPU Instance remains active and will continue billing. Use <code>/scaleway_kill KILL</code> to terminate it when you are finished.","trusted_html":true}
```

#### Error Event (stdout):
```json
{"type":"error","request_id":"97e68bc6-9289-4d6f-870f-90e87dcd3e44","reason":"Capacity Unavailable","technical_details":"Out of L40S-1-48G instances in fr-par-2","suggested_action":"Please retry later or check Scaleway Console."}
```

### Installation & Deployment

To build and deploy the application and configure the launcher for HAL:

```bash
# 1. Clone and compile release binary
git clone https://github.com/gegout/scaleway-chat
cd scaleway-chat
cargo build --release

# 2. Install executable
mkdir -p /home/cgegout/bin
install -m 755 target/release/scaleway-chat /home/cgegout/bin/scaleway-chat

# 3. Create launcher script
cat > /home/cgegout/bin/scaleway-chat-hal <<'EOF'
#!/bin/sh
exec "$HOME/bin/scaleway-chat" hal
EOF

# 4. Make launcher executable
chmod 755 /home/cgegout/bin/scaleway-chat-hal
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](file:///home/cgegout/Documents/Antigravity/scaleway-chat/LICENSE) file for details.
