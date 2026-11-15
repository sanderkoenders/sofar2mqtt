# Repository Guidelines

## Project Structure & Module Organization

The bridge is a Rust application in `src/`: `main.rs` coordinates startup and retries, `config.rs` loads YAML settings, `inverter.rs` handles SOFAR Modbus RTU frames, `mqtt.rs` owns MQTT topics and Home Assistant discovery, and `state.rs` persists the desired inverter command. `config.yaml` contains deployment settings. `sofar-state.json` is generated at runtime and should not be committed.

## Build, Test, and Development Commands

- `cargo fmt --check` checks Rust formatting; use `cargo fmt` to apply it.
- `cargo check` compiles the application without producing a release binary.
- `cargo test` runs unit and integration tests.
- `cargo run -- config.yaml` starts the bridge using the selected configuration file.
- `cargo build --release` creates an optimized binary under `target/release/`.

The application needs access to both the MQTT broker and the inverter. It does not require the inverter to be online to retain a desired command. Optional `MQTT_USERNAME` and `MQTT_PASSWORD` environment variables override credentials in YAML.

## Coding Style & Naming Conventions

Use idiomatic Rust, run `cargo fmt`, and keep protocol details, configuration, persistence, and MQTT behavior in their respective modules. Prefer explicit error context with `anyhow::Context`; do not log credentials or discard desired commands on transient inverter failures.

## Testing Guidelines

Add unit tests for pure logic such as CRC calculation, command parsing, configuration defaults, and state persistence. Keep tests independent of live hardware and brokers. Any manual inverter validation should be clearly identified as hardware-dependent.

## Commit & Pull Request Guidelines

Git history is unavailable in this checkout, so no repository-specific commit convention can be confirmed. Use short imperative commit subjects. Pull requests should describe behavior and configuration changes, link issues when relevant, and include test or validation results. Do not include credentials in descriptions or logs.

## Configuration & Operations

Configure the Home Assistant discovery topic root with `mqtt.homeassistant_prefix`; state is published under `mqtt.topic_prefix`. The state file stores the latest requested ON/OFF value and must live on persistent storage if it should survive restarts. Bridge availability reports the bridge's MQTT connection, not inverter availability.
