# sofar-mqtt

An asynchronous Rust bridge between a SOFAR inverter (Modbus RTU framing over TCP) and MQTT.

## Run

Install Rust, then build with `cargo build --release` and start with `cargo run --release -- config.yaml`. The YAML file is passed as the optional first argument. Set broker credentials with `MQTT_USERNAME` and `MQTT_PASSWORD` to override values in the file.

## Behavior

ON and OFF commands are persisted to `state_file` before an inverter write is attempted. The bridge retries the desired command on its polling interval and reports that desired command as MQTT state, so inverter downtime does not make the Home Assistant switch unavailable. When no state has been saved yet, the bridge seeds desired state from a recognized inverter command the first time it can read the inverter. The retained MQTT availability topic reports bridge-to-broker connectivity only.

The MQTT `topic_prefix` controls bridge state and command topics. `homeassistant_prefix` controls Home Assistant discovery topics and defaults to `homeassistant`. The older `discovery_prefix` YAML key remains accepted as an alias.

## Modules

- `src/config.rs`: YAML configuration and environment credential overrides.
- `src/inverter.rs`: Modbus frame handling, CRC validation, command writes, and verification.
- `src/state.rs`: durable desired-state file with atomic replacement.
- `src/mqtt.rs`: MQTT connection, discovery, command, availability, and state topics.
- `src/main.rs`: startup, event loop, polling, and retry coordination.
