use anyhow::{Context, Result};
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub inverter: InverterConfig,
    pub mqtt: MqttConfig,
    #[serde(default = "default_state_file")]
    pub state_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InverterConfig {
    pub host: String,
    #[serde(default = "default_inverter_port")]
    pub port: u16,
    #[serde(default = "default_slave_id")]
    pub slave_id: u8,
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    #[serde(default)]
    pub active_power: ActivePowerConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ActivePowerConfig {
    #[serde(default = "default_power_address")]
    pub address: u16,
    #[serde(default = "default_power_function")]
    pub function: u8,
    #[serde(default = "default_power_scale")]
    pub scale: f64,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for ActivePowerConfig {
    fn default() -> Self {
        Self {
            address: 12,
            function: 3,
            scale: 10.0,
            enabled: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct MqttConfig {
    pub host: String,
    #[serde(default = "default_mqtt_port")]
    pub port: u16,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
    #[serde(default = "default_ha_prefix", alias = "discovery_prefix")]
    pub homeassistant_prefix: String,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let contents =
            fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut config: Self =
            serde_yaml::from_str(&contents).context("parsing configuration YAML")?;
        config.apply_environment_overrides();
        config.validate()?;
        Ok(config)
    }

    fn apply_environment_overrides(&mut self) {
        if let Ok(username) = std::env::var("MQTT_USERNAME") {
            self.mqtt.username = Some(username);
        }
        if let Ok(password) = std::env::var("MQTT_PASSWORD") {
            self.mqtt.password = Some(password);
        }
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(!self.inverter.host.is_empty(), "inverter.host must be set");
        anyhow::ensure!(!self.mqtt.host.is_empty(), "mqtt.host must be set");
        anyhow::ensure!(
            !self.mqtt.topic_prefix.trim_matches('/').is_empty(),
            "mqtt.topic_prefix must not be empty"
        );
        anyhow::ensure!(
            !self.mqtt.homeassistant_prefix.trim_matches('/').is_empty(),
            "mqtt.homeassistant_prefix must not be empty"
        );
        Ok(())
    }
}

fn default_state_file() -> Option<PathBuf> {
    Some(PathBuf::from("sofar-state.json"))
}
fn default_inverter_port() -> u16 {
    8899
}
fn default_slave_id() -> u8 {
    1
}
fn default_poll_interval() -> u64 {
    5
}
fn default_timeout() -> u64 {
    3
}
fn default_power_address() -> u16 {
    12
}
fn default_power_function() -> u8 {
    3
}
fn default_power_scale() -> f64 {
    10.0
}
fn default_true() -> bool {
    true
}
fn default_mqtt_port() -> u16 {
    1883
}
fn default_topic_prefix() -> String {
    "sofar".into()
}
fn default_ha_prefix() -> String {
    "homeassistant".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_values_use_configuration_defaults() {
        let config: Config =
            serde_yaml::from_str("inverter:\n  host: inverter\nmqtt:\n  host: broker\n").unwrap();

        config.validate().unwrap();
        assert_eq!(config.inverter.port, 8899);
        assert_eq!(config.inverter.slave_id, 1);
        assert_eq!(config.inverter.poll_interval, 5);
        assert_eq!(config.inverter.timeout, 3);
        assert_eq!(config.inverter.active_power.address, 12);
        assert_eq!(config.inverter.active_power.function, 3);
        assert_eq!(config.inverter.active_power.scale, 10.0);
        assert!(config.inverter.active_power.enabled);
        assert_eq!(config.mqtt.port, 1883);
        assert_eq!(config.mqtt.topic_prefix, "sofar");
        assert_eq!(config.mqtt.homeassistant_prefix, "homeassistant");
        assert_eq!(
            config.state_file.as_deref(),
            Some(std::path::Path::new("sofar-state.json"))
        );
    }

    #[test]
    fn rejects_topic_prefixes_that_contain_only_slashes() {
        let config: Config = serde_yaml::from_str(
            "inverter:\n  host: inverter\nmqtt:\n  host: broker\n  topic_prefix: '///'\n",
        )
        .unwrap();

        assert!(config.validate().is_err());
    }
}
