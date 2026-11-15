use anyhow::Result;
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use serde_json::json;
use std::time::Duration;

use crate::{config::MqttConfig, inverter::InverterCommand};

#[derive(Clone)]
pub struct Topics {
    pub availability: String,
    pub command: String,
    pub desired: String,
    pub power: String,
    discovery_prefix: String,
}

impl Topics {
    pub fn new(prefix: &str, discovery_prefix: &str) -> Self {
        let prefix = prefix.trim_matches('/');
        Self {
            availability: format!("{prefix}/availability"),
            command: format!("{prefix}/inverter/set"),
            desired: format!("{prefix}/remote_command"),
            power: format!("{prefix}/active_power"),
            discovery_prefix: discovery_prefix.trim_matches('/').to_owned(),
        }
    }
}

pub struct MqttBridge {
    pub client: AsyncClient,
    pub eventloop: EventLoop,
    topics: Topics,
}

impl MqttBridge {
    pub fn new(config: &MqttConfig, topics: &Topics) -> Result<Self> {
        let mut options = MqttOptions::new("sofar-mqtt", config.host.clone(), config.port);
        options.set_keep_alive(Duration::from_secs(30));
        options.set_last_will(rumqttc::LastWill::new(
            &topics.availability,
            "offline",
            QoS::AtLeastOnce,
            true,
        ));
        if let Some(username) = &config.username {
            options.set_credentials(
                username.clone(),
                config.password.clone().unwrap_or_default(),
            );
        }
        let (client, eventloop) = AsyncClient::new(options, 32);
        Ok(Self {
            client,
            eventloop,
            topics: topics.clone(),
        })
    }

    pub async fn subscribe_commands(&self) -> Result<()> {
        self.client
            .subscribe(&self.topics.command, QoS::AtLeastOnce)
            .await?;
        Ok(())
    }

    pub async fn publish_availability(&self, value: &str) -> Result<()> {
        self.client
            .publish(&self.topics.availability, QoS::AtLeastOnce, true, value)
            .await?;
        Ok(())
    }
    pub async fn publish_desired(&self, command: Option<InverterCommand>) -> Result<()> {
        if let Some(command) = command {
            self.client
                .publish(
                    &self.topics.desired,
                    QoS::AtLeastOnce,
                    true,
                    format!("{}", command.register()),
                )
                .await?;
        }
        Ok(())
    }
    pub async fn publish_power(&self, power: f64) -> Result<()> {
        self.client
            .publish(
                &self.topics.power,
                QoS::AtLeastOnce,
                true,
                power.to_string(),
            )
            .await?;
        Ok(())
    }
    pub async fn publish_discovery(&self) -> Result<()> {
        let device = json!({"identifiers":["sofar_2200tl"],"name":"SOFAR 2200TL","manufacturer":"SOFAR","model":"2200TL"});
        let entities = [
            (
                "sensor",
                "active_power",
                json!({"name":"Active Power","unique_id":"sofar_active_power","state_topic":self.topics.power,"device_class":"power","state_class":"measurement","unit_of_measurement":"W"}),
            ),
            (
                "sensor",
                "remote_command",
                json!({"name":"Remote Command","unique_id":"sofar_remote_command","state_topic":self.topics.desired,"entity_category":"diagnostic"}),
            ),
            (
                "switch",
                "inverter",
                json!({"name":"Inverter","unique_id":"sofar_inverter","command_topic":self.topics.command,"state_topic":self.topics.desired,"payload_on":"ON","payload_off":"OFF","state_on":"85","state_off":"102","optimistic":false}),
            ),
            (
                "binary_sensor",
                "online",
                json!({"name":"Bridge Online","unique_id":"sofar_online","state_topic":self.topics.availability,"payload_on":"online","payload_off":"offline","device_class":"connectivity"}),
            ),
        ];
        for (component, entity, mut config) in entities {
            config["availability_topic"] = json!(self.topics.availability);
            config["payload_available"] = json!("online");
            config["payload_not_available"] = json!("offline");
            config["device"] = device.clone();
            let topic = format!(
                "{}/{}/sofar/{}/config",
                self.topics.discovery_prefix, component, entity
            );
            self.client
                .publish(topic, QoS::AtLeastOnce, true, config.to_string())
                .await?;
        }
        Ok(())
    }
}
