use anyhow::Result;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, QoS};
use serde_json::json;
use std::time::Duration;

use crate::{config::MqttConfig, inverter::InverterCommand};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Availability {
    Bridge(AvailabilityState),
    Inverter(AvailabilityState),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AvailabilityState {
    Online,
    Offline,
}

impl Availability {
    fn topic<'a>(self, topics: &'a Topics) -> &'a str {
        match self {
            Self::Bridge(_) => &topics.availability,
            Self::Inverter(_) => &topics.inverter_availability,
        }
    }

    fn payload(self) -> &'static str {
        match self {
            Self::Bridge(AvailabilityState::Online) | Self::Inverter(AvailabilityState::Online) => {
                "online"
            }
            Self::Bridge(AvailabilityState::Offline)
            | Self::Inverter(AvailabilityState::Offline) => "offline",
        }
    }
}

#[derive(Clone)]
pub struct Topics {
    availability: String,
    inverter_availability: String,
    command: String,
    desired: String,
    power: String,
    discovery_prefix: String,
}

impl Topics {
    pub fn new(prefix: &str, discovery_prefix: &str) -> Self {
        let prefix = prefix.trim_matches('/');
        let inverter_prefix = format!("{prefix}/inverter");
        Self {
            availability: format!("{prefix}/availability"),
            inverter_availability: format!("{inverter_prefix}/availability"),
            command: format!("{inverter_prefix}/set"),
            desired: format!("{inverter_prefix}/remote_command"),
            power: format!("{inverter_prefix}/active_power"),
            discovery_prefix: discovery_prefix.trim_matches('/').to_owned(),
        }
    }

    pub fn is_command_topic(&self, topic: &str) -> bool {
        topic == self.command
    }
}

pub struct MqttBridge {
    client: AsyncClient,
    eventloop: EventLoop,
    topics: Topics,
}

impl MqttBridge {
    pub fn new(config: &MqttConfig, topics: &Topics) -> Self {
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
        Self {
            client,
            eventloop,
            topics: topics.clone(),
        }
    }

    pub async fn poll_event(&mut self) -> std::result::Result<Event, rumqttc::ConnectionError> {
        self.eventloop.poll().await
    }

    pub async fn subscribe_commands(&self) -> Result<()> {
        self.client
            .subscribe(&self.topics.command, QoS::AtLeastOnce)
            .await?;
        Ok(())
    }

    pub async fn publish_availability(&self, availability: Availability) -> Result<()> {
        self.client
            .publish(
                availability.topic(&self.topics),
                QoS::AtLeastOnce,
                true,
                availability.payload(),
            )
            .await?;
        Ok(())
    }
    pub async fn publish_desired(&self, command: InverterCommand) -> Result<()> {
        self.client
            .publish(
                &self.topics.desired,
                QoS::AtLeastOnce,
                true,
                command.register().to_string(),
            )
            .await?;
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
        let device = json!({
            "identifiers": ["sofar_2200tl"],
            "name": "SOFAR 2200TL",
            "manufacturer": "SOFAR",
            "model": "2200TL"
        });
        for entity in self.discovery_entities() {
            self.publish_discovery_entity(entity, &device).await?;
        }
        Ok(())
    }

    fn discovery_entities(&self) -> [DiscoveryEntity; 5] {
        [
            DiscoveryEntity {
                component: "sensor",
                id: "active_power",
                config: json!({
                    "name": "Active Power",
                    "unique_id": "sofar_active_power",
                    "state_topic": self.topics.power,
                    "device_class": "power",
                    "state_class": "measurement",
                    "unit_of_measurement": "W"
                }),
            },
            DiscoveryEntity {
                component: "sensor",
                id: "remote_command",
                config: json!({
                    "name": "Remote Command",
                    "unique_id": "sofar_remote_command",
                    "state_topic": self.topics.desired,
                    "entity_category": "diagnostic"
                }),
            },
            DiscoveryEntity {
                component: "switch",
                id: "inverter",
                config: json!({
                    "name": "Inverter",
                    "unique_id": "sofar_inverter",
                    "command_topic": self.topics.command,
                    "state_topic": self.topics.desired,
                    "payload_on": "ON",
                    "payload_off": "OFF",
                    "state_on": "85",
                    "state_off": "102",
                    "optimistic": false
                }),
            },
            DiscoveryEntity {
                component: "binary_sensor",
                id: "online",
                config: json!({
                    "name": "Bridge Online",
                    "unique_id": "sofar_online",
                    "state_topic": self.topics.availability,
                    "payload_on": "online",
                    "payload_off": "offline",
                    "device_class": "connectivity",
                    "entity_category": "diagnostic"
                }),
            },
            DiscoveryEntity {
                component: "binary_sensor",
                id: "inverter_online",
                config: json!({
                    "name": "Inverter Online",
                    "unique_id": "sofar_inverter_online",
                    "state_topic": self.topics.inverter_availability,
                    "payload_on": "online",
                    "payload_off": "offline",
                    "device_class": "connectivity",
                    "entity_category": "diagnostic"
                }),
            },
        ]
    }

    async fn publish_discovery_entity(
        &self,
        mut entity: DiscoveryEntity,
        device: &serde_json::Value,
    ) -> Result<()> {
        entity.config["availability_topic"] = json!(self.topics.availability);
        entity.config["payload_available"] = json!("online");
        entity.config["payload_not_available"] = json!("offline");
        entity.config["device"] = device.clone();
        let topic = format!(
            "{}/{}/sofar/{}/config",
            self.topics.discovery_prefix, entity.component, entity.id
        );
        self.client
            .publish(topic, QoS::AtLeastOnce, true, entity.config.to_string())
            .await?;
        Ok(())
    }
}

struct DiscoveryEntity {
    component: &'static str,
    id: &'static str,
    config: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn availability_selects_topic_and_payload() {
        let topics = Topics::new("solar", "homeassistant");

        assert_eq!(
            Availability::Bridge(AvailabilityState::Offline).topic(&topics),
            "solar/availability"
        );
        assert_eq!(
            Availability::Inverter(AvailabilityState::Online).topic(&topics),
            "solar/inverter/availability"
        );
        assert_eq!(
            Availability::Inverter(AvailabilityState::Online).payload(),
            "online"
        );
        assert_eq!(
            Availability::Bridge(AvailabilityState::Offline).payload(),
            "offline"
        );
    }

    #[test]
    fn trims_topic_prefixes_and_matches_command_topic() {
        let topics = Topics::new("/solar/", "/homeassistant/");

        assert!(topics.is_command_topic("solar/inverter/set"));
        assert!(!topics.is_command_topic("solar/inverter/remote_command"));
        assert_eq!(topics.availability, "solar/availability");
        assert_eq!(topics.power, "solar/inverter/active_power");
    }
}
