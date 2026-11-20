mod config;
mod inverter;
mod mqtt;
mod state;

use anyhow::Result;
use rumqttc::{Event, Packet};
use std::{path::PathBuf, time::Duration};
use tokio::time::{self, MissedTickBehavior};
use tracing::{error, info, warn};

use config::Config;
use inverter::{Inverter, InverterCommand};
use mqtt::{Availability, AvailabilityState, MqttBridge, Topics};
use state::DesiredState;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.yaml"));

    let config = Config::load(&config_path)?;

    let poll_interval = Duration::from_secs(config.inverter.poll_interval.max(1));

    Bridge::new(config)?.run(poll_interval).await
}

struct Bridge {
    topics: Topics,
    desired: DesiredState,
    inverter: Inverter,
    mqtt: MqttBridge,
    inverter_online: Option<AvailabilityState>,
}

impl Bridge {
    fn new(config: Config) -> Result<Self> {
        let topics = Topics::new(&config.mqtt.topic_prefix, &config.mqtt.homeassistant_prefix);
        let desired = DesiredState::load(config.state_file.as_deref())?;
        let inverter = Inverter::new(config.inverter.clone());
        let mqtt = MqttBridge::new(&config.mqtt, &topics);
        Ok(Self {
            topics,
            desired,
            inverter,
            mqtt,
            inverter_online: None,
        })
    }

    async fn run(mut self, poll_interval: Duration) -> Result<()> {
        let mut poll = time::interval(poll_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!("SOFAR MQTT bridge started");
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown requested");
                    self.mqtt
                        .publish_availability(Availability::Bridge(AvailabilityState::Offline))
                        .await?;
                    break;
                }
                event = self.mqtt.poll_event() => {
                    self.handle_mqtt_event(event).await?;
                }
                _ = poll.tick() => {
                    let online = self.poll_inverter().await?;
                    if self.inverter_online != Some(online) {
                        self.mqtt
                            .publish_availability(Availability::Inverter(online))
                            .await?;
                        self.inverter_online = Some(online);
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_mqtt_event(
        &mut self,
        event: std::result::Result<Event, rumqttc::ConnectionError>,
    ) -> Result<()> {
        match event {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                info!("Connected to MQTT broker");
                self.mqtt.subscribe_commands().await?;
                self.mqtt
                    .publish_availability(Availability::Bridge(AvailabilityState::Online))
                    .await?;
                self.mqtt.publish_discovery().await?;
                if let Some(command) = self.desired.command() {
                    self.mqtt.publish_desired(command).await?;
                }
                if let Some(online) = self.inverter_online {
                    self.mqtt
                        .publish_availability(Availability::Inverter(online))
                        .await?;
                }
            }
            Ok(Event::Incoming(Packet::Publish(message)))
                if self.should_ignore_retained_command(&message.topic, message.retain) =>
            {
                warn!("Ignoring retained broker command because a persisted desired state exists");
            }
            Ok(Event::Incoming(Packet::Publish(message)))
                if self.topics.is_command_topic(&message.topic) =>
            {
                self.handle_command(&message.payload).await?;
            }
            Ok(_) => {}
            Err(err) => {
                warn!(?err, "MQTT connection error; client will reconnect");
                time::sleep(Duration::from_secs(1)).await;
            }
        }
        Ok(())
    }

    fn should_ignore_retained_command(&self, topic: &str, retained: bool) -> bool {
        retained && self.topics.is_command_topic(topic) && self.desired.command().is_some()
    }

    async fn handle_command(&mut self, payload: &[u8]) -> Result<()> {
        match InverterCommand::parse(payload) {
            Ok(command) => {
                if let Err(err) = self.desired.set(command) {
                    error!(
                        ?err,
                        "Could not persist desired state; command not accepted"
                    );
                    return Ok(());
                }
                self.mqtt.publish_desired(command).await?;
                info!(?command, "Stored desired inverter state");
                if let Err(err) = self.inverter.apply(command).await {
                    warn!(?err, "Inverter unavailable; desired state will be retried");
                }
            }
            Err(err) => warn!(?err, "Ignoring invalid MQTT command"),
        }
        Ok(())
    }

    async fn poll_inverter(&mut self) -> Result<AvailabilityState> {
        let mut availability = AvailabilityState::Offline;
        if let Some(command) = self.desired.command() {
            match self.inverter.apply(command).await {
                Ok(()) => availability = AvailabilityState::Online,
                Err(err) => warn!(?err, ?command, "Could not apply desired state to inverter"),
            }
        } else {
            match self.inverter.observed_command().await {
                Ok(Some(command)) => {
                    availability = AvailabilityState::Online;
                    if let Err(err) = self.desired.set(command) {
                        error!(?err, "Could not persist inverter's initial state");
                    } else {
                        self.mqtt.publish_desired(command).await?;
                    }
                }
                Ok(None) => availability = AvailabilityState::Online,
                Err(err) => warn!(?err, "Could not read inverter state yet"),
            }
        }
        match self.inverter.read_active_power().await {
            Ok(Some(power)) => {
                availability = AvailabilityState::Online;
                self.mqtt.publish_power(power).await?;
            }
            Ok(None) => {}
            Err(err) => warn!(?err, "Could not read inverter power"),
        }
        Ok(availability)
    }
}
