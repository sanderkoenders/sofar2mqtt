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
use mqtt::{MqttBridge, Topics};
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
    let topics = Topics::new(&config.mqtt.topic_prefix, &config.mqtt.homeassistant_prefix);
    let mut desired = DesiredState::load(config.state_file.as_deref())?;
    let inverter = Inverter::new(config.inverter.clone());
    let mut mqtt = MqttBridge::new(&config.mqtt, &topics)?;
    let mut poll = time::interval(Duration::from_secs(config.inverter.poll_interval.max(1)));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);

    info!("SOFAR MQTT bridge started");
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown requested");
                mqtt.publish_availability("offline").await?;
                break;
            }
            event = mqtt.eventloop.poll() => match event {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!("Connected to MQTT broker");
                    mqtt.subscribe_commands().await?;
                    mqtt.publish_availability("online").await?;
                    mqtt.publish_discovery().await?;
                    mqtt.publish_desired(desired.command()).await?;
                }
                Ok(Event::Incoming(Packet::Publish(message))) if message.topic == topics.command => {
                    if message.retain && desired.command().is_some() {
                        warn!("Ignoring retained broker command because a persisted desired state exists");
                        continue;
                    }
                    match InverterCommand::parse(&message.payload) {
                        Ok(command) => {
                            if let Err(err) = desired.set(command, config.state_file.as_deref()) {
                                error!(?err, "Could not persist desired state; command not accepted");
                                continue;
                            }
                            mqtt.publish_desired(Some(command)).await?;
                            info!(?command, "Stored desired inverter state");
                            if let Err(err) = inverter.apply(command).await {
                                warn!(?err, "Inverter unavailable; desired state will be retried");
                            }
                        }
                        Err(err) => warn!(?err, "Ignoring invalid MQTT command"),
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(?err, "MQTT connection error; client will reconnect");
                    time::sleep(Duration::from_secs(1)).await;
                }
            },
            _ = poll.tick() => {
                if let Some(command) = desired.command() {
                    if let Err(err) = inverter.apply(command).await {
                        warn!(?err, ?command, "Inverter unavailable; desired state remains queued");
                    }
                } else {
                    match inverter.observed_command().await {
                        Ok(Some(command)) => {
                            if let Err(err) = desired.set(command, config.state_file.as_deref()) {
                                error!(?err, "Could not persist inverter's initial state");
                            } else {
                                mqtt.publish_desired(Some(command)).await?;
                            }
                        }
                        Ok(None) => {},
                        Err(err) => warn!(?err, "Could not read inverter state yet"),
                    }
                }
                match inverter.read_active_power().await {
                    Ok(Some(power)) => mqtt.publish_power(power).await?,
                    Ok(None) => {},
                    Err(err) => warn!(?err, "Could not read inverter power"),
                }
            }
        }
    }
    Ok(())
}
