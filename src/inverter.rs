use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

use crate::config::InverterConfig;

const CONTROL_START: u16 = 0x1040;
const CONTROL_COUNT: u16 = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum InverterCommand {
    On,
    Off,
}

impl InverterCommand {
    pub fn register(self) -> u16 {
        match self {
            Self::On => 0x0055,
            Self::Off => 0x0066,
        }
    }
    pub fn parse(payload: &[u8]) -> Result<Self> {
        match std::str::from_utf8(payload)?
            .trim()
            .to_ascii_uppercase()
            .as_str()
        {
            "ON" => Ok(Self::On),
            "OFF" => Ok(Self::Off),
            value => bail!("expected ON or OFF, got {value:?}"),
        }
    }
}

pub struct Inverter {
    config: InverterConfig,
}

impl Inverter {
    pub fn new(config: InverterConfig) -> Self {
        Self { config }
    }

    pub async fn read_active_power(&self) -> Result<Option<f64>> {
        if !self.config.active_power.enabled {
            return Ok(None);
        }
        let values = self
            .read_registers(
                self.config.active_power.address,
                1,
                self.config.active_power.function,
            )
            .await?;
        Ok(Some(f64::from(values[0]) * self.config.active_power.scale))
    }

    pub async fn observed_command(&self) -> Result<Option<InverterCommand>> {
        let registers = self
            .read_registers(CONTROL_START, CONTROL_COUNT, 0x04)
            .await?;
        Ok(match registers[2] {
            0x0055 => Some(InverterCommand::On),
            0x0066 => Some(InverterCommand::Off),
            _ => None,
        })
    }

    pub async fn apply(&self, command: InverterCommand) -> Result<()> {
        let mut registers = self
            .read_registers(CONTROL_START, CONTROL_COUNT, 0x04)
            .await?;
        if registers[0] & 0x0002 != 0 && registers[2] == command.register() {
            return Ok(());
        }
        registers[0] |= 0x0002; // Enable remote ON/OFF control.
        registers[2] = command.register();
        let payload: Vec<u8> = registers.iter().flat_map(|v| v.to_be_bytes()).collect();
        let mut request = Vec::with_capacity(8 + payload.len());
        request.extend_from_slice(&[self.config.slave_id, 0x13]);
        request.extend_from_slice(&CONTROL_START.to_be_bytes());
        request.extend_from_slice(&CONTROL_COUNT.to_be_bytes());
        request.extend_from_slice(&payload);
        append_crc(&mut request);
        let response = self.exchange(&request, request.len()).await?;
        check_crc(&response)?;
        ensure!(
            response == request,
            "SOFAR write response did not echo the request"
        );

        let result = self
            .read_registers(CONTROL_START, CONTROL_COUNT, 0x04)
            .await?;
        ensure!(
            result[0] & 0x0002 != 0,
            "remote control enable was not verified"
        );
        ensure!(
            result[2] == command.register(),
            "command verification failed: {:#06X}",
            result[2]
        );
        Ok(())
    }

    async fn read_registers(&self, address: u16, count: u16, function: u8) -> Result<Vec<u16>> {
        let mut request = Vec::with_capacity(8);
        request.extend_from_slice(&[self.config.slave_id, function]);
        request.extend_from_slice(&address.to_be_bytes());
        request.extend_from_slice(&count.to_be_bytes());
        append_crc(&mut request);
        let response = self.exchange_read(&request, function).await?;
        check_crc(&response)?;
        ensure!(
            response[0] == self.config.slave_id,
            "unexpected Modbus slave id"
        );
        if response[1] == (function | 0x80) {
            bail!("Modbus exception response: {:02x?}", response);
        }
        ensure!(
            response[1] == function,
            "unexpected Modbus function {}",
            response[1]
        );
        ensure!(
            response[2] == (count * 2) as u8,
            "unexpected Modbus byte count {}",
            response[2]
        );
        Ok(response[3..response.len() - 2]
            .chunks_exact(2)
            .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
            .collect())
    }

    async fn exchange(&self, request: &[u8], response_len: usize) -> Result<Vec<u8>> {
        let duration = Duration::from_secs(self.config.timeout.max(1));
        timeout(duration, async {
            let mut stream = TcpStream::connect((self.config.host.as_str(), self.config.port))
                .await
                .with_context(|| {
                    format!(
                        "connecting to inverter {}:{}",
                        self.config.host, self.config.port
                    )
                })?;
            stream
                .write_all(request)
                .await
                .context("sending Modbus request")?;
            let mut response = vec![0; response_len];
            stream
                .read_exact(&mut response)
                .await
                .context("reading Modbus response")?;
            Ok(response)
        })
        .await
        .context("inverter request timed out")?
    }

    async fn exchange_read(&self, request: &[u8], function: u8) -> Result<Vec<u8>> {
        let duration = Duration::from_secs(self.config.timeout.max(1));
        timeout(duration, async {
            let mut stream = TcpStream::connect((self.config.host.as_str(), self.config.port))
                .await
                .with_context(|| {
                    format!(
                        "connecting to inverter {}:{}",
                        self.config.host, self.config.port
                    )
                })?;
            stream
                .write_all(request)
                .await
                .context("sending Modbus request")?;
            let mut header = [0; 3];
            stream
                .read_exact(&mut header)
                .await
                .context("reading Modbus response header")?;
            let remaining = if header[1] == (function | 0x80) {
                2
            } else {
                usize::from(header[2]) + 2
            };
            let mut response = header.to_vec();
            let start = response.len();
            response.resize(start + remaining, 0);
            stream
                .read_exact(&mut response[start..])
                .await
                .context("reading Modbus response body")?;
            Ok(response)
        })
        .await
        .context("inverter request timed out")?
    }
}

fn append_crc(frame: &mut Vec<u8>) {
    let crc = crc16(frame);
    frame.extend_from_slice(&crc.to_le_bytes());
}
fn check_crc(frame: &[u8]) -> Result<()> {
    ensure!(frame.len() >= 5, "Modbus response too short");
    let expected = u16::from_le_bytes([frame[frame.len() - 2], frame[frame.len() - 1]]);
    ensure!(
        crc16(&frame[..frame.len() - 2]) == expected,
        "invalid Modbus CRC"
    );
    Ok(())
}
fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0xFFFF;
    for byte in data {
        crc ^= u16::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xA001
            } else {
                crc >> 1
            };
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_commands_case_insensitively_and_trims_whitespace() {
        assert_eq!(InverterCommand::parse(b"ON").unwrap(), InverterCommand::On);
        assert_eq!(
            InverterCommand::parse(b" off \n").unwrap(),
            InverterCommand::Off
        );
    }

    #[test]
    fn rejects_invalid_command_payloads() {
        assert!(InverterCommand::parse(b"START").is_err());
        assert!(InverterCommand::parse(&[0xFF]).is_err());
    }

    #[test]
    fn maps_commands_to_register_values() {
        assert_eq!(InverterCommand::On.register(), 0x0055);
        assert_eq!(InverterCommand::Off.register(), 0x0066);
    }

    #[test]
    fn calculates_modbus_crc_for_standard_check_value() {
        assert_eq!(crc16(b"123456789"), 0x4B37);
    }
}
