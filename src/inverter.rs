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
const CONTROL_READ_FUNCTION: u8 = 0x04;
const CONTROL_WRITE_FUNCTION: u8 = 0x13;
const REMOTE_CONTROL_ENABLED: u16 = 0x0002;
const ON_REGISTER: u16 = 0x0055;
const OFF_REGISTER: u16 = 0x0066;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum InverterCommand {
    On,
    Off,
}

impl InverterCommand {
    pub fn register(self) -> u16 {
        match self {
            Self::On => ON_REGISTER,
            Self::Off => OFF_REGISTER,
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
        let registers = self.read_control_registers().await?;
        Ok(match registers[2] {
            ON_REGISTER => Some(InverterCommand::On),
            OFF_REGISTER => Some(InverterCommand::Off),
            _ => None,
        })
    }

    pub async fn apply(&self, command: InverterCommand) -> Result<()> {
        let mut registers = self.read_control_registers().await?;
        let Some(request) = self.control_write_request(&mut registers, command) else {
            return Ok(());
        };
        self.write_control_request(&request).await?;
        self.verify_control_command(command).await
    }

    async fn read_registers(&self, address: u16, count: u16, function: u8) -> Result<Vec<u16>> {
        let mut request = Vec::with_capacity(8);
        request.extend_from_slice(&[self.config.slave_id, function]);
        request.extend_from_slice(&address.to_be_bytes());
        request.extend_from_slice(&count.to_be_bytes());
        append_crc(&mut request);
        let response = self
            .exchange(&request, ResponseKind::ModbusRead { function })
            .await?;
        parse_register_response(
            &response,
            RegisterResponseSpec {
                slave_id: self.config.slave_id,
                function,
                count,
            },
        )
    }

    async fn read_control_registers(&self) -> Result<Vec<u16>> {
        self.read_registers(CONTROL_START, CONTROL_COUNT, CONTROL_READ_FUNCTION)
            .await
    }

    fn control_write_request(
        &self,
        registers: &mut [u16],
        command: InverterCommand,
    ) -> Option<Vec<u8>> {
        if registers[0] & REMOTE_CONTROL_ENABLED != 0 && registers[2] == command.register() {
            return None;
        }
        registers[0] |= REMOTE_CONTROL_ENABLED;
        registers[2] = command.register();

        let mut request = Vec::with_capacity(8 + registers.len() * 2);
        request.extend_from_slice(&[self.config.slave_id, CONTROL_WRITE_FUNCTION]);
        request.extend_from_slice(&CONTROL_START.to_be_bytes());
        request.extend_from_slice(&CONTROL_COUNT.to_be_bytes());
        request.extend(registers.iter().flat_map(|value| value.to_be_bytes()));
        append_crc(&mut request);
        Some(request)
    }

    async fn write_control_request(&self, request: &[u8]) -> Result<()> {
        let response = self
            .exchange(request, ResponseKind::FixedLength(request.len()))
            .await?;
        check_crc(&response)?;
        ensure!(
            response == request,
            "SOFAR write response did not echo the request"
        );
        Ok(())
    }

    async fn verify_control_command(&self, command: InverterCommand) -> Result<()> {
        let registers = self.read_control_registers().await?;
        ensure!(
            registers[0] & REMOTE_CONTROL_ENABLED != 0,
            "remote control enable was not verified"
        );
        ensure!(
            registers[2] == command.register(),
            "command verification failed: {:#06X}",
            registers[2]
        );
        Ok(())
    }

    async fn exchange(&self, request: &[u8], response_kind: ResponseKind) -> Result<Vec<u8>> {
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
            read_response(&mut stream, response_kind).await
        })
        .await
        .context("inverter request timed out")?
    }
}

#[derive(Clone, Copy)]
enum ResponseKind {
    FixedLength(usize),
    ModbusRead { function: u8 },
}

struct RegisterResponseSpec {
    slave_id: u8,
    function: u8,
    count: u16,
}

async fn read_response(stream: &mut TcpStream, kind: ResponseKind) -> Result<Vec<u8>> {
    match kind {
        ResponseKind::FixedLength(length) => {
            let mut response = vec![0; length];
            stream
                .read_exact(&mut response)
                .await
                .context("reading Modbus response")?;
            Ok(response)
        }
        ResponseKind::ModbusRead { function } => {
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
        }
    }
}

fn parse_register_response(response: &[u8], expected: RegisterResponseSpec) -> Result<Vec<u16>> {
    check_crc(response)?;
    ensure!(
        response[0] == expected.slave_id,
        "unexpected Modbus slave id"
    );
    if response[1] == (expected.function | 0x80) {
        bail!("Modbus exception response: {:02x?}", response);
    }
    ensure!(
        response[1] == expected.function,
        "unexpected Modbus function {}",
        response[1]
    );
    ensure!(
        response[2] == (expected.count * 2) as u8,
        "unexpected Modbus byte count {}",
        response[2]
    );
    Ok(response[3..response.len() - 2]
        .chunks_exact(2)
        .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
        .collect())
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

    #[test]
    fn decodes_register_response_words() {
        let mut response = vec![1, 3, 4, 0x12, 0x34, 0xAB, 0xCD];
        append_crc(&mut response);

        assert_eq!(
            parse_register_response(
                &response,
                RegisterResponseSpec {
                    slave_id: 1,
                    function: 3,
                    count: 2,
                }
            )
            .unwrap(),
            vec![0x1234, 0xABCD]
        );
    }

    #[test]
    fn rejects_register_response_with_invalid_crc() {
        let mut response = vec![1, 3, 2, 0x12, 0x34];
        append_crc(&mut response);
        response[3] ^= 0xFF;

        assert!(parse_register_response(
            &response,
            RegisterResponseSpec {
                slave_id: 1,
                function: 3,
                count: 1,
            }
        )
        .is_err());
    }
}
