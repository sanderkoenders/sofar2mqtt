use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

use crate::inverter::InverterCommand;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DesiredState {
    command: Option<InverterCommand>,
}

impl DesiredState {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        match path {
            Some(path) if path.exists() => {
                let content = fs::read_to_string(path)
                    .with_context(|| format!("reading state file {}", path.display()))?;
                serde_json::from_str(&content).context("parsing desired state")
            }
            _ => Ok(Self::default()),
        }
    }

    pub fn command(&self) -> Option<InverterCommand> {
        self.command
    }

    pub fn set(&mut self, command: InverterCommand, path: Option<&Path>) -> Result<()> {
        if let Some(path) = path {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating state directory {}", parent.display()))?;
            }
            let temporary = path.with_extension("json.tmp");
            let candidate = Self {
                command: Some(command),
            };
            let bytes = serde_json::to_vec_pretty(&candidate)?;
            let mut file = File::create(&temporary)
                .with_context(|| format!("creating {}", temporary.display()))?;
            file.write_all(&bytes)
                .with_context(|| format!("writing {}", temporary.display()))?;
            file.sync_all()
                .with_context(|| format!("syncing {}", temporary.display()))?;
            drop(file);
            fs::rename(&temporary, path)
                .with_context(|| format!("replacing state file {}", path.display()))?;
        }
        self.command = Some(command);
        Ok(())
    }
}
