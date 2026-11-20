use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use crate::inverter::InverterCommand;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DesiredState {
    command: Option<InverterCommand>,
    #[serde(skip)]
    path: Option<PathBuf>,
}

impl DesiredState {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let path = path.map(Path::to_path_buf);
        let mut state = match path.as_deref() {
            Some(path) if path.exists() => {
                let content = fs::read_to_string(path)
                    .with_context(|| format!("reading state file {}", path.display()))?;
                serde_json::from_str(&content).context("parsing desired state")
            }
            _ => Ok(Self::default()),
        }?;
        state.path = path;
        Ok(state)
    }

    pub fn command(&self) -> Option<InverterCommand> {
        self.command
    }

    pub fn set(&mut self, command: InverterCommand) -> Result<()> {
        if let Some(path) = self.path.as_deref() {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating state directory {}", parent.display()))?;
            }
            let temporary = path.with_extension("json.tmp");
            let candidate = Self {
                command: Some(command),
                path: self.path.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempStateFile(PathBuf);

    impl TempStateFile {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after Unix epoch")
                .as_nanos();
            Self(std::env::temp_dir().join(format!(
                "sofar2mqtt-state-{}-{unique}.json",
                std::process::id()
            )))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempStateFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
            let _ = fs::remove_file(self.0.with_extension("json.tmp"));
        }
    }

    #[test]
    fn saves_and_reloads_desired_command() {
        let file = TempStateFile::new();
        let mut state = DesiredState::load(Some(file.path())).unwrap();

        state.set(InverterCommand::Off).unwrap();

        let loaded = DesiredState::load(Some(file.path())).unwrap();
        assert_eq!(loaded.command(), Some(InverterCommand::Off));
    }

    #[test]
    fn updates_desired_command_without_persistence() {
        let mut state = DesiredState::default();

        state.set(InverterCommand::On).unwrap();

        assert_eq!(state.command(), Some(InverterCommand::On));
    }
}
