use std::collections::HashMap;

use colored::Colorize;
use epicars::ServerEvent;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, info};

use std::net::Ipv4Addr;

use bytemuck::{Pod, Zeroable};
use pnet::datalink;

pub async fn watch_lifecycle(
    mut recv: tokio::sync::broadcast::Receiver<ServerEvent>,
    read: bool,
    write: bool,
) {
    let mut peers = HashMap::new();
    let mut client_id: HashMap<u64, String> = HashMap::new();
    let mut channel_names = HashMap::new();
    loop {
        let event = match recv.recv().await {
            Ok(event) => event,
            Err(RecvError::Closed) => break,
            Err(RecvError::Lagged(n)) => {
                debug!("Warning: Lagged behind in server event listening (missed {n} events)");
                continue;
            }
        };

        match event {
            ServerEvent::CircuitOpened { id, peer } => {
                peers.insert(id, peer);
            }
            ServerEvent::CircuitClose { id } => {
                peers.remove(&id);
                client_id.remove(&id);
                channel_names.remove(&id);
            }
            ServerEvent::ClientIdentified {
                circuit_id,
                client_hostname,
                client_username,
            } => {
                let user_str = format!("{}@{}", client_username.magenta(), client_hostname.blue());
                client_id.insert(circuit_id, user_str);
                channel_names.insert(circuit_id, HashMap::new());
            }
            ServerEvent::CreateChannel {
                circuit_id,
                channel_id,
                channel_name,
            } => {
                channel_names
                    .entry(circuit_id)
                    .or_insert_with(HashMap::new)
                    .insert(channel_id, channel_name);
            }
            ServerEvent::ClearChannel {
                circuit_id,
                channel_id,
            } => {
                channel_names
                    .get_mut(&circuit_id)
                    .and_then(|v| v.remove(&channel_id));
            }
            ServerEvent::Read {
                circuit_id,
                channel_id,
                success,
            } => {
                if read {
                    let user_str = &client_id.get(&circuit_id).cloned().unwrap_or_else(|| {
                        format!("(unknown user {}", circuit_id.to_string().green())
                    });
                    let channel_name = channel_names
                        .get(&circuit_id)
                        .and_then(|m| m.get(&channel_id))
                        .cloned()
                        .unwrap_or(format!("(Unknown {circuit_id}:{channel_id})"));
                    info!(
                        "{:5} {name} by {user_str}",
                        if success {
                            "Read".green()
                        } else {
                            "Read (failed) ".red()
                        },
                        name = channel_name.bold(),
                    );
                }
            }
            ServerEvent::Write {
                circuit_id,
                channel_id,
                success,
            } => {
                if write {
                    let user_str = &client_id.get(&circuit_id).cloned().unwrap_or_else(|| {
                        format!("(unknown user {}", circuit_id.to_string().green())
                    });
                    let channel_name = channel_names
                        .get(&circuit_id)
                        .and_then(|m| m.get(&channel_id))
                        .cloned()
                        .unwrap_or(format!("(Unknown {circuit_id}:{channel_id})"));
                    info!(
                        "{:5} {name} by {user_str}",
                        if success {
                            "Write".yellow()
                        } else {
                            "Write (failed)".red()
                        },
                        name = channel_name.bold(),
                    );
                }
            }
            ServerEvent::Subscribe {
                circuit_id,
                channel_id,
            } => {
                if read {
                    let user_str = &client_id.get(&circuit_id).cloned().unwrap_or_else(|| {
                        format!("(unknown user {}", circuit_id.to_string().green())
                    });
                    let channel_name = channel_names
                        .get(&circuit_id)
                        .and_then(|m| m.get(&channel_id))
                        .cloned()
                        .unwrap_or(format!("(Unknown {circuit_id}:{channel_id})"));
                    info!("{user_str} subscribed to {}", channel_name.bold());
                }
            }
            ServerEvent::Unsubscribe {
                circuit_id,
                channel_id,
            } => {
                if read {
                    let user_str = &client_id.get(&circuit_id).cloned().unwrap_or_else(|| {
                        format!("(unknown user {}", circuit_id.to_string().green())
                    });
                    let channel_name = channel_names
                        .get(&circuit_id)
                        .and_then(|m| m.get(&channel_id))
                        .cloned()
                        .unwrap_or(format!("(Unknown {circuit_id}:{channel_id})"));
                    info!("{user_str} unsubscribed from {}", channel_name.bold());
                }
            }
        }
    }
}

pub fn ball_spinner() -> impl Iterator<Item = &'static str> {
    [
        "( ●    )",
        "(  ●   )",
        "(   ●  )",
        "(    ● )",
        "(     ●)",
        "(    ● )",
        "(   ●  )",
        "(  ●   )",
        "( ●    )",
        "(●     )",
    ]
    .iter()
    .copied()
    .cycle()
}

#[repr(C)]
#[derive(Debug, Copy, Clone, Zeroable, Pod)]
pub struct DelugeTrigger {
    pub frames: u128,
    pub exptime: f32,
    pub uuid: [u8; 12],
}
impl Default for DelugeTrigger {
    fn default() -> Self {
        DelugeTrigger {
            frames: 0,
            exptime: 0.0,
            uuid: rand::random(),
        }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, Zeroable, Pod)]
pub struct SlsDetectorHeader {
    /// Frame number to which the current packet belongs to
    pub frame_number: u64,
    /// Measured exposure time of the frame in tenths of microsecond (100ns)
    pub exposure_length: u32,
    /// Packet number of the frame to which the current data belongs to.
    pub packet_number: u32,
    /// detSpec1: Bunch identification number received by the detector at the moment of frame acquisition.
    pub bunch_id: u64,
    /// Time measured at the start of frame exposure since the start of the current measurement. It is expressed in tenths of microsecond.
    pub timestamp: u64,
    /// module ID picked up from det_id_[detector type].txt on the detector cpu
    pub module_id: u16,
    /// row position of the module in the detector system. It is calculated by the order of the module in hostname command, as well as the detsize command. The modules are stacked row by row until they reach the y-axis limit set by detsize (if specified). Then, stacking continues in the next column and so on.
    pub row: u16,
    /// column position of the module in the detector system. It is calculated by the order of the module in hostname command, as well as the detsize command. The modules are stacked row by row until they reach the y-axis limit set by detsize (if specified). Then, stacking continues in the next column and so on.
    pub column: u16,
    /// Unused for Jungfrau
    _det_spec_2: u16,
    /// DAQ Info field: See https://slsdetectorgroup.github.io/devdoc/udpdetspec.html#id10
    pub daq_info: u32,
    /// Unused for Jungfrau
    _det_spec_4: u16,

    /// detector type from enum of detectorType in the package.
    pub det_type: u8,

    /// Current version of the detector header
    pub version: u8,
}

pub enum SlsDetectorType {
    Generic = 0,
    Eiger = 1,
    Gotthard = 2,
    Jungfrau = 3,
    ChipTestBoard = 4,
    Moench = 5,
    Mythen3 = 6,
    Gotthard2 = 7,
}

impl TryFrom<u8> for SlsDetectorType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SlsDetectorType::Generic),
            1 => Ok(SlsDetectorType::Eiger),
            2 => Ok(SlsDetectorType::Gotthard),
            3 => Ok(SlsDetectorType::Jungfrau),
            4 => Ok(SlsDetectorType::ChipTestBoard),
            5 => Ok(SlsDetectorType::Moench),
            6 => Ok(SlsDetectorType::Mythen3),
            7 => Ok(SlsDetectorType::Gotthard2),
            _ => Err(()),
        }
    }
}

pub fn get_interface_addreses_with_prefix(prefix: u8) -> Vec<Ipv4Addr> {
    let mut addresses: Vec<_> = datalink::interfaces()
        .iter()
        .flat_map(|x| &x.ips)
        .flat_map(|x| match x {
            pnet::ipnetwork::IpNetwork::V4(ip) => Some(ip),
            _ => None,
        })
        .map(|x| x.ip())
        .filter(|x| x.octets()[0] == prefix)
        .collect();
    addresses.sort();
    addresses
}
