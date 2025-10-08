use std::collections::HashMap;

use colored::Colorize;
use epicars::ServerEvent;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, info};

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
