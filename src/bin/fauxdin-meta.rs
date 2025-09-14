#![deny(clippy::indexing_slicing)]

use std::collections::HashMap;

use clap::Parser;
use colored::Colorize;
use epicars::{ServerBuilder, ServerEvent, providers::IntercomProvider};
use time::macros::format_description;
use tokio::{select, sync::broadcast::error::RecvError};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::fmt::time::LocalTime;

#[derive(Parser)]
struct Options {
    /// Show debug output
    #[clap(short, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    let opts = Options::parse();
    tracing_subscriber::fmt()
        .with_max_level(match opts.verbose {
            0 => LevelFilter::INFO,
            1 => LevelFilter::DEBUG,
            2.. => LevelFilter::TRACE,
        })
        .with_timer(LocalTime::new(format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"
        )))
        .with_target(opts.verbose != 0)
        .with_level(opts.verbose != 0)
        .init();

    let mut provider = IntercomProvider::new();
    //provider.prefix = "BL24I-JUNGFRAU-META:FD:".to_string();
    provider.rbv = true;

    let _pv_path = provider
        .add_string_pv("BL24I-JUNGFRAU-META:FD:FilePath", "", Some(128))
        .unwrap();
    let _pv_name = provider
        .add_string_pv("BL24I-JUNGFRAU-META:FD:FileName", "", Some(128))
        .unwrap();
    let _pv_count = provider
        .add_pv("BL24I-JUNGFRAU-META:FD:NumCapture", 0i32)
        .unwrap();
    let _pv_count_captured = provider
        .add_pv("BL24I-JUNGFRAU-META:FD:NumCaptured", 0i32)
        .unwrap();
    let _pv_subfolder = provider
        .add_pv("BL24I-JUNGFRAU-META:FD:Subfolder", 0i8)
        .unwrap();
    let _pv_ready = provider
        .add_pv("BL24I-JUNGFRAU-META:FD:Ready", 0i8)
        .unwrap();

    let mut server = ServerBuilder::new(provider).start();

    let listen = server.listen_to_events();
    let listener = tokio::spawn(async move {
        watch_lifecycle(listen).await;
    });

    select! {
        _ = listener => {},
        _ = server.join() => {},
        _ = tokio::signal::ctrl_c() => {
            println!("Ctrl-C: Shutting down");
        },
    };
    // Wait for shutdown, unless another ctrl-c
    select! {
        _ = server.stop() => (),
        _ = tokio::signal::ctrl_c() => println!("Terminating"),
    };
}

async fn watch_lifecycle(mut recv: tokio::sync::broadcast::Receiver<ServerEvent>) {
    let mut peers = HashMap::new();
    let mut client_id: HashMap<u64, String> = HashMap::new();
    let mut channel_names = HashMap::new();
    loop {
        let event = match recv.recv().await {
            Ok(event) => event,
            Err(RecvError::Closed) => break,
            Err(RecvError::Lagged(n)) => {
                info!("Warning: Lagged behind in server event listening (missed {n} events)");
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
                let user_str = &client_id
                    .get(&circuit_id)
                    .cloned()
                    .unwrap_or_else(|| format!("(unknown user {}", circuit_id.to_string().green()));
                let channel_name = channel_names
                    .get(&circuit_id)
                    .and_then(|m| m.get(&channel_id))
                    .cloned()
                    .unwrap_or(format!("(Unknown {circuit_id}:{channel_id})"));
                info!(
                    "{name}: {:5} by {user_str}",
                    if success {
                        "Read".green()
                    } else {
                        "Read (failed) ".red()
                    },
                    name = channel_name.bold(),
                );
            }
            ServerEvent::Write {
                circuit_id,
                channel_id,
                success,
            } => {
                let user_str = &client_id
                    .get(&circuit_id)
                    .cloned()
                    .unwrap_or_else(|| format!("(unknown user {}", circuit_id.to_string().green()));
                let channel_name = channel_names
                    .get(&circuit_id)
                    .and_then(|m| m.get(&channel_id))
                    .cloned()
                    .unwrap_or(format!("(Unknown {circuit_id}:{channel_id})"));
                info!(
                    "{}: {:5} by {user_str}",
                    channel_name.bold(),
                    if success {
                        "Write".yellow()
                    } else {
                        "Write (failed)".red()
                    },
                );
            }
            ServerEvent::Subscribe {
                circuit_id,
                channel_id,
            } => {
                let user_str = &client_id
                    .get(&circuit_id)
                    .cloned()
                    .unwrap_or_else(|| format!("(unknown user {}", circuit_id.to_string().green()));
                let channel_name = channel_names
                    .get(&circuit_id)
                    .and_then(|m| m.get(&channel_id))
                    .cloned()
                    .unwrap_or(format!("(Unknown {circuit_id}:{channel_id})"));
                info!("{user_str} subscribed to {}", channel_name.bold());
            }
        }
    }
}
