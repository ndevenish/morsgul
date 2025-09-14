use core::num;
use std::{
    num::NonZeroU16,
    panic::{self, UnwindSafe},
    path::PathBuf,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use clap::Parser;
use colored::Colorize;
use epicars::providers::intercom::StringIntercom;
use epicars::{ServerBuilder, ServerHandle, providers::IntercomProvider};
use time::macros::format_description;
use tracing::{debug, info, level_filters::LevelFilter};
use tracing_subscriber::fmt::time::LocalTime;

const DEFAULT_PREFIX: &str = "BL24I-JUNGFRAU:FD:";

#[derive(Parser, Debug)]
struct Args {
    /// The ZeroMQ connection string, including port
    #[clap(value_parser = parse_zmq_address_and_port)]
    connection_string: (String, u16),
    /// The number of listeners to launch.
    listeners: NonZeroU16,

    /// The number of ports to skip e.g. if running distributed
    #[clap(long, default_value_t = 0u16)]
    skipped_ports: u16,

    /// Show various levels of debug output
    #[clap(short, action = clap::ArgAction::Count)]
    verbose: u8,
    /// Prefix for PV exposed via epics
    #[clap(default_value = DEFAULT_PREFIX)]
    pv_prefix: String,
}

/// Parse a ZeroMQ connection string into address, port parts
///
/// This should work for non-TCP connections, as ":" is a valid part of
/// the name for other connection types.
fn parse_zmq_address_and_port(value: &str) -> Result<(String, u16), String> {
    let Some(idx) = value.rfind(":") else {
        return Err(
            "No colon present in address; You must provide a port-like numeric identifier as 'addr:num'".to_string(),
        );
    };
    let (left, right) = value.split_at(idx);
    let Ok(port) = right[1..].parse::<u16>() else {
        return Err(format!("Could not parse port '{right}' as number"));
    };
    Ok((left.to_string(), port))
}

#[derive(Clone, Debug)]
struct SharedState {
    barrier: Arc<Barrier>,
    cancelled: Arc<AtomicBool>,
    // condition_mutex: Arc<Mutex<bool>>,
}
impl SharedState {
    fn new(num_listeners: NonZeroU16) -> Self {
        SharedState {
            barrier: Arc::new(Barrier::new(num_listeners.get() as usize)),
            cancelled: Arc::new(AtomicBool::new(false)),
            // condition_mutex: Arc::new(Mutex::new(false)),
        }
    }
}

fn main() {
    let opts = Args::parse();
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

    println!(
        r#" ███▄ ▄███▓ ▒█████   ██▀███    ▄████  █    ██  ██▓
▓██▒▀█▀ ██▒▒██▒  ██▒▓██ ▒ ██▒ ██▒ ▀█▒ ██  ▓██▒▓██▒
▓██    ▓██░▒██░  ██▒▓██ ░▄█ ▒▒██░▄▄▄░▓██  ▒██░▒██░
▒██    ▒██ ▒██   ██░▒██▀▀█▄  ░▓█  ██▓▓▓█  ░██░▒██░
▒██▒   ░██▒░ ████▓▒░░██▓ ▒██▒░▒▓███▀▒▒▒█████▓ ░██████▒
░ ▒░   ░  ░░ ▒░▒░▒░ ░ ▒▓ ░▒▓░ ░▒   ▒ ░▒▓▒ ▒ ▒ ░ ▒░▓  ░
░  ░      ░  ░ ▒ ▒░   ░▒ ░ ▒░  ░   ░ ░░▒░ ░ ░ ░ ░ ▒  ░
░      ░   ░ ░ ░ ▒    ░░   ░ ░ ░   ░  ░░░ ░ ░   ░ ░
       ░       ░ ░     ░           ░    ░         ░  ░
          _       __     _ __
         | |     / /____(_) /____  _____(rs)
         | | /| / / ___/ / __/ _ \/ ___/
         | |/ |/ / /  / / /_/  __/ /
         |__/|__/_/  /_/\__/\___/_/"#
    );
    let start_port = opts.connection_string.1 + opts.skipped_ports;
    info!(
        "Connecting {} listeners to {}",
        opts.listeners.to_string().bold(),
        format!(
            "{}:{}-{}",
            opts.connection_string.0,
            start_port,
            start_port + opts.listeners.get()
        )
        .blue()
    );
    // println!("{opts:?}");

    // Make the shared communication object
    let shared_state = SharedState::new(opts.listeners);

    let mut threads = Vec::new();
    for port in start_port..(start_port + opts.listeners.get()) {
        let conn_str = opts.connection_string.0.clone();
        // Pull out the cancellation token so that we can cancel it on panic
        let token: Arc<AtomicBool> = shared_state.cancelled.clone();
        let this_thread_state = shared_state.clone();

        threads.push(thread::spawn(move || {
            listener_thread_safety_handler(token, || {
                do_single_listener(this_thread_state, conn_str, port);
            });
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }
    info!("All threads terminated, closing.");
}

/// Catch unwinds in the thread, then set a cancellation token before resuming
fn listener_thread_safety_handler<F>(cancel_token: Arc<AtomicBool>, task: F)
where
    F: FnOnce() + Send + UnwindSafe,
{
    match panic::catch_unwind(task) {
        Ok(_) => (),
        Err(err) => {
            cancel_token.store(true, Ordering::Relaxed);
            panic::resume_unwind(err);
        }
    }
}

#[tracing::instrument(name = "listener", skip(shared, connection_str))]
fn do_single_listener(shared: SharedState, connection_str: String, port: u16) {
    debug!("Starting port {port}");
}
