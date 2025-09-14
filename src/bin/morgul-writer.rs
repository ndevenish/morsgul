use core::num;
use std::{
    collections::HashMap,
    num::NonZeroU16,
    panic::{self, UnwindSafe},
    path::PathBuf,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use clap::Parser;
use colored::Colorize;
use epicars::{
    Server, ServerEvent,
    providers::intercom::{Intercom, StringIntercom},
};
use epicars::{ServerBuilder, ServerHandle, providers::IntercomProvider};
use morsgul::utils::watch_lifecycle;
use time::macros::format_description;
use tokio::{runtime::Runtime, sync::broadcast::error::RecvError};
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
    pv: SharedPV,
}
impl SharedState {
    fn new(num_listeners: NonZeroU16, pv: SharedPV) -> Self {
        SharedState {
            barrier: Arc::new(Barrier::new(num_listeners.get() as usize)),
            cancelled: Arc::new(AtomicBool::new(false)),
            // condition_mutex: Arc::new(Mutex::new(false)),
            pv,
        }
    }
}

// let mut provider = IntercomProvider::new();
// //provider.prefix = "BL24I-JUNGFRAU-META:FD:".to_string();
// provider.rbv = true;

// let _pv_count = provider
//     .add_pv("BL24I-JUNGFRAU-META:FD:NumCapture", 0i32)
//     .unwrap();
// let _pv_count_captured = provider
//     .add_pv("BL24I-JUNGFRAU-META:FD:NumCaptured", 0i32)
//     .unwrap();
// let _pv_subfolder = provider
//     .add_pv("BL24I-JUNGFRAU-META:FD:Subfolder", 0i8)
//     .unwrap();
// let _pv_ready = provider
//     .add_pv("BL24I-JUNGFRAU-META:FD:Ready", 0i8)
//     .unwrap();
/// Holds PV accessors for threads to get shared info from
#[derive(Clone, Debug)]
struct SharedPV {
    filepath: StringIntercom,
    filename: StringIntercom,
    frames: Intercom<i32>,
    ready: Intercom<i8>,
}
impl SharedPV {
    pub fn get_filename_template(&self) -> PathBuf {
        let path: PathBuf = [
            self.filepath.load(),
            format!(
                "{}_{{acquisition}}_{{module:02}}_{{index:06}}.h5",
                self.filename.load()
            ),
        ]
        .iter()
        .collect();

        path
    }
    pub fn set_ready(&mut self, ready: bool) {
        self.ready.store(if ready { &1 } else { &0 });
    }
    pub fn get_frames(&self) -> u32 {
        self.frames.load().max(0i32) as u32
    }
}

fn start_ca_server(prefix: &str) -> (ServerHandle, SharedPV) {
    info!("Starting IOC with prefix: {prefix}");
    let mut provider = IntercomProvider::new();
    provider.rbv = true;
    let pvs = SharedPV {
        filepath: provider
            .add_string_pv(&format!("{prefix}FilePath"), "", Some(128))
            .unwrap(),
        filename: provider
            .add_string_pv(&format!("{prefix}FileName"), "", Some(128))
            .unwrap(),
        frames: provider
            .add_pv(&format!("{prefix}NumCapture"), 0i32)
            .unwrap(),
        ready: provider.add_pv(&format!("{prefix}Ready"), 0i8).unwrap(),
    };
    let server = ServerBuilder::new(provider).start();
    let listen = server.listen_to_events();
    tokio::spawn(async move {
        watch_lifecycle(listen, false, true).await;
    });
    (server, pvs)
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
    let runtime = Runtime::new().unwrap();
    let (server, pvs) = runtime.block_on(async { start_ca_server(&opts.pv_prefix) });

    // Make the shared communication object
    let shared_state = SharedState::new(opts.listeners, pvs);

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

    // Close down the PV server
    runtime.block_on(async {
        let _ = server.stop().await;
    });

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
