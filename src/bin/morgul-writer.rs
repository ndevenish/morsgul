use std::{
    io::Write,
    num::NonZeroU16,
    panic::{self, UnwindSafe},
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, bail};
use clap::Parser;
use colored::Colorize;
use epicars::providers::intercom::{Intercom, StringIntercom};
use epicars::{ServerBuilder, ServerHandle, providers::IntercomProvider};
use hdf5_sys::h5p::H5P_DEFAULT;
use indicatif::ProgressBar;
use morsgul::utils::{ball_spinner, watch_lifecycle};
use serde::Deserialize;
use time::macros::format_description;
use tokio::runtime::Runtime;
use tracing::{debug, error, info, level_filters::LevelFilter, warn};
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
    if !left.contains("://") {
        Ok((format!("tcp://{left}"), port))
    } else {
        Ok((left.to_string(), port))
    }
}

// class Header(BaseModel):
//     frameIndex: int
//     row: int
//     column: int
//     shape: Tuple[int, int]
//     detshape: Tuple[int, int]
//     bitmode: int
//     expLength: int
//     acquisition: int

//     @property
//     def hmi(self):
//         return self.detshape[1] * self.column + self.row

#[derive(Deserialize, Clone, Copy)]
struct Header {
    #[serde(rename = "frameIndex")]
    frame_index: u32,
    row: u8,
    column: u8,
    shape: (u16, u16),
    #[serde(rename = "detShape", alias = "detshape")]
    det_shape: (u16, u16),
    bitmode: u8,
    #[serde(rename = "expLength")]
    exposure_length: usize,
    acquisition: u32,
}

impl Header {
    fn get_module_index(&self) -> u16 {
        self.det_shape.0 * self.row as u16 + self.column as u16
    }
}
#[derive(Clone, Debug)]
struct SharedState {
    barrier: Arc<Barrier>,
    cancelled: Arc<AtomicBool>,
    listeners_ready: Arc<AtomicU16>,
    // condition_mutex: Arc<Mutex<bool>>,
    pv: SharedPV,
    state: mpsc::Sender<(u16, LifeCycleState)>,
}
impl SharedState {
    fn new(
        num_listeners: NonZeroU16,
        pv: SharedPV,
        state_sender: mpsc::Sender<(u16, LifeCycleState)>,
    ) -> Self {
        SharedState {
            barrier: Arc::new(Barrier::new(num_listeners.get() as usize)),
            cancelled: Arc::new(AtomicBool::new(false)),
            listeners_ready: Arc::new(AtomicU16::new(0)),
            state: state_sender,
            pv,
        }
    }
}

#[derive(Debug)]
struct ThreadState {
    data_transferred: Arc<AtomicUsize>,
    frames: Arc<AtomicUsize>,
}
impl ThreadState {
    fn new() -> Self {
        ThreadState {
            data_transferred: Arc::new(AtomicUsize::new(0)),
            frames: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// Holds PV accessors for threads to get shared info from
#[derive(Clone, Debug)]
struct SharedPV {
    filepath: StringIntercom,
    filename: StringIntercom,
    frames: Intercom<i32>,
    received_frames: Intercom<i32>,
    ready: Intercom<i8>,
    subfolder: Intercom<i8>,
}
impl SharedPV {
    pub fn get_filename_template(&self) -> PathBuf {
        let path: PathBuf = [
            self.filepath.load(),
            self.filename.load(), // format!(
                                  //     "{}_{{acquisition}}_{{module:02}}_{{index:06}}.h5",

                                  // ),
        ]
        .iter()
        .collect();

        path
    }
    /// Set the ready state. Only writes to PV if changed.
    pub fn set_ready(&mut self, ready: bool) {
        let val = if ready { 1 } else { 0 };
        if self.ready.load() != val {
            self.ready.store(&val);
        }
    }
    pub fn get_frames(&self) -> u32 {
        self.frames.load().max(0i32) as u32
    }
    pub fn set_received_frames(&mut self, frames: u32) {
        // self.frames.load().max(0i32) as u32
        self.frames.store(&(frames.min(i32::MAX as u32) as i32));
    }
}

async fn start_ca_server(prefix: &str) -> (ServerHandle, SharedPV) {
    info!("Starting IOC with prefix: {}", prefix.bold());
    let mut provider = IntercomProvider::new();
    provider.rbv = true;
    let pvs = SharedPV {
        filepath: provider
            .add_string_pv(
                &format!("{prefix}FilePath"),
                "/dls/i24/data/2025/cm40647-4/jungfrau/2025-09-16/test",
                Some(128),
            )
            .unwrap(),
        filename: provider
            .add_string_pv(&format!("{prefix}FileName"), "somewrite", Some(128))
            .unwrap(),
        frames: provider
            .add_pv(&format!("{prefix}NumCapture"), 3600i32)
            .unwrap(),
        received_frames: provider
            .add_pv(&format!("{prefix}NumCaptured"), 0i32)
            .unwrap(),
        ready: provider.add_pv(&format!("{prefix}Ready"), 0i8).unwrap(),
        subfolder: provider.add_pv(&format!("{prefix}Subfolder"), 0i8).unwrap(),
    };
    let server = ServerBuilder::new(provider).start().await.unwrap();
    let listen = server.listen_to_events();
    tokio::spawn(async move {
        watch_lifecycle(listen, false, true).await;
    });
    (server, pvs)
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

#[derive(Debug)]
enum LifeCycleState {
    Ready,
    Started {
        acquisition: u32,
        expected_frames: usize,
    },
    Frame {
        size: usize,
    },
    Complete {
        total_frames: usize,
    },
    NoUpdate,
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
    let (server, pvs) = runtime.block_on(async { start_ca_server(&opts.pv_prefix).await });

    // Make the shared communication object
    let (sender, state_rec) = mpsc::channel();
    let shared_state = SharedState::new(opts.listeners, pvs, sender);

    // Keep track of per-thread stats
    let mut status_data = Vec::new();
    let mut status_frames = Vec::new();

    let mut threads = Vec::new();
    for port in start_port..(start_port + opts.listeners.get()) {
        let conn_str = opts.connection_string.0.clone();
        // Pull out the cancellation token so that we can cancel it on panic
        let token: Arc<AtomicBool> = shared_state.cancelled.clone();
        let this_thread_state = shared_state.clone();
        let threadstat = ThreadState::new();
        status_data.push(threadstat.data_transferred.clone());
        status_frames.push(threadstat.frames.clone());
        threads.push(thread::spawn(move || {
            listener_thread_safety_handler(token, || {
                do_single_listener(this_thread_state, conn_str, port, port == start_port);
            });
        }));
    }
    #[derive(Debug)]
    enum BulkStates {
        Starting,
        Ready,
        Capturing,
        Complete,
    }

    let mut bulk_state = BulkStates::Starting;
    let mut expected_frames = 0usize;
    let mut count_ready = 0u16;
    let mut count_complete: u16 = 0u16;
    let mut frames_seen = 0usize;
    let mut bytes_written = 0usize;
    let mut spinner = ball_spinner();
    let mut last_update = Instant::now();
    let mut progress: Option<ProgressBar> = None;
    // While all the threads run, we coordinate the shared data/state
    while !shared_state.cancelled.load(Ordering::Relaxed) {
        let (_port, status_update) = match state_rec.recv_timeout(Duration::from_millis(100)) {
            Ok(x) => x,
            Err(RecvTimeoutError::Timeout) => (0, LifeCycleState::NoUpdate),
            Err(RecvTimeoutError::Disconnected) => break,
        };

        match bulk_state {
            BulkStates::Ready => match status_update {
                LifeCycleState::Started {
                    acquisition,
                    expected_frames: expected_frames_update,
                } => {
                    count_ready = 0;
                    expected_frames = expected_frames_update;
                    info!(
                        "Started acquisition {}, expect {} images",
                        acquisition.to_string().bright_cyan(),
                        expected_frames.to_string().bright_cyan()
                    );
                    progress = Some(ProgressBar::new(
                        expected_frames as u64 * (opts.listeners.get() as u64),
                    ));
                    info!(
                        "Writing to data files {}",
                        shared_state
                            .pv
                            .get_filename_template()
                            .to_string_lossy()
                            .purple()
                    );
                    bulk_state = BulkStates::Capturing;
                }
                LifeCycleState::NoUpdate => (),
                x => panic!("Received something other than Starting update when ready: {x:?}"),
            },
            BulkStates::Capturing => match status_update {
                LifeCycleState::Started { .. } => (),
                LifeCycleState::Frame { size } => {
                    frames_seen += 1;
                    bytes_written += size;
                    progress.as_ref().unwrap().inc(1);
                }
                LifeCycleState::Complete { .. } => {
                    count_complete += 1;
                    if count_complete == opts.listeners.get() {
                        bulk_state = BulkStates::Complete;
                        count_complete = 0;
                        progress.as_ref().unwrap().finish_and_clear();
                    }
                }
                LifeCycleState::NoUpdate => (),
                x => panic!("Unexpected message in Capturing state: {x:?}"),
            },
            BulkStates::Complete => match status_update {
                LifeCycleState::Ready => {
                    count_ready += 1;
                    if count_ready == opts.listeners.get() {
                        // Collection completely finished, do any global post here
                        info!(
                            "Collection complete. {frames_seen} frames sent in {bytes_written} bytes"
                        );
                        bulk_state = BulkStates::Ready;
                    }
                }
                LifeCycleState::NoUpdate => (),
                x => panic!("Unexpected status message when Complete: {x:?}"),
            },
            BulkStates::Starting => match status_update {
                LifeCycleState::Ready => {
                    count_ready += 1;
                    if count_ready == opts.listeners.get() {
                        bulk_state = BulkStates::Ready;
                        info!("Ready for acquisition");
                    }
                }
                LifeCycleState::NoUpdate => (),
                x => panic!("Unexpected status message when Starting: {x:?}"),
            },
        }
        if let BulkStates::Ready = bulk_state {
            if Instant::now() - last_update > Duration::from_millis(100) {
                print!("      {}\r", spinner.next().unwrap());
                let _ = std::io::stdout().flush();
                last_update = Instant::now();
            }
        };
    }

    // Now, wait for all threads to finish...
    for thread in threads {
        thread.join().unwrap();
    }

    // ... and close down the PV server
    runtime.block_on(async {
        let _ = server.stop().await;
    });

    info!("All threads terminated, closing.");
}

struct HDF5Writer {
    filename_template: PathBuf,
    header: Header,
    current_filename: Option<PathBuf>,
    current_file: Option<hdf5::File>,
    current_dataset: Option<hdf5::Dataset>,
    frames: usize,
}
fn create_hdf5_file(filename: &Path, frames: usize, header: Header) -> Result<hdf5::File> {
    let h5 = hdf5::File::create_excl(filename)?;
    h5.new_dataset::<f32>()
        .shape(())
        .create("exptime")?
        .write_scalar(&(header.exposure_length as f32 * 1e-9))?;
    h5.new_dataset::<u8>()
        .shape(())
        .create("row")?
        .write_scalar(&header.row)?;
    h5.new_dataset::<u8>()
        .shape(())
        .create("column")?
        .write_scalar(&header.column)?;
    h5.new_dataset::<f32>()
        .shape(())
        .create("timestamp")?
        .write_scalar(
            &(SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("System time is before unix epoch!")
                .as_secs_f32()),
        )?;
    let ds = h5
        .new_dataset_builder()
        .chunk((1usize, header.shape.0 as usize, header.shape.1 as usize))
        .add_filter(32008, &[0, 2])
        .empty::<u16>()
        .shape([frames, header.shape.0 as usize, header.shape.1 as usize])
        .create("data")?;

    Ok(h5)
}

impl HDF5Writer {
    fn new(filename_template: PathBuf, header: Header, frames: usize) -> Self {
        assert!(!filename_template.as_os_str().is_empty());
        HDF5Writer {
            filename_template,
            header,
            current_filename: None,
            current_file: None,
            current_dataset: None,
            frames,
        }
    }

    fn write_frame(&mut self, index: usize, data: &[u8]) -> Result<()> {
        // For when we want to roll over multiple files
        let file_index = 0;
        let filename = self
            .filename_template
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(PathBuf::from(format!(
                "{}_{acquisition}_{module:02}_{index:06}.h5",
                self.filename_template
                    .file_name()
                    .map(|v| v.to_string_lossy())
                    .unwrap_or(std::borrow::Cow::Borrowed("Unnamed")),
                acquisition = self.header.acquisition,
                module = self.header.get_module_index(),
                index = file_index
            )));
        if Some(&filename) != self.current_filename.as_ref() {
            // Close one if open already
            self.close();
            info!("Creating output file {}", filename.to_string_lossy());
            let file = create_hdf5_file(filename.as_path(), self.frames, self.header).unwrap();
            self.current_filename = Some(filename);
            self.current_dataset = Some(file.dataset("data")?);
            self.current_file = Some(file);
        }
        // Do a direct chunk write to this file
        if let Some(ref dataset) = self.current_dataset {
            let chunk_offset = [index as u64, 0u64, 0u64];
            unsafe {
                let status = hdf5_sys::h5d::H5Dwrite_chunk(
                    dataset.id(),
                    H5P_DEFAULT,
                    0u32,
                    chunk_offset.as_ptr(),
                    data.len(),
                    data.as_ptr() as *const _,
                );
                if status != 0 {
                    error!("Error writing chunk {chunk_offset:?}");
                    bail!(hdf5::Error::query().unwrap());
                }
            }
        }

        Ok(())
    }
    fn close(&mut self) {
        if let Some(file) = self.current_file.take() {
            let _ = file.close();
        }
        self.current_filename = None;
        self.current_dataset = None;
    }
}
impl Drop for HDF5Writer {
    fn drop(&mut self) {
        self.close();
    }
}

#[tracing::instrument(name = "listener", skip(shared, connection_str, is_first))]
fn do_single_listener(shared: SharedState, connection_str: String, port: u16, is_first: bool) {
    debug!("Starting port {port}");
    let context = zmq::Context::new();
    let socket = context.socket(zmq::PULL).unwrap();
    socket.set_rcvhwm(50000).unwrap();
    socket.connect(&format!("{connection_str}:{port}")).unwrap();

    while !shared.cancelled.load(Ordering::Relaxed) {
        socket.set_rcvtimeo(200).unwrap();
        let _ = shared.state.send((port, LifeCycleState::Ready));
        shared.barrier.wait();
        let messages = loop {
            match socket.recv_multipart(0) {
                Ok(messages) => break messages,
                Err(zmq::Error::EAGAIN) => {
                    // TODO: Handle case where this thread didn't start but others did
                    continue;
                }
                Err(x) => {
                    warn!("Got unexpected zeroMQ error: {x}");
                    continue;
                }
            }
        };
        let expected_frames = shared.pv.get_frames() as usize;
        assert_eq!(
            messages.len(),
            2,
            "Got unexpected number of messages ({})",
            messages.len()
        );
        let header: Header = serde_json::from_slice(messages.first().unwrap()).unwrap();
        let _ = shared.state.send((
            port,
            LifeCycleState::Started {
                acquisition: header.acquisition,
                expected_frames,
            },
        ));
        let _ = shared.state.send((
            port,
            LifeCycleState::Frame {
                size: messages[1].len(),
            },
        ));
        socket.set_rcvtimeo(2000).unwrap();
        // Start writing data, creating if necessary
        let mut writer =
            HDF5Writer::new(shared.pv.get_filename_template(), header, expected_frames);
        writer
            .write_frame(header.frame_index as usize, &messages[1])
            .unwrap();
        let mut num_images = 1;
        // Get the rest of the images now
        while !shared.cancelled.load(Ordering::Relaxed) && num_images < expected_frames {
            let messages = match socket.recv_multipart(0) {
                Ok(messages) => messages,
                Err(zmq::Error::EAGAIN) => {
                    info!(
                        "{port}: Got timeout waiting for more images. Saw {} images",
                        num_images.to_string().bright_cyan()
                    );
                    break;
                }
                Err(x) => panic!("Unexpected ~MQ err: {x}"),
            };
            let _ = shared.state.send((
                port,
                LifeCycleState::Frame {
                    size: messages[1].len(),
                },
            ));
            num_images += 1;
            let header: Header = serde_json::from_slice(messages.first().unwrap()).unwrap();
            writer
                .write_frame(header.frame_index as usize, &messages[1])
                .unwrap();
        }
        // We have finished frames for this collection
        writer.close();
        let _ = shared.state.send((
            port,
            LifeCycleState::Complete {
                total_frames: num_images,
            },
        ));
        shared.barrier.wait();
    }
}
