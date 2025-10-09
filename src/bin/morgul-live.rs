use clap::Parser;
use epicars::client::{Subscription, Watcher};
use itertools::multizip;
use morsgul::utils::{SlsDetectorHeader, SlsDetectorType, get_interface_addreses_with_prefix};
use nix::errno::Errno;
use nix::sys::socket::{
    ControlMessageOwned, MsgFlags, RecvMsg, SockaddrStorage, recvmsg, setsockopt, sockopt,
};

use socket2::{Domain, Socket, Type};
use std::io::{self, IoSliceMut, Write};
use std::iter;
use std::net::{SocketAddr, UdpSocket};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use thread_priority::set_current_thread_priority;

use std::thread::{self};
use std::time::{Duration, Instant};

const LISTENERS_PER_PORT: usize = 9;
const MODULE_SIZE_X: usize = 1024;
const MODULE_SIZE_Y: usize = 256;
const NUM_PIXELS: usize = MODULE_SIZE_X * MODULE_SIZE_Y;
const BIT_DEPTH: usize = 2;
const THREAD_IMAGE_BUFFER_LENGTH: usize = 10;

struct ReceiveImage {
    frame_number: u64,
    header: SlsDetectorHeader,
    received_packets: usize,
    data: Box<[u8]>,
}

impl std::fmt::Debug for ReceiveImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiveImage")
            .field("frame_number", &self.frame_number)
            .field("header", &self.header)
            .field("received_packets", &self.received_packets)
            .finish()
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    #[arg(long, short, default_value = "30000")]
    udp_port: u16,
    // #[arg(default_value = "36")]
    // listeners: u16,
}

fn allocate_image_buffer() -> Box<[u8]> {
    let mut empty_image = Vec::with_capacity(NUM_PIXELS * BIT_DEPTH);
    empty_image.resize(MODULE_SIZE_X * MODULE_SIZE_Y * BIT_DEPTH, 0u8);
    empty_image.into_boxed_slice()
}

static ACQUISITION_NUMBER: AtomicUsize = AtomicUsize::new(0usize);

#[derive(Debug, Default)]
struct AcquisitionStats {
    /// How many images have we seen at least one packet for
    images_seen: usize,
    /// How many images received all packet data
    complete_images: usize,
    /// How many packets were we expecting but didn't arrive
    packets_dropped: usize,
    /// How many packets did we get too late to assemble
    out_of_order: usize,
    /// How low did the image buffer queue length get?
    min_spare_image_buffers: Option<usize>,
}

/// For reporting ongoing progress/statistics to a central thread
#[derive(Debug)]
enum AcquisitionLifecycleState {
    /// An acquisition task is starting, along with the acquisition ID
    Starting {
        acquisition_number: usize,
        expected_images: usize,
    },
    ImageReceived {
        image_number: usize,
        dropped_packets: usize,
    },
    /// An acquisition was ended by a thread
    Ended(AcquisitionStats),
}

/// Start a UDP socket, with custom options
///
/// At the moment this is just
///   - Turn on RX
fn start_socket(address: SocketAddr, buffer_size: usize) -> std::io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
    socket.set_recv_buffer_size(buffer_size)?;
    socket.bind(&address.into())?;
    setsockopt(&socket, sockopt::RxqOvfl, &1)?;
    Ok(socket.into())
}

trait RecvMessageWrapper {
    fn get_dropped_packets(&self) -> nix::Result<usize>;
}
impl<'a, 's, S> RecvMessageWrapper for RecvMsg<'a, 's, S> {
    fn get_dropped_packets(&self) -> nix::Result<usize> {
        for cmsg in self.cmsgs()? {
            if let ControlMessageOwned::RxqOvfl(count) = cmsg {
                return Ok(count as usize);
            }
        }
        Ok(0)
    }
}

struct Receiver {
    spare_buffers: Vec<Box<[u8]>>,
    state_reporter: Sender<(u16, AcquisitionLifecycleState)>,
    expected_image_count: Watcher<i32>,
    udp_port: u16,
}

impl Receiver {
    fn start(
        port: u16,
        state_reporter: Sender<(u16, AcquisitionLifecycleState)>,
        expected_images: Watcher<i32>,
    ) -> ! {
        // Build the image data buffers we will use
        let spare_images: Vec<_> = std::iter::repeat_n((), THREAD_IMAGE_BUFFER_LENGTH)
            .map(|()| allocate_image_buffer())
            .collect();

        let mut recv = Receiver {
            spare_buffers: spare_images,
            state_reporter,
            expected_image_count: expected_images,
            udp_port: port,
        };
        recv.listen_port(port);
    }

    fn deliver_image(&mut self, image: ReceiveImage) {
        // for now, do nothing and just return the buffer to the pool
        self.spare_buffers.push(image.data);
        self.state_reporter
            .send((
                self.udp_port,
                AcquisitionLifecycleState::ImageReceived {
                    image_number: image.frame_number as usize,
                    dropped_packets: 64 - image.received_packets,
                },
            ))
            .unwrap();
    }

    fn listen_port(&mut self, port: u16) -> ! {
        let bind_addr: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
        let socket = start_socket(bind_addr, 512 * 1024 * 1024).unwrap();
        println!("{port}: Listening to 0.0.0.0");

        // The UDP receive buffer
        let mut buffer = [0u8; size_of::<SlsDetectorHeader>() + 8192];

        let fd = socket.as_raw_fd();
        let mut iov = [IoSliceMut::new(&mut buffer)];
        let mut cmsgspace = nix::cmsg_space!(libc::c_uint);

        loop {
            let mut stats = AcquisitionStats::default();
            let acquisition_number = ACQUISITION_NUMBER.load(Ordering::Relaxed);
            let mut is_first_image = true;

            // Wait forever for the first image in an acquisition
            socket.set_read_timeout(None).unwrap();

            // Potentially keep two images around; current and (incomplete)
            // previous image. If the current image is finished, then the
            // previous will also get flushed, but if a new image comes in
            // while the current is incomplete it will be demoted to the
            // previous image.
            let mut current_image: Option<ReceiveImage> = None;
            let mut previous_image: Option<ReceiveImage> = None;
            let mut expected_images = 0usize;
            // Many images in one acquisition
            loop {
                let msg = match recvmsg::<SockaddrStorage>(
                    fd,
                    &mut iov,
                    Some(&mut cmsgspace),
                    MsgFlags::empty(),
                ) {
                    Ok(msg) => msg,
                    Err(Errno::EAGAIN) => break,
                    Err(e) => {
                        panic!("Error: {e}");
                    }
                };

                // If the kernel reports that we dropped packets, report it
                if let Ok(dropped) = msg.get_dropped_packets()
                    && dropped > 0
                {
                    // We can't count these in the "packets lost" counter as
                    // we don't know how to avoid double-counting. But we
                    // should definitely notify
                    println!("{port}: Packet queue overflowed! {dropped} packets dropped!");
                }
                // Is this the start of a new acquisition?
                if is_first_image {
                    is_first_image = false;
                    expected_images = self
                        .expected_image_count
                        .borrow()
                        .expect("Tried to read expected images but not ready")
                        as usize;
                    // Once we have started an acquisition, we want to expire it when the images stop
                    socket
                        .set_read_timeout(Some(Duration::from_millis(500)))
                        .unwrap();
                    // Send a state update saying that we started
                    self.state_reporter
                        .send((
                            port,
                            AcquisitionLifecycleState::Starting {
                                acquisition_number,
                                expected_images,
                            },
                        ))
                        .unwrap();
                }

                // Unwrap the buffer data
                let buffer = msg.iovs().next().unwrap();

                let header: &SlsDetectorHeader =
                    bytemuck::from_bytes(&buffer[..size_of::<SlsDetectorHeader>()]);

                // Basic header validation
                assert!(
                    header.packet_number < 64,
                    "Got too many packets per image; are you running in half-module mode?"
                );
                assert!(msg.bytes - size_of::<SlsDetectorHeader>() == 8192);
                assert!(
                    header.det_type == SlsDetectorType::Jungfrau as u8,
                    "Unrecognised det_type in header: {} != Jungfrau ({})",
                    header.det_type,
                    SlsDetectorType::Jungfrau as u8
                );
                assert!(
                    header.version == 2,
                    "Unknown sls_detector_header version: {}",
                    header.version
                );

                // If new packet is for a new image, handle any previous, incomplete images
                if let Some(ref curr) = current_image
                    && header.frame_number != curr.header.frame_number
                {
                    if let Some(old_image) = previous_image.take() {
                        // Oh dear, this isn't going well, we have two
                        // incomplete previous images. Let's send off the
                        // oldest even though it is incomplete and count
                        // the dropped packets.
                        stats.packets_dropped += 64 - old_image.received_packets;
                        self.deliver_image(old_image);
                    }
                    previous_image = current_image.take()
                }

                // Get the current WIP image or make a new one
                let mut this_image = current_image.take().unwrap_or_else(|| {
                    stats.images_seen += 1;
                    ReceiveImage {
                        frame_number: header.frame_number,
                        header: *header,
                        received_packets: 0,
                        data: self
                            .spare_buffers
                            .pop()
                            .expect("Ran out of spare packet buffers"),
                    }
                });

                assert!(header.frame_number == this_image.frame_number);

                // Add a packet to this image
                this_image.received_packets += 1;
                // Copy the new data into the image data at the right place
                this_image.data[(header.packet_number as usize * 8192usize)
                    ..((header.packet_number as usize + 1) * 8192usize)]
                    .copy_from_slice(&buffer[size_of::<SlsDetectorHeader>()..]);

                // If we've received an entire image, then send it
                if this_image.received_packets == 64 {
                    stats.complete_images += 1;
                    self.deliver_image(this_image);
                    debug_assert!(current_image.is_none());
                } else {
                    // Push it back
                    current_image = Some(this_image);
                }

                if stats.complete_images == expected_images as usize {
                    break;
                }
            } // Acquisition loop

            // Handle any incomplete images
            if let Some(prev) = previous_image.take() {
                stats.packets_dropped += 64 - prev.received_packets;
                self.deliver_image(prev);
            }
            if let Some(curr) = current_image.take() {
                stats.packets_dropped += 64 - curr.received_packets;
                self.deliver_image(curr);
            }
            println!(
                "{port}: End of acquisition, seen {is} images, {ci} complete, {pd} packets dropped, {ooo} out-of-order.",
                is = stats.images_seen,
                ci = stats.complete_images,
                pd = stats.packets_dropped,
                ooo = stats.out_of_order
            );
            self.state_reporter
                .send((port, AcquisitionLifecycleState::Ended(stats)))
                .unwrap();
            continue;
        }
    }
}
fn main() {
    let args = Args::parse();
    println!("Args: {args:?}");

    let interfaces = get_interface_addreses_with_prefix(192);
    if interfaces.is_empty() {
        println!("Error: Could not find any 192. interfaces. Have you set up the network?");
        std::process::exit(1);
    }
    let num_listeners = interfaces.len() * LISTENERS_PER_PORT;

    // Get a list of cores so that we can set affinity to them
    let mut core_ids = core_affinity::get_core_ids().unwrap().into_iter().rev();

    let (state_tx, state_rx) = mpsc::channel::<(u16, AcquisitionLifecycleState)>();

    // Start the CA receiver
    // let client = epicars::Client::new().unw
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(epicars::Client::new()).unwrap();
    let num_frames = client.watch::<i32>("BL24I-JUNGFRAU-META:FD:NumCapture");

    // let client = tokio::task::spawn_blocking({ epicars::Client::new() });
    let mut threads = Vec::new();

    for (port, _address) in multizip((
        args.udp_port..(args.udp_port + num_listeners as u16),
        interfaces
            .iter()
            .flat_map(|x| iter::repeat_n(*x, LISTENERS_PER_PORT)),
    )) {
        let core = core_ids.next().unwrap();
        let stat = state_tx.clone();
        let num_frames_inner = num_frames.clone();
        threads.push(thread::spawn(move || {
            if !core_affinity::set_for_current(core) {
                println!("{port}: Failed to set affinity to core {}", core.id);
            } else {
                println!("{port}: Setting affinity to CPU {}", core.id);
            }
            if set_current_thread_priority(thread_priority::ThreadPriority::Max).is_err() {
                println!(
                    "{port}: Warning: Could not set thread priority. Are you running as root?"
                );
            };

            Receiver::start(port, stat, num_frames_inner);
        }));
    }

    let mut currently_active = 0usize;
    let mut high = 0usize;
    let mut start_time = Instant::now();
    let mut last_update = Instant::now() - Duration::from_secs(100);
    let mut num_images_seen = 0usize;
    let mut total_expected_images = 0usize;
    let mut acquisition_number = 0usize;
    let mut packets_dropped = 0usize;
    loop {
        match state_rx.recv().unwrap() {
            (
                _port,
                AcquisitionLifecycleState::Starting {
                    acquisition_number: acq_number,
                    expected_images,
                },
            ) => {
                if currently_active == 0 {
                    // The first image of the acquisition
                    println!(
                        "Starting acquisition {} with {} expected images",
                        acquisition_number, expected_images
                    );
                    start_time = Instant::now();
                    num_images_seen = 0;
                    total_expected_images = expected_images * num_listeners;
                    acquisition_number = acq_number;
                    packets_dropped = 0;
                }
                currently_active += 1;
                high = std::cmp::max(currently_active, high);
            }
            (_port, AcquisitionLifecycleState::Ended(_stats)) => {
                currently_active -= 1;
                if currently_active == 0 {
                    let elapsed = Instant::now() - start_time;
                    println!(
                        "Acquisition finished in {:.2} s. {high} streams participated. {} packets dropped.",
                        elapsed.as_secs_f32(),
                        packets_dropped,
                    );
                    high = 0;
                    ACQUISITION_NUMBER.fetch_add(1, Ordering::Relaxed);
                }
            }
            (
                _,
                AcquisitionLifecycleState::ImageReceived {
                    image_number: _,
                    dropped_packets,
                },
            ) => {
                num_images_seen += 1;
                if (Instant::now() - last_update).as_millis() > 100 {
                    last_update = Instant::now();
                    print!(
                        " {acquisition_number}: {:5.1} %\r",
                        100.0f32 * (num_images_seen as f32 / total_expected_images as f32)
                    );
                    let _ = io::stdout().flush();
                }
                packets_dropped += dropped_packets;
            }
        }
        // thread::sleep(Duration::from_secs(20));
    }
    // #[allow(clippy::never_loop)]
    // for thread in threads {
    //     thread.join().unwrap();
    // }
    // thread::spawn(f)
    // let ip = vec![
    //     "192.168.201.101",
    //     "192.168.202.101",
    //     "192.168.203.101",
    //     "192.168.204.101",
    // ];
}
