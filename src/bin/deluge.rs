//! Deluge
//!
//! Flood a target udp-port-only slsDetector receiver with data
use std::{
    io::{self, Write},
    iter::{self},
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{Arc, Barrier},
    thread::{self},
    time::{Duration, Instant},
};

use bytemuck::{Zeroable, bytes_of};
use clap::Parser;
use itertools::multizip;
use morsgul::utils::{DelugeTrigger, SlsDetectorHeader, get_interface_addreses_with_prefix};
use socket2::Protocol;

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    /// The first target port to send data to
    #[arg(long, short, default_value = "30000")]
    target_port: u16,

    targets: Vec<Ipv4Addr>,

    /// The port to listen for broadcast triggers on
    #[arg(default_value = "9999", long)]
    trigger_port: u16,
}

fn send_data(
    source_address: &Ipv4Addr,
    target_address: &Ipv4Addr,
    target_port: u16,
    sync: Arc<Barrier>,
    mut trigger: bus::BusReader<DelugeTrigger>,
) -> ! {
    let bind_addr: SocketAddr = format!("{source_address}:0").parse().unwrap();
    let to_addr: SocketAddr = format!("{target_address}:{target_port}").parse().unwrap();
    let socket = UdpSocket::bind(bind_addr).unwrap();
    let mut buff = vec![0u8; 8192 + size_of::<SlsDetectorHeader>()];
    let mut header = SlsDetectorHeader::zeroed();

    sync.wait();
    loop {
        let acq = trigger.recv().unwrap();
        println!(
            "{target_port}: Starting {} images at {:.0} Hz",
            acq.frames,
            1.0 / acq.exptime
        );
        // println!("{target_port}: Starting send");
        let start_acq = Instant::now();
        for image_num in 0..acq.frames {
            let acq_elapsed = (Instant::now() - start_acq).as_secs_f32();
            if acq_elapsed < image_num as f32 * acq.exptime {
                thread::sleep(Duration::from_secs_f32(
                    image_num as f32 * acq.exptime - acq_elapsed,
                ));
            }
            for _ in 0..64 {
                buff[..size_of::<SlsDetectorHeader>()].copy_from_slice(bytes_of(&header));

                socket.send_to(&buff, to_addr).unwrap();
                header.packet_number += 1;
            }

            header.frame_number += 1;
            header.packet_number = 0;
        }
        println!("{target_port}: Sent {} images", acq.frames);
        std::io::stdout().flush().unwrap();
        let sync_result = sync.wait();
        if sync_result.is_leader() {
            println!(
                "Sent {} images in {:.0} ms",
                acq.frames,
                (Instant::now() - start_acq).as_millis()
            );
        }
    }
}

pub fn new_reusable_udp_socket<T: std::net::ToSocketAddrs>(
    address: T,
) -> io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(Protocol::UDP),
    )?;
    socket.set_reuse_port(true)?;
    // socket.set_nonblocking(true)?;
    let addr = address.to_socket_addrs()?.next().unwrap();
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

fn main() {
    let args = Args::parse();

    println!("{args:?}");

    let interfaces = get_interface_addreses_with_prefix(192);
    if interfaces.is_empty() {
        println!("Error: Could not find any 192. interfaces. Have you set up the network?");
        std::process::exit(1);
    }
    // // Get a list of cores so that we can set affinity to them
    // let mut core_ids = core_affinity::get_core_ids().unwrap().into_iter().rev();
    // println!("{core_ids:?}");
    // println!("Start threads");

    let mut threads = Vec::new();

    let barrier = Arc::new(Barrier::new(interfaces.len() * 4));
    let mut bus = bus::Bus::new(1);

    for (port, source, target) in multizip((
        args.target_port..(args.target_port + interfaces.len() as u16 * 4),
        interfaces.iter().flat_map(|x| iter::repeat_n(*x, 4)),
        args.targets,
    )) {
        println!("Starting {source} -> {target}:{port}");
        let bar = barrier.clone();
        let trig = bus.add_rx();
        threads.push(thread::spawn(move || {
            send_data(&source, &target, port, bar, trig);
        }));
    }

    // drop(trigger_rx);
    // Wait for broadcasts
    let mut buf = vec![0; size_of::<DelugeTrigger>()];
    let broad = new_reusable_udp_socket("0.0.0.0:9999").unwrap();
    // let broad = UdpSocket::bind("0.0.0.0:9999").unwrap();
    // broad.recv(buf)
    // let mut last_trigger = None;
    let mut last_trigger: Option<DelugeTrigger> = None;
    loop {
        if let Ok(size) = broad.recv(buf.as_mut_slice()) {
            assert!(size == size_of::<DelugeTrigger>());
            let trigger: &DelugeTrigger = bytemuck::from_bytes(&buf);

            // Ignore retriggers with the same UUID
            if let Some(last) = last_trigger
                && last.uuid == trigger.uuid
            {
                continue;
            }

            bus.broadcast(*trigger);

            last_trigger = Some(*trigger);
        }
    }
}
