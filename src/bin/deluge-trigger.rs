use std::net::{IpAddr, Ipv4Addr, UdpSocket};

use clap::Parser;
use morsgul::utils::DelugeTrigger;
use pnet::datalink;

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    exptime: f32,
    numimages: usize,
    #[arg(long, short, default_value = "9999")]
    port: u16,
}

fn get_broadcast_ips() -> Vec<Ipv4Addr> {
    datalink::interfaces()
        .into_iter()
        .filter(|i| !i.is_loopback())
        .flat_map(|i| i.ips.into_iter())
        .filter_map(|i| match i.broadcast() {
            IpAddr::V4(broadcast_ip) => Some(broadcast_ip),
            _ => None,
        })
        .collect()
}
fn main() {
    let args = Args::parse();
    let trig = DelugeTrigger {
        exptime: args.exptime,
        frames: args.numimages as u128,
        ..Default::default()
    };

    let buffer = bytemuck::bytes_of(&trig);
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.set_broadcast(true).unwrap();
    for addr in get_broadcast_ips().iter() {
        socket.send_to(buffer, (*addr, args.port)).unwrap();
    }
}
