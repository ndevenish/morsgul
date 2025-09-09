use std::num::NonZeroU16;

use clap::Parser;
use tracing::level_filters::LevelFilter;

// const DEFAULT_PATH_PV: &str = "BL24I-EA-EIGER-01:OD:FilePath_RBV";
// const DEFAULT_NAME_PV: &str = "BL24I-EA-EIGER-01:OD:FP:FileName_RBV";
// const DEFAULT_COUNT_PV: &str = "BL24I-EA-EIGER-01:OD:NumCapture";

#[derive(Parser, Debug)]
struct Args {
    /// The ZeroMQ connection string, including port
    #[clap(value_parser = parse_zmq_address_and_port)]
    connection_string: (String, u16),
    /// The number of listeners to launch.
    listeners: NonZeroU16,
    /// Show various levels of debug output
    #[clap(short, action = clap::ArgAction::Count)]
    verbose: u8,
    /// Prefix for PV exposed via epics
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

fn main() {
    let opts = Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(match opts.verbose {
            0 => LevelFilter::INFO,
            1 => LevelFilter::DEBUG,
            2.. => LevelFilter::TRACE,
        })
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
            | |     / /____(_) /____  _____
            | | /| / / ___/ / __/ _ \/ ___/
            | |/ |/ / /  / / /_/  __/ /
            |__/|__/_/  /_/\__/\___/_/"#
    );
    println!("{opts:?}");
}
