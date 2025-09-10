use clap::Parser;
use epicars::{providers::IntercomProvider, ServerBuilder};
use tokio::select;
use tracing::level_filters::LevelFilter;

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
        .init();

    let mut provider = IntercomProvider::new();
    provider.prefix = "BL24I-JUNGFRAU-META:FD:".to_string();
    provider.rbv = true;

    let _pv_path = provider.add_string_pv("FilePath", "", Some(128)).unwrap();
    let _pv_name = provider.add_string_pv("FileName", "", Some(128)).unwrap();
    let _pv_count = provider.add_pv("NumCapture", 0i32).unwrap();
    let _pv_count_captured = provider.add_pv("NumCaptured", 0i32).unwrap();
    let _pv_subfolder = provider.add_pv("Subfolder", 0i8).unwrap();

    let mut server = ServerBuilder::new(provider).start();

    select! {
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
