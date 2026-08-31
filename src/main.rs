use clap::Parser;
use tracing_subscriber::EnvFilter;

use overture_stac::Result;

mod cli;

use cli::Command;

/// Generate a STAC index for Overture Maps data from the public release bucket.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    if let Err(e) = run().await {
        // Display formatting (respects thiserror's #[error("…")]) — plus the source
        // chain when Context wraps something meaningful.
        eprintln!("Error: {e}");
        let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&e);
        while let Some(s) = src {
            eprintln!("  Caused by: {s}");
            src = s.source();
        }
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    Cli::parse().command.run().await
}
