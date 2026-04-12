mod daemon;
mod job;

use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const SOCKET_PATH: &str = "/tmp/solara-scheduler.sock";
const JOBS_TOML: &str = "/etc/solara-sch/jobs.toml";
const LOG_PATH: &str = "/home/ubuntu/solaradocs/tasks.log";

#[derive(Parser)]
#[command(name = "sch", about = "solara-scheduler")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the daemon (control plane)
    Daemon,
    /// Start a job by name
    Job { name: String },
    /// Stop a job by name
    Joboff { name: String },
    /// List all active jobs
    Jobs,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Daemon => {
            let d = daemon::Daemon::new(SOCKET_PATH, LOG_PATH);
            if let Err(e) = d.start(JOBS_TOML).await {
                eprintln!("[daemon] fatal: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Job { name } => {
            send_command(&format!("job {}", name)).await;
        }
        Commands::Joboff { name } => {
            send_command(&format!("joboff {}", name)).await;
        }
        Commands::Jobs => {
            send_command("jobs").await;
        }
    }
}

async fn send_command(cmd: &str) {
    let stream = match UnixStream::connect(SOCKET_PATH).await {
        Ok(s) => s,
        Err(_) => {
            eprintln!("error: daemon is not running");
            std::process::exit(1);
        }
    };

    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let _ = writer.write_all(format!("{}\n", cmd).as_bytes()).await;
    let _ = writer.shutdown().await;

    let mut response = String::new();
    while reader.read_line(&mut response).await.unwrap_or(0) > 0 {}

    print!("{}", response);
}