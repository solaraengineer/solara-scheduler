use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

use crate::job::Job;

pub struct ActiveJob {
    pub job: Job,
    pub handle: tokio::task::JoinHandle<()>,
}

pub struct Daemon {
    pub active_jobs: Arc<Mutex<HashMap<String, ActiveJob>>>,
    pub socket_path: String,
    pub log_path: String,
}

impl Daemon {
    pub fn new(socket_path: &str, log_path: &str) -> Self {
        Daemon {
            active_jobs: Arc::new(Mutex::new(HashMap::new())),
            socket_path: socket_path.to_string(),
            log_path: log_path.to_string(),
        }
    }

    pub async fn start(&self, jobs_toml_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        // clean up old socket file if it exists
        let _ = std::fs::remove_file(&self.socket_path);

        let listener = UnixListener::bind(&self.socket_path)?;
        println!("[daemon] listening on {}", self.socket_path);

        let active_jobs = self.active_jobs.clone();
        let jobs_toml = jobs_toml_path.to_string();
        let log_path = self.log_path.clone();

        loop {
            let (stream, _) = listener.accept().await?;
            let active_jobs = active_jobs.clone();
            let jobs_toml = jobs_toml.clone();
            let log_path = log_path.clone();

            tokio::spawn(async move {
                let (reader, mut writer) = stream.into_split();
                let mut reader = BufReader::new(reader);
                let mut line = String::new();

                if reader.read_line(&mut line).await.is_err() {
                    return;
                }

                let parts: Vec<&str> = line.trim().splitn(2, ' ').collect();
                let command = parts[0];

                let response = match command {
                    "job" => {
                        if parts.len() < 2 {
                            "error: missing job name\n".to_string()
                        } else {
                            let job_name = parts[1];
                            handle_job_start(job_name, &jobs_toml, &log_path, &active_jobs).await
                        }
                    }
                    "joboff" => {
                        if parts.len() < 2 {
                            "error: missing job name\n".to_string()
                        } else {
                            let job_name = parts[1];
                            handle_job_stop(job_name, &active_jobs).await
                        }
                    }
                    "jobs" => handle_jobs_list(&active_jobs).await,
                    _ => format!("error: unknown command '{}'\n", command),
                };

                let _ = writer.write_all(response.as_bytes()).await;
            });
        }
    }
}

async fn handle_job_start(
    job_name: &str,
    jobs_toml: &str,
    log_path: &str,
    active_jobs: &Arc<Mutex<HashMap<String, ActiveJob>>>,
) -> String {
    // check if already running
    {
        let jobs = active_jobs.lock().await;
        if jobs.contains_key(job_name) {
            return format!("error: '{}' is already running\n", job_name);
        }
    }

    // parse jobs.toml and find the job
    let jobs = match crate::job::parse_jobs(jobs_toml) {
        Ok(j) => j,
        Err(e) => return format!("error: failed to parse jobs.toml: {}\n", e),
    };

    let job = match jobs.into_iter().find(|j| j.name == job_name) {
        Some(j) => j,
        None => return format!("not found job check format or create it\n"),
    };

    let interval_secs = match parse_interval(&job.interval) {
        Some(s) => s,
        None => return format!("error: invalid interval '{}' for job '{}'\n", job.interval, job_name),
    };

    let job_clone = job.clone();
    let log_path = log_path.to_string();
    let name = job_name.to_string();

    let handle = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(interval_secs));

        loop {
            ticker.tick().await;
            run_job(&job_clone, &log_path).await;
        }
    });

    let active_job = ActiveJob {
        job: job.clone(),
        handle,
    };

    let mut jobs_map = active_jobs.lock().await;
    jobs_map.insert(name.clone(), active_job);

    format!("started '{}' — runs every {}\n", name, job.interval)
}

async fn handle_job_stop(
    job_name: &str,
    active_jobs: &Arc<Mutex<HashMap<String, ActiveJob>>>,
) -> String {
    let mut jobs = active_jobs.lock().await;

    match jobs.remove(job_name) {
        Some(active_job) => {
            active_job.handle.abort();
            format!("stopped '{}'\n", job_name)
        }
        None => format!("error: '{}' is not running\n", job_name),
    }
}

async fn handle_jobs_list(
    active_jobs: &Arc<Mutex<HashMap<String, ActiveJob>>>,
) -> String {
    let jobs = active_jobs.lock().await;

    if jobs.is_empty() {
        return "no active jobs\n".to_string();
    }

    let mut output = String::new();
    for (name, active_job) in jobs.iter() {
        output.push_str(&format!(
            "{} — {} — every {}\n",
            name, active_job.job.command, active_job.job.interval
        ));
    }
    output
}

async fn run_job(job: &Job, log_path: &str) {
    let path = PathBuf::from(&job.path);

    let result = Command::new("sh")
        .arg("-c")
        .arg(&job.command)
        .current_dir(&path)
        .output()
        .await;

    match result {
        Ok(output) => {
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let msg = format!(
                    "[{}] FAIL '{}': exit code {} — {}\n",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                    job.name,
                    output.status.code().unwrap_or(-1),
                    stderr.trim()
                );
                println!("{}", msg.trim());
                append_log(log_path, &msg);
            } else {
                println!(
                    "[{}] OK '{}'",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                    job.name
                );
            }
        }
        Err(e) => {
            let msg = format!(
                "[{}] FAIL '{}': {}\n",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                job.name,
                e
            );
            println!("{}", msg.trim());
            append_log(log_path, &msg);
        }
    }
}

fn append_log(log_path: &str, msg: &str) {
    use std::fs::OpenOptions;
    use std::io::Write;

    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        let _ = file.write_all(msg.as_bytes());
    }
}

fn parse_interval(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.ends_with('h') {
        s[..s.len() - 1].parse::<u64>().ok().map(|h| h * 3600)
    } else if s.ends_with('m') {
        s[..s.len() - 1].parse::<u64>().ok().map(|m| m * 60)
    } else {
        None
    }
}