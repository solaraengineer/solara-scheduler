use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Job {
    pub name: String,
    pub command: String,
    pub path: String,
    pub interval: String,
}

#[derive(Debug, Deserialize)]
struct JobsConfig {
    jobs: Vec<Job>,
}

pub fn parse_jobs(path: &str) -> Result<Vec<Job>, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    let config: JobsConfig = toml::from_str(&content)?;
    Ok(config.jobs)
}