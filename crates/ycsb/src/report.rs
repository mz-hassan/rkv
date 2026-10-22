//! Result files with honesty labels: bench/results/<ID>/<utc>/result.json
use serde_json::{json, Value};
use std::process::Command;

fn sh(cmd: &str, args: &[&str]) -> String {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

pub fn hardware() -> Value {
    let cpu = if cfg!(target_os = "macos") {
        sh("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        sh("sh", &["-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"])
    };
    let mem =
        if cfg!(target_os = "macos") { sh("sysctl", &["-n", "hw.memsize"]) } else { sh("sh", &["-c", "grep MemTotal /proc/meminfo | awk '{print $2*1024}'"]) };
    json!({
        "cpu": cpu, "memory_bytes": mem, "os": sh("uname", &["-sr"]), "arch": std::env::consts::ARCH,
        "cores": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
    })
}

pub fn git_commit() -> String {
    sh("git", &["rev-parse", "--short", "HEAD"])
}

pub fn utc_stamp() -> String {
    // seconds since epoch rendered as sortable string (no chrono dependency)
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    format!("{t}")
}

/// Write `<out>/<id>/<stamp>/result.json` and return the path.
pub fn write_result(out_dir: &str, id: &str, mut body: Value, labels: Value) -> std::io::Result<String> {
    let stamp = utc_stamp();
    let dir = format!("{out_dir}/{id}/{stamp}");
    std::fs::create_dir_all(&dir)?;
    body["id"] = json!(id);
    body["timestamp_unix"] = json!(stamp);
    body["git_commit"] = json!(git_commit());
    body["hardware"] = hardware();
    body["labels"] = labels;
    let path = format!("{dir}/result.json");
    std::fs::write(&path, serde_json::to_string_pretty(&body).unwrap())?;
    Ok(path)
}
