use std::{fs, path::Path, process::Command};

use tempfile::TempDir;

#[test]
fn invalid_configuration_exits_with_stable_code() {
    let temp = TempDir::new().expect("temp dir");
    let status = Command::new(env!("CARGO_BIN_EXE_lightcdc"))
        .args(["inspect", "--config"])
        .arg(temp.path().join("missing.toml"))
        .status()
        .expect("run lightcdc");

    assert_eq!(status.code(), Some(10));
}

#[test]
fn unsafe_durable_state_exits_with_stable_code() {
    let temp = TempDir::new().expect("temp dir");
    let config = write_config(temp.path());
    fs::write(
        temp.path().join("lightcdc.redb.format"),
        "lightcdc-control-format=999\n",
    )
    .expect("write future format marker");

    let status = Command::new(env!("CARGO_BIN_EXE_lightcdc"))
        .args(["inspect", "--config"])
        .arg(config)
        .status()
        .expect("run lightcdc");

    assert_eq!(status.code(), Some(20));
}

#[test]
fn ordinary_runtime_failure_uses_generic_code() {
    let temp = TempDir::new().expect("temp dir");
    let config = write_config(temp.path());

    let status = Command::new(env!("CARGO_BIN_EXE_lightcdc"))
        .args(["inspect", "--config"])
        .arg(config)
        .args(["--sequence", "1"])
        .status()
        .expect("run lightcdc");

    assert_eq!(status.code(), Some(1));
}

fn write_config(data_dir: &Path) -> std::path::PathBuf {
    let config = data_dir.join("lightcdc.toml");
    fs::write(
        &config,
        format!(
            r#"
[source]
name = "default"
host = "localhost"
port = 5432
database = "lightcdc"
user = "lightcdc"
password = "secret"
tls_mode = "disable"
publication = "publication"
slot = "slot"

[runtime]
data_dir = {data_dir:?}
storage_file = "lightcdc.redb"
channel_capacity = 32
shutdown_timeout_ms = 1000

[logging]
level = "info"

[[streams]]
name = "orders"
source = "default"
tables = ["public.orders"]
"#,
            data_dir = data_dir.display().to_string()
        ),
    )
    .expect("write config");
    config
}
