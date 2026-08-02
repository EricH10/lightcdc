#![cfg(unix)]

use std::{fs, process::Command, thread, time::Duration};

use tempfile::TempDir;

#[test]
fn sigterm_stops_the_connector_cleanly() {
    let temp = TempDir::new().expect("temp dir");
    let config = temp.path().join("redis.toml");
    fs::write(
        &config,
        r#"
[lightcdc]
endpoint = "http://127.0.0.1:59999"
stream = "orders"
consumer = "signal-test"
reconnect_initial_ms = 100
reconnect_max_ms = 100

[redis]
url = "redis://127.0.0.1:59998"

[[rules]]
table = "public.orders"
key = "order:{id}"
action = "invalidate"
"#,
    )
    .expect("write connector config");
    let mut child = Command::new(env!("CARGO_BIN_EXE_lightcdc-redis"))
        .args(["--config", config.to_str().expect("UTF-8 config path")])
        .spawn()
        .expect("start Redis connector");
    thread::sleep(Duration::from_secs(1));

    let signal = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(signal.success(), "kill command failed with {signal}");

    let status = child.wait().expect("wait for Redis connector");
    assert!(status.success(), "connector exited with {status}");
}
