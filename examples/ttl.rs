use std::fs;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use diskcache::{DiskCache, NamespaceConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache_dir = std::env::temp_dir().join(format!(
        "diskcache-example-ttl-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));

    let cache = DiskCache::open(&cache_dir)?;
    let sessions = cache.namespace("sessions", NamespaceConfig::default())?;

    sessions.set(
        "session",
        &"token-123".to_string(),
        Some(Duration::from_secs(1)),
    )?;

    let before_expiry: Option<String> = sessions.get("session")?;
    println!("Before expiry: {before_expiry:?}");

    thread::sleep(Duration::from_millis(1200));

    let after_expiry: Option<String> = sessions.get("session")?;
    println!("After expiry: {after_expiry:?}");

    let _ = fs::remove_dir_all(&cache_dir);
    Ok(())
}
