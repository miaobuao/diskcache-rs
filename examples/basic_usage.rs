use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use diskcache::{DiskCache, DiskCacheConfig};
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
struct UserProfile {
    name: String,
    score: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache_dir = std::env::temp_dir().join(format!(
        "diskcache-example-basic-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));

    let cache = DiskCache::open(&cache_dir, DiskCacheConfig::default())?;

    let profile = UserProfile {
        name: "alice".to_string(),
        score: 42,
    };

    cache.set("user:1", &profile, None)?;
    let loaded: Option<UserProfile> = cache.get("user:1")?;

    println!("Loaded profile: {loaded:?}");

    cache.remove("user:1")?;
    cache.persist()?;

    let _ = fs::remove_dir_all(&cache_dir);
    Ok(())
}
