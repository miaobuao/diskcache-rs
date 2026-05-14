use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use diskcache::{DiskCache, DiskCacheConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache_dir = std::env::temp_dir().join(format!(
        "diskcache-example-blob-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));

    let cache = DiskCache::open(
        &cache_dir,
        DiskCacheConfig {
            inline_threshold_bytes: 64,
        },
    )?;

    let large_payload = "x".repeat(32 * 1024);
    cache.set("payload", &large_payload, None)?;

    let blob_path = cache.blob_path_for_key("payload");
    println!("Blob path: {}", blob_path.display());
    println!("Blob exists: {}", blob_path.exists());

    let loaded: Option<String> = cache.get("payload")?;
    println!("Loaded bytes: {}", loaded.as_deref().map_or(0, str::len));

    cache.remove("payload")?;
    let _ = fs::remove_dir_all(&cache_dir);
    Ok(())
}
