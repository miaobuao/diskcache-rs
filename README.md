# diskcache

Disk-backed, namespace-first KV cache in Rust using [fjall](https://github.com/fjall-rs/fjall) and [rkyv](https://github.com/rkyv/rkyv). Inspired by Python's `diskcache`.

Every value belongs to a namespace. Each namespace is backed by its own fjall keyspace, and each record is stored as a versioned rkyv envelope with optional TTL metadata.

Large-value separation is delegated to fjall's KV separation. diskcache does not manage external value files.

## Features

- **Namespace-first API** - all `set`/`get`/`remove` operations happen through `CacheNamespace`
- **Type-safe** - generic `set`/`get` for any `rkyv`-serializable type
- **TTL** - per-key expiry checked at `get` time and by fjall compaction filters
- **Namespace config** - caller passes fjall `KeyspaceCreateOptions` per namespace open
- **Versioned envelope** - `RecordEnvelope::V1` with `StoredValueV1::Inline` for future extension
- **Thread-safe** - `DiskCache` and `CacheNamespace` are `Send + Sync`, shareable via `Arc`

## On-disk layout

```text
<root>/
  db/
    meta          namespace name -> internal namespace id
    ns_<id>/      one fjall keyspace per namespace
```

Namespace names are stored in `meta` as `name -> internal namespace id`. Internal namespace IDs are used for fjall keyspace names, keeping user-facing names separate from storage naming.

## Quickstart

```rust
use diskcache::{DiskCache, NamespaceConfig};
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
struct UserProfile {
    name: String,
    score: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache_dir = std::env::temp_dir().join("diskcache-demo");
    let cache = DiskCache::open(&cache_dir)?;
    let users = cache.namespace("users", NamespaceConfig::default())?;

    let profile = UserProfile { name: "alice".into(), score: 42 };
    users.set("1", &profile, None)?;

    let loaded: Option<UserProfile> = users.get("1")?;
    assert_eq!(Some(profile), loaded);

    users.remove("1")?;
    Ok(())
}
```

## API

### Open

```rust
let cache = DiskCache::open(cache_path)?;
```

### Namespaces

```rust
use fjall::{KeyspaceCreateOptions, KvSeparationOptions};

let users = cache.namespace("users", NamespaceConfig::default())?;
let sessions = cache.namespace("sessions", NamespaceConfig {
    keyspace_create_options: KeyspaceCreateOptions::default().with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(8 * 1024),
    )),
})?;

let names = cache.list_namespaces()?;
let deleted = cache.delete_namespace("sessions")?;
```

`namespace(name, config)` opens an existing namespace or creates it if it does not exist. `meta` stores only the namespace ID; `config` is provided by the caller for each open call.

`delete_namespace(name)` deletes the namespace keyspace and metadata record, returning `false` when the namespace does not exist.

### Set / Get / Remove / Contains

```rust
users.set("key", &value, None)?;
users.set("key", &value, Some(Duration::from_secs(60)))?;

let v: Option<MyType> = users.get("key")?;

users.remove("key")?;

let exists = users.contains_key("key")?; // respects TTL
let removed = users.clear()?; // removes every key in this namespace
```

### Persist

```rust
cache.persist()?; // fsync all pending writes
```

## Error handling

```rust
pub enum DiskCacheError {
    Io(std::io::Error),
    Fjall(fjall::Error),
    Serialize(String),
    Deserialize(String),
    InvalidNamespaceName(String),
}
```

## TTL

TTL is stored as an `expires_at_ms` timestamp. Enforcement happens in two places:

1. **Get-time** - `get` and `contains_key` check expiry before returning data; expired entries trigger cleanup.
2. **Compaction filter** - fjall's `CompactionFilter` drops expired records during LSM merges for namespace keyspaces.

The compaction filter returns `Keep` on parse failure, defensively avoiding data loss from version skew.

## Run examples

```bash
cargo run --example basic_usage
cargo run --example ttl
```

## Benchmarks

```bash
# all benchmarks
cargo bench

# concurrent only
cargo bench --bench cache_bench concurrent_

# profile one group
cargo bench --bench cache_bench concurrent_set_inline -- --profile-time 10
```

Benchmarks cover: `set` (inline/blob), `get` (inline/blob), `contains_key` (hit/miss), and concurrent `set`/`get` with `2/4/8` threads.

### Results

**Environment:** AMD Ryzen 9 7950X (16C/32T, 4.5 GHz), 64 GB DDR5, Windows 11 Pro (NVMe SSD), Rust 1.95.0

#### Single-threaded Operations

| Operation | Value Size | Latency (avg) | Throughput (avg) |
|-----------|------------|----------------|-------------------|
| `set_new_key/inline` | 512 B | 12.212 µs | 39.985 MiB/s |
| `set_new_key/blob` | 128 KiB | 543.36 µs | 230.05 MiB/s |
| `set_overwrite/inline` | 512 B | 3.9304 µs | 124.23 MiB/s |
| `set_overwrite/blob` | 128 KiB | 485.83 µs | 257.29 MiB/s |
| `get_hot_one_key/inline` | 512 B | 218.66 ns | 2.1807 GiB/s |
| `get_hot_one_key/blob` | 128 KiB | 32.392 µs | 3.7685 GiB/s |
| `get_warm_many_keys/inline` | 512 B | 429.59 ns | 1.1100 GiB/s |
| `get_warm_many_keys/blob` | 128 KiB | 39.841 µs | 3.0639 GiB/s |
| `contains_key/hit` | - | 208.90 ns | - |
| `contains_key/miss` | - | 83.078 ns | - |
| `contains_key/hit_many` | - | 320.08 ns | - |
| `contains_key/miss_many` | - | 82.552 ns | - |

#### Concurrent Operations (2/4/8 threads)

| Operation | Threads | Latency (avg) | Throughput (avg) |
|-----------|---------|----------------|-------------------|
| `concurrent_set_new_key/inline` | 2 | 2.1456 ms | 58.260 MiB/s |
| `concurrent_set_new_key/inline` | 4 | 6.6669 ms | 37.499 MiB/s |
| `concurrent_set_new_key/inline` | 8 | 13.697 ms | 36.503 MiB/s |
| `concurrent_set_new_key/blob` | 2 | 12.843 ms | 311.44 MiB/s |
| `concurrent_set_new_key/blob` | 4 | 19.252 ms | 415.55 MiB/s |
| `concurrent_set_new_key/blob` | 8 | 43.611 ms | 366.88 MiB/s |
| `concurrent_get_many_keys_sharded/inline` | 2 | 110.64 µs | 2.2065 GiB/s |
| `concurrent_get_many_keys_sharded/inline` | 4 | 133.71 µs | 3.6518 GiB/s |
| `concurrent_get_many_keys_sharded/inline` | 8 | 247.57 µs | 3.9446 GiB/s |
| `concurrent_get_shared_many_keys/inline` | 2 | 125.83 µs | 1.9402 GiB/s |
| `concurrent_get_shared_many_keys/inline` | 4 | 147.81 µs | 3.3035 GiB/s |
| `concurrent_get_shared_many_keys/inline` | 8 | 263.50 µs | 3.7062 GiB/s |
| `concurrent_get_many_keys_sharded/blob` | 2 | 10.653 ms | 5.8667 GiB/s |
| `concurrent_get_many_keys_sharded/blob` | 4 | 11.517 ms | 10.853 GiB/s |
| `concurrent_get_many_keys_sharded/blob` | 8 | 17.030 ms | 14.680 GiB/s |

*Measured with `diskcache` v0.2.0.*
