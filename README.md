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
| `set_new_key/inline` | 512 B | 3.476 µs | 140.47 MiB/s |
| `set_new_key/kv_sep` | 128 KiB | 70.306 µs | 1.7363 GiB/s |
| `set_overwrite/inline` | 512 B | 3.1276 µs | 156.12 MiB/s |
| `set_overwrite/kv_sep` | 128 KiB | 98.209 µs | 1.2430 GiB/s |
| `get_hot_one_key/inline` | 512 B | 188.72 ns | 2.5267 GiB/s |
| `get_hot_one_key/kv_sep` | 128 KiB | 5.6293 µs | 21.685 GiB/s |
| `get_warm_many_keys/inline` | 512 B | 394.80 ns | 1.2078 GiB/s |
| `get_warm_many_keys/kv_sep` | 128 KiB | 17.965 µs | 6.7949 GiB/s |
| `contains_key/hit` | - | 205.94 ns | - |
| `contains_key/miss` | - | 75.838 ns | - |
| `contains_key/hit_many` | - | 331.56 ns | - |
| `contains_key/miss_many` | - | 79.014 ns | - |

#### Concurrent Operations (2/4/8 threads)

| Operation | Threads | Latency (avg) | Throughput (avg) |
|-----------|---------|----------------|-------------------|
| `concurrent_set_new_key/inline` | 2 | 1.1403 ms | 109.62 MiB/s |
| `concurrent_set_new_key/inline` | 4 | 2.6139 ms | 95.643 MiB/s |
| `concurrent_set_new_key/inline` | 8 | 5.5188 ms | 90.599 MiB/s |
| `concurrent_set_new_key/kv_sep` | 2 | 6.6543 ms | 601.11 MiB/s |
| `concurrent_set_new_key/kv_sep` | 4 | 12.285 ms | 651.20 MiB/s |
| `concurrent_set_new_key/kv_sep` | 8 | 25.374 ms | 630.56 MiB/s |
| `concurrent_get_many_keys_sharded/inline` | 2 | 105.59 µs | 2.3122 GiB/s |
| `concurrent_get_many_keys_sharded/inline` | 4 | 118.90 µs | 4.1066 GiB/s |
| `concurrent_get_many_keys_sharded/inline` | 8 | 241.99 µs | 4.0355 GiB/s |
| `concurrent_get_shared_many_keys/inline` | 2 | 119.87 µs | 2.0367 GiB/s |
| `concurrent_get_shared_many_keys/inline` | 4 | 135.85 µs | 3.5942 GiB/s |
| `concurrent_get_shared_many_keys/inline` | 8 | 306.28 µs | 3.1885 GiB/s |
| `concurrent_get_many_keys_sharded/kv_sep` | 2 | 5.3478 ms | 11.687 GiB/s |
| `concurrent_get_many_keys_sharded/kv_sep` | 4 | 13.391 ms | 9.3346 GiB/s |
| `concurrent_get_many_keys_sharded/kv_sep` | 8 | 71.189 ms | 3.5118 GiB/s |

*Measured with `diskcache` v0.3.0.*