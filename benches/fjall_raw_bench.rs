use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions};
use rayon::{ThreadPoolBuilder, prelude::*};
use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    de::Pool,
    rancor::{Error, Strategy},
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
enum RecordEnvelope {
    V1(RecordV1),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct RecordV1 {
    expires_at_ms: Option<u64>,
    value: StoredValueV1,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
enum StoredValueV1 {
    Inline { bytes: Vec<u8> },
}

impl RecordEnvelope {
    fn as_v1(&self) -> &RecordV1 {
        match self {
            Self::V1(v1) => v1,
        }
    }
}

fn serialize_value<T>(value: &T) -> Vec<u8>
where
    T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, Error>>,
{
    rkyv::to_bytes::<Error>(value).expect("serialize").to_vec()
}

fn deserialize_value<T>(bytes: &[u8]) -> T
where
    T: Archive,
    T::Archived:
        for<'a> CheckBytes<HighValidator<'a, Error>> + Deserialize<T, Strategy<Pool, Error>>,
{
    rkyv::from_bytes::<T, Error>(bytes).expect("deserialize")
}

fn encode_record(value: &str) -> Vec<u8> {
    let payload = serialize_value(&value.to_string());
    let record = RecordEnvelope::V1(RecordV1 {
        expires_at_ms: None,
        value: StoredValueV1::Inline { bytes: payload },
    });
    serialize_value(&record)
}

fn decode_record_value(bytes: &[u8]) -> String {
    let record: RecordEnvelope = deserialize_value(bytes);
    let StoredValueV1::Inline { bytes: payload } = &record.as_v1().value;
    deserialize_value::<String>(payload)
}

// ---------------------------------------------------------------------------
// RawInline — no KV separation; everything stored in the fjall keyspace.
// Matches diskcache's "inline-like" path for small values.
// ---------------------------------------------------------------------------

struct RawInline {
    _db: Database,
    partition: Keyspace,
}

impl RawInline {
    fn open(path: &Path) -> Self {
        let db = Database::builder(path).open().expect("open db");
        let partition = db
            .keyspace("data", KeyspaceCreateOptions::default)
            .expect("open partition");
        Self { _db: db, partition }
    }

    fn set(&self, key: &[u8], value: &str) {
        let encoded = encode_record(value);
        self.partition.insert(key, encoded).expect("insert");
    }

    fn get(&self, key: &[u8]) -> Option<String> {
        self.partition
            .get(key)
            .expect("get")
            .map(|v| decode_record_value(v.as_ref()))
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.partition
            .get(key)
            .expect("contains_key")
            .map(|v| deserialize_value::<RecordEnvelope>(v.as_ref()))
            .is_some()
    }
}

// ---------------------------------------------------------------------------
// RawKVSep — fjall's built-in key-value separation, configurable threshold.
//
//   RawKVSep::open(path, 64)   -> threshold 64 B, mirrors diskcache large-value case
//   RawKVSep::open(path, 1024) → threshold 1 KiB, fjall's default
// ---------------------------------------------------------------------------

struct RawKVSep {
    _db: Database,
    partition: Keyspace,
    threshold: u32,
}

impl RawKVSep {
    fn open(path: &Path, separation_threshold: u32) -> Self {
        let db = Database::builder(path).open().expect("open db");
        let partition = db
            .keyspace("data", move || {
                KeyspaceCreateOptions::default().with_kv_separation(Some(
                    KvSeparationOptions::default().separation_threshold(separation_threshold),
                ))
            })
            .expect("open kv-sep partition");
        Self {
            _db: db,
            partition,
            threshold: separation_threshold,
        }
    }

    fn label(&self) -> String {
        if self.threshold < 1024 {
            format!("kvsep_{}b", self.threshold)
        } else {
            format!("kvsep_{}k", self.threshold / 1024)
        }
    }

    fn set(&self, key: &[u8], value: &str) {
        let encoded = encode_record(value);
        self.partition.insert(key, encoded).expect("insert");
    }

    fn get(&self, key: &[u8]) -> Option<String> {
        self.partition
            .get(key)
            .expect("get")
            .map(|v| decode_record_value(v.as_ref()))
    }
}

// ---------------------------------------------------------------------------
// bench_set — mirrors cache_bench::bench_set exactly
// ---------------------------------------------------------------------------

fn bench_set(c: &mut Criterion) {
    const OVERWRITE_KEY_SPACE: usize = 1024;

    // --- set_new_key/inline ---
    let mut append_inline_group = c.benchmark_group("set_new_key/inline");
    append_inline_group.measurement_time(Duration::from_secs(8));
    let append_inline_dir = tempfile::tempdir().expect("create append inline tempdir");
    let append_inline_cache = RawInline::open(append_inline_dir.path());
    let append_inline_value = "x".repeat(512);
    append_inline_group.throughput(Throughput::Bytes(append_inline_value.len() as u64));
    let mut append_inline_index = 0_u64;
    append_inline_group.bench_function(
        BenchmarkId::new("value_bytes", append_inline_value.len()),
        |b| {
            b.iter(|| {
                append_inline_index = append_inline_index.wrapping_add(1);
                let key = format!("inline-append-{append_inline_index}");
                append_inline_cache.set(black_box(key.as_bytes()), black_box(&append_inline_value));
            })
        },
    );
    append_inline_group.finish();

    // --- set_new_key/kv_sep — two KVSep variants in the same group ---
    let mut append_kv_sep_group = c.benchmark_group("set_new_key/kv_sep");
    append_kv_sep_group.measurement_time(Duration::from_secs(8));
    let append_kv_sep_value = "y".repeat(128 * 1024);
    append_kv_sep_group.throughput(Throughput::Bytes(append_kv_sep_value.len() as u64));
    let mut append_kv_sep_index = 0_u64;

    for threshold in [64_u32, 1024] {
        let dir = tempfile::tempdir().expect("create append kv_sep tempdir");
        let cache = RawKVSep::open(dir.path(), threshold);
        let label = cache.label();
        append_kv_sep_group.bench_function(
            BenchmarkId::new(&label, append_kv_sep_value.len()),
            |b| {
                b.iter(|| {
                    append_kv_sep_index = append_kv_sep_index.wrapping_add(1);
                    let key = format!("kv_sep-append-{append_kv_sep_index}");
                    cache.set(black_box(key.as_bytes()), black_box(&append_kv_sep_value));
                })
            },
        );
        drop(dir);
    }
    append_kv_sep_group.finish();

    // --- set_overwrite/inline ---
    let mut overwrite_inline_group = c.benchmark_group("set_overwrite/inline");
    overwrite_inline_group.measurement_time(Duration::from_secs(8));
    let overwrite_inline_dir = tempfile::tempdir().expect("create overwrite inline tempdir");
    let overwrite_inline_cache = RawInline::open(overwrite_inline_dir.path());
    let overwrite_inline_value = "x".repeat(512);
    overwrite_inline_group.throughput(Throughput::Bytes(overwrite_inline_value.len() as u64));
    let overwrite_inline_keys: Vec<String> = (0..OVERWRITE_KEY_SPACE)
        .map(|slot| format!("inline-overwrite-{slot}"))
        .collect();
    for key in &overwrite_inline_keys {
        overwrite_inline_cache.set(key.as_bytes(), &overwrite_inline_value);
    }
    let mut overwrite_inline_index = 0_usize;
    overwrite_inline_group.bench_function(
        BenchmarkId::new("value_bytes", overwrite_inline_value.len()),
        |b| {
            b.iter(|| {
                overwrite_inline_index = overwrite_inline_index.wrapping_add(1);
                let key =
                    &overwrite_inline_keys[overwrite_inline_index % overwrite_inline_keys.len()];
                overwrite_inline_cache.set(
                    black_box(key.as_bytes()),
                    black_box(&overwrite_inline_value),
                );
            })
        },
    );
    overwrite_inline_group.finish();

    // --- set_overwrite/kv_sep — two KVSep variants ---
    let mut overwrite_kv_sep_group = c.benchmark_group("set_overwrite/kv_sep");
    overwrite_kv_sep_group.measurement_time(Duration::from_secs(8));
    let overwrite_kv_sep_value = "y".repeat(128 * 1024);
    overwrite_kv_sep_group.throughput(Throughput::Bytes(overwrite_kv_sep_value.len() as u64));
    let overwrite_kv_sep_keys: Vec<String> = (0..OVERWRITE_KEY_SPACE)
        .map(|slot| format!("kv_sep-overwrite-{slot}"))
        .collect();

    for threshold in [64_u32, 1024] {
        let dir = tempfile::tempdir().expect("create overwrite kv_sep tempdir");
        let cache = RawKVSep::open(dir.path(), threshold);
        let label = cache.label();
        for key in &overwrite_kv_sep_keys {
            cache.set(key.as_bytes(), &overwrite_kv_sep_value);
        }
        let mut overwrite_kv_sep_index = 0_usize;
        overwrite_kv_sep_group.bench_function(
            BenchmarkId::new(&label, overwrite_kv_sep_value.len()),
            |b| {
                b.iter(|| {
                    overwrite_kv_sep_index = overwrite_kv_sep_index.wrapping_add(1);
                    let key = &overwrite_kv_sep_keys
                        [overwrite_kv_sep_index % overwrite_kv_sep_keys.len()];
                    cache.set(
                        black_box(key.as_bytes()),
                        black_box(&overwrite_kv_sep_value),
                    );
                })
            },
        );
        drop(dir);
    }
    overwrite_kv_sep_group.finish();
}

// ---------------------------------------------------------------------------
// bench_get — mirrors cache_bench::bench_get exactly
// ---------------------------------------------------------------------------

fn bench_get(c: &mut Criterion) {
    const WARM_KEY_SPACE: usize = 1024;

    // --- get_hot_one_key/inline ---
    let mut hot_inline_group = c.benchmark_group("get_hot_one_key/inline");
    hot_inline_group.measurement_time(Duration::from_secs(8));
    let hot_inline_dir = tempfile::tempdir().expect("create hot inline tempdir");
    let hot_inline_cache = RawInline::open(hot_inline_dir.path());
    let hot_inline_value = "x".repeat(512);
    hot_inline_cache.set(b"inline-key", &hot_inline_value);
    hot_inline_group.throughput(Throughput::Bytes(hot_inline_value.len() as u64));
    hot_inline_group.bench_function(
        BenchmarkId::new("value_bytes", hot_inline_value.len()),
        |b| {
            b.iter(|| {
                let value = hot_inline_cache.get(black_box(b"inline-key"));
                black_box(value.as_deref().map_or(0, str::len));
            })
        },
    );
    hot_inline_group.finish();

    // --- get_hot_one_key/kv_sep — two KVSep variants ---
    let mut hot_kv_sep_group = c.benchmark_group("get_hot_one_key/kv_sep");
    hot_kv_sep_group.measurement_time(Duration::from_secs(8));
    let hot_kv_sep_value = "y".repeat(128 * 1024);
    hot_kv_sep_group.throughput(Throughput::Bytes(hot_kv_sep_value.len() as u64));

    for threshold in [64_u32, 1024] {
        let dir = tempfile::tempdir().expect("create hot kv_sep tempdir");
        let cache = RawKVSep::open(dir.path(), threshold);
        let label = cache.label();
        cache.set(b"kv_sep-key", &hot_kv_sep_value);
        hot_kv_sep_group.bench_function(BenchmarkId::new(&label, hot_kv_sep_value.len()), |b| {
            b.iter(|| {
                let value = cache.get(black_box(b"kv_sep-key"));
                black_box(value.as_deref().map_or(0, str::len));
            })
        });
        drop(dir);
    }
    hot_kv_sep_group.finish();

    // --- get_warm_many_keys/inline ---
    let mut warm_inline_group = c.benchmark_group("get_warm_many_keys/inline");
    warm_inline_group.measurement_time(Duration::from_secs(8));
    let warm_inline_dir = tempfile::tempdir().expect("create warm inline tempdir");
    let warm_inline_cache = RawInline::open(warm_inline_dir.path());
    let warm_inline_value = "x".repeat(512);
    let warm_inline_keys: Vec<String> = (0..WARM_KEY_SPACE)
        .map(|slot| format!("inline-warm-key-{slot}"))
        .collect();
    for key in &warm_inline_keys {
        warm_inline_cache.set(key.as_bytes(), &warm_inline_value);
    }
    warm_inline_group.throughput(Throughput::Bytes(warm_inline_value.len() as u64));
    let mut warm_inline_index = 0_usize;
    warm_inline_group.bench_function(
        BenchmarkId::new("value_bytes", warm_inline_value.len()),
        |b| {
            b.iter(|| {
                warm_inline_index = warm_inline_index.wrapping_add(1);
                let key = &warm_inline_keys[warm_inline_index % warm_inline_keys.len()];
                let value = warm_inline_cache.get(black_box(key.as_bytes()));
                black_box(value.as_deref().map_or(0, str::len));
            })
        },
    );
    warm_inline_group.finish();

    // --- get_warm_many_keys/kv_sep — two KVSep variants ---
    let mut warm_kv_sep_group = c.benchmark_group("get_warm_many_keys/kv_sep");
    warm_kv_sep_group.measurement_time(Duration::from_secs(8));
    let warm_kv_sep_value = "y".repeat(128 * 1024);
    warm_kv_sep_group.throughput(Throughput::Bytes(warm_kv_sep_value.len() as u64));
    let warm_kv_sep_keys: Vec<String> = (0..WARM_KEY_SPACE)
        .map(|slot| format!("kv_sep-warm-key-{slot}"))
        .collect();

    for threshold in [64_u32, 1024] {
        let dir = tempfile::tempdir().expect("create warm kv_sep tempdir");
        let cache = RawKVSep::open(dir.path(), threshold);
        let label = cache.label();
        for key in &warm_kv_sep_keys {
            cache.set(key.as_bytes(), &warm_kv_sep_value);
        }
        let mut warm_kv_sep_index = 0_usize;
        warm_kv_sep_group.bench_function(BenchmarkId::new(&label, warm_kv_sep_value.len()), |b| {
            b.iter(|| {
                warm_kv_sep_index = warm_kv_sep_index.wrapping_add(1);
                let key = &warm_kv_sep_keys[warm_kv_sep_index % warm_kv_sep_keys.len()];
                let value = cache.get(black_box(key.as_bytes()));
                black_box(value.as_deref().map_or(0, str::len));
            })
        });
        drop(dir);
    }
    warm_kv_sep_group.finish();
}

// ---------------------------------------------------------------------------
// bench_contains_key — mirrors cache_bench::bench_contains_key exactly
// ---------------------------------------------------------------------------

fn bench_contains_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_key");
    group.measurement_time(Duration::from_secs(6));

    let dir = tempfile::tempdir().expect("create tempdir");
    let cache = RawInline::open(dir.path());
    cache.set(b"present", "value");
    let hit_keys: Vec<String> = (0..1024).map(|slot| format!("present-{slot}")).collect();
    for key in &hit_keys {
        cache.set(key.as_bytes(), "value");
    }
    let miss_keys: Vec<String> = (0..1024).map(|slot| format!("missing-{slot}")).collect();

    group.bench_function("hit", |b| {
        b.iter(|| black_box(cache.contains_key(black_box(b"present"))))
    });
    group.bench_function("miss", |b| {
        b.iter(|| black_box(cache.contains_key(black_box(b"missing"))))
    });

    let mut hit_many_index = 0_usize;
    group.bench_function("hit_many", |b| {
        b.iter(|| {
            hit_many_index = hit_many_index.wrapping_add(1);
            let key = &hit_keys[hit_many_index % hit_keys.len()];
            black_box(cache.contains_key(black_box(key.as_bytes())));
        })
    });

    let mut miss_many_index = 0_usize;
    group.bench_function("miss_many", |b| {
        b.iter(|| {
            miss_many_index = miss_many_index.wrapping_add(1);
            let key = &miss_keys[miss_many_index % miss_keys.len()];
            black_box(cache.contains_key(black_box(key.as_bytes())));
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// bench_concurrent — mirrors cache_bench::bench_concurrent exactly
// ---------------------------------------------------------------------------

fn bench_concurrent(c: &mut Criterion) {
    const INLINE_SET_OPS_PER_THREAD: usize = 128;
    const KV_SEP_SET_OPS_PER_THREAD: usize = 16;
    const KEYS_PER_THREAD: usize = 64;
    const GET_OPS_PER_THREAD: usize = 256;

    // concurrent_set_new_key/inline
    let mut inline_set_group = c.benchmark_group("concurrent_set_new_key/inline");
    inline_set_group.measurement_time(Duration::from_secs(8));
    let inline_set_payload = Arc::new("z".repeat(512));
    for &threads in &[2_usize, 4, 8] {
        let set_dir = tempfile::tempdir().expect("create concurrent inline set tempdir");
        let set_cache = Arc::new(RawInline::open(set_dir.path()));
        let set_counter = Arc::new(AtomicU64::new(0));
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("build pool"),
        );
        inline_set_group.throughput(Throughput::Bytes(
            (inline_set_payload.len() * INLINE_SET_OPS_PER_THREAD * threads) as u64,
        ));
        inline_set_group.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                let cache = Arc::clone(&set_cache);
                let payload = Arc::clone(&inline_set_payload);
                let counter = Arc::clone(&set_counter);
                let pool = Arc::clone(&pool);
                b.iter(|| {
                    let batch_size = (threads * INLINE_SET_OPS_PER_THREAD) as u64;
                    let batch_start = counter.fetch_add(batch_size, Ordering::Relaxed);
                    pool.install(|| {
                        (0..threads).into_par_iter().for_each(|tid| {
                            let thread_base =
                                batch_start + (tid * INLINE_SET_OPS_PER_THREAD) as u64;
                            for offset in 0..INLINE_SET_OPS_PER_THREAD {
                                let key = format!("concurrent-set-{}", thread_base + offset as u64);
                                cache.set(black_box(key.as_bytes()), black_box(payload.as_ref()));
                            }
                        });
                    });
                })
            },
        );
        drop(set_dir);
    }
    inline_set_group.finish();

    // concurrent_set_new_key/kv_sep — two KVSep variants
    let mut kv_sep_set_group = c.benchmark_group("concurrent_set_new_key/kv_sep");
    kv_sep_set_group.measurement_time(Duration::from_secs(8));
    let kv_sep_set_payload = Arc::new("b".repeat(128 * 1024));
    for threshold in [64_u32, 1024] {
        for &threads in &[2_usize, 4, 8] {
            let set_dir = tempfile::tempdir().expect("create concurrent kv_sep set tempdir");
            let set_cache = Arc::new(RawKVSep::open(set_dir.path(), threshold));
            let label = set_cache.label();
            let set_counter = Arc::new(AtomicU64::new(0));
            let pool = Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("build pool"),
            );
            kv_sep_set_group.throughput(Throughput::Bytes(
                (kv_sep_set_payload.len() * KV_SEP_SET_OPS_PER_THREAD * threads) as u64,
            ));
            kv_sep_set_group.bench_with_input(
                BenchmarkId::new(format!("{label}/threads"), threads),
                &threads,
                |b, &threads| {
                    let cache = Arc::clone(&set_cache);
                    let payload = Arc::clone(&kv_sep_set_payload);
                    let counter = Arc::clone(&set_counter);
                    let pool = Arc::clone(&pool);
                    b.iter(|| {
                        let batch_size = (threads * KV_SEP_SET_OPS_PER_THREAD) as u64;
                        let batch_start = counter.fetch_add(batch_size, Ordering::Relaxed);
                        pool.install(|| {
                            (0..threads).into_par_iter().for_each(|tid| {
                                let thread_base =
                                    batch_start + (tid * KV_SEP_SET_OPS_PER_THREAD) as u64;
                                for offset in 0..KV_SEP_SET_OPS_PER_THREAD {
                                    let key = format!(
                                        "concurrent-kv_sep-set-{}",
                                        thread_base + offset as u64
                                    );
                                    cache.set(
                                        black_box(key.as_bytes()),
                                        black_box(payload.as_ref()),
                                    );
                                }
                            });
                        });
                    })
                },
            );
            drop(set_dir);
        }
    }
    kv_sep_set_group.finish();

    // concurrent_get_many_keys_sharded/inline
    let inline_value = "q".repeat(512);
    let mut inline_sharded_get_group = c.benchmark_group("concurrent_get_many_keys_sharded/inline");
    inline_sharded_get_group.measurement_time(Duration::from_secs(8));
    for &threads in &[2_usize, 4, 8] {
        let get_dir = tempfile::tempdir().expect("create concurrent inline sharded get tempdir");
        let get_cache = Arc::new(RawInline::open(get_dir.path()));
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("build pool"),
        );
        let inline_keys_by_thread: Vec<Vec<String>> = (0..threads)
            .map(|tid| {
                (0..KEYS_PER_THREAD)
                    .map(|slot| format!("inline-key-{threads}-{}", tid * KEYS_PER_THREAD + slot))
                    .collect()
            })
            .collect();
        for keys in &inline_keys_by_thread {
            for key in keys {
                get_cache.set(key.as_bytes(), &inline_value);
            }
        }
        let inline_keys_by_thread = Arc::new(inline_keys_by_thread);
        inline_sharded_get_group.throughput(Throughput::Bytes(
            (inline_value.len() * GET_OPS_PER_THREAD * threads) as u64,
        ));
        inline_sharded_get_group.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                let cache = Arc::clone(&get_cache);
                let keys_by_thread = Arc::clone(&inline_keys_by_thread);
                let pool = Arc::clone(&pool);
                b.iter(|| {
                    pool.install(|| {
                        (0..threads).into_par_iter().for_each(|tid| {
                            let keys = &keys_by_thread[tid];
                            for op in 0..GET_OPS_PER_THREAD {
                                let slot = (op + tid * 31) % KEYS_PER_THREAD;
                                let value = cache.get(black_box(keys[slot].as_bytes()));
                                black_box(value.as_deref().map_or(0, str::len));
                            }
                        });
                    });
                })
            },
        );
        drop(get_dir);
    }
    inline_sharded_get_group.finish();

    // concurrent_get_shared_many_keys/inline
    let mut inline_shared_get_group = c.benchmark_group("concurrent_get_shared_many_keys/inline");
    inline_shared_get_group.measurement_time(Duration::from_secs(8));
    for &threads in &[2_usize, 4, 8] {
        let get_dir = tempfile::tempdir().expect("create concurrent inline shared get tempdir");
        let get_cache = Arc::new(RawInline::open(get_dir.path()));
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("build pool"),
        );
        let shared_keys: Vec<String> = (0..(KEYS_PER_THREAD * threads))
            .map(|slot| format!("inline-shared-key-{threads}-{slot}"))
            .collect();
        for key in &shared_keys {
            get_cache.set(key.as_bytes(), &inline_value);
        }
        let shared_keys = Arc::new(shared_keys);
        inline_shared_get_group.throughput(Throughput::Bytes(
            (inline_value.len() * GET_OPS_PER_THREAD * threads) as u64,
        ));
        inline_shared_get_group.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                let cache = Arc::clone(&get_cache);
                let keys = Arc::clone(&shared_keys);
                let pool = Arc::clone(&pool);
                b.iter(|| {
                    pool.install(|| {
                        (0..threads).into_par_iter().for_each(|tid| {
                            for op in 0..GET_OPS_PER_THREAD {
                                let slot = (op + tid * 31) % keys.len();
                                let value = cache.get(black_box(keys[slot].as_bytes()));
                                black_box(value.as_deref().map_or(0, str::len));
                            }
                        });
                    });
                })
            },
        );
        drop(get_dir);
    }
    inline_shared_get_group.finish();

    // concurrent_get_many_keys_sharded/kv_sep — two KVSep variants
    let kv_sep_value = "w".repeat(128 * 1024);
    let mut kv_sep_sharded_get_group = c.benchmark_group("concurrent_get_many_keys_sharded/kv_sep");
    kv_sep_sharded_get_group.measurement_time(Duration::from_secs(8));
    for threshold in [64_u32, 1024] {
        for &threads in &[2_usize, 4, 8] {
            let get_dir =
                tempfile::tempdir().expect("create concurrent kv_sep sharded get tempdir");
            let get_cache = Arc::new(RawKVSep::open(get_dir.path(), threshold));
            let label = get_cache.label();
            let pool = Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("build pool"),
            );
            let kv_sep_keys_by_thread: Vec<Vec<String>> = (0..threads)
                .map(|tid| {
                    (0..KEYS_PER_THREAD)
                        .map(|slot| {
                            format!("kv_sep-key-{threads}-{}", tid * KEYS_PER_THREAD + slot)
                        })
                        .collect()
                })
                .collect();
            for keys in &kv_sep_keys_by_thread {
                for key in keys {
                    get_cache.set(key.as_bytes(), &kv_sep_value);
                }
            }
            let kv_sep_keys_by_thread = Arc::new(kv_sep_keys_by_thread);
            kv_sep_sharded_get_group.throughput(Throughput::Bytes(
                (kv_sep_value.len() * GET_OPS_PER_THREAD * threads) as u64,
            ));
            kv_sep_sharded_get_group.bench_with_input(
                BenchmarkId::new(format!("{label}/threads"), threads),
                &threads,
                |b, &threads| {
                    let cache = Arc::clone(&get_cache);
                    let keys_by_thread = Arc::clone(&kv_sep_keys_by_thread);
                    let pool = Arc::clone(&pool);
                    b.iter(|| {
                        pool.install(|| {
                            (0..threads).into_par_iter().for_each(|tid| {
                                let keys = &keys_by_thread[tid];
                                for op in 0..GET_OPS_PER_THREAD {
                                    let slot = (op + tid * 31) % KEYS_PER_THREAD;
                                    let value = cache.get(black_box(keys[slot].as_bytes()));
                                    black_box(value.as_deref().map_or(0, str::len));
                                }
                            });
                        });
                    })
                },
            );
            drop(get_dir);
        }
    }
    kv_sep_sharded_get_group.finish();
}

criterion_group!(
    benches,
    bench_set,
    bench_get,
    bench_contains_key,
    bench_concurrent
);
criterion_main!(benches);
