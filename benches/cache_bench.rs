use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use diskcache::{CacheNamespace, DiskCache, NamespaceConfig};
use fjall::{KeyspaceCreateOptions, KvSeparationOptions};
use rayon::{ThreadPoolBuilder, prelude::*};

fn open_namespace(
    path: impl AsRef<std::path::Path>,
    config: NamespaceConfig,
    cache_msg: &str,
) -> CacheNamespace {
    DiskCache::open(path)
        .expect(cache_msg)
        .namespace("bench", config)
        .expect("open benchmark namespace")
}

fn config_with_threshold(threshold_bytes: usize) -> NamespaceConfig {
    let threshold = u32::try_from(threshold_bytes).expect("threshold should fit u32");
    NamespaceConfig {
        keyspace_create_options: KeyspaceCreateOptions::default().with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(threshold),
        )),
    }
}

fn bench_set(c: &mut Criterion) {
    const OVERWRITE_KEY_SPACE: usize = 1024;

    let mut append_inline_group = c.benchmark_group("set_new_key/inline");
    append_inline_group.measurement_time(Duration::from_secs(8));
    let append_inline_dir = tempfile::tempdir().expect("create append inline tempdir");
    let append_inline_cache = open_namespace(
        append_inline_dir.path(),
        config_with_threshold(64 * 1024),
        "open append inline cache",
    );
    let append_inline_value = "x".repeat(512);
    append_inline_group.throughput(Throughput::Bytes(append_inline_value.len() as u64));

    let mut append_inline_index = 0_u64;
    append_inline_group.bench_function(
        BenchmarkId::new("value_bytes", append_inline_value.len()),
        |b| {
            b.iter(|| {
                append_inline_index = append_inline_index.wrapping_add(1);
                let key = format!("inline-append-{append_inline_index}");
                append_inline_cache
                    .set(
                        black_box(key.as_bytes()),
                        black_box(&append_inline_value),
                        None,
                    )
                    .expect("set append inline");
            })
        },
    );
    append_inline_group.finish();

    let mut append_kv_sep_group = c.benchmark_group("set_new_key/kv_sep");
    append_kv_sep_group.measurement_time(Duration::from_secs(8));
    let append_kv_sep_dir = tempfile::tempdir().expect("create append kv_sep tempdir");
    let append_kv_sep_cache = open_namespace(
        append_kv_sep_dir.path(),
        config_with_threshold(64),
        "open append kv_sep cache",
    );
    let append_kv_sep_value = "y".repeat(128 * 1024);
    append_kv_sep_group.throughput(Throughput::Bytes(append_kv_sep_value.len() as u64));

    let mut append_kv_sep_index = 0_u64;
    append_kv_sep_group.bench_function(
        BenchmarkId::new("value_bytes", append_kv_sep_value.len()),
        |b| {
            b.iter(|| {
                append_kv_sep_index = append_kv_sep_index.wrapping_add(1);
                let key = format!("kv_sep-append-{append_kv_sep_index}");
                append_kv_sep_cache
                    .set(
                        black_box(key.as_bytes()),
                        black_box(&append_kv_sep_value),
                        None,
                    )
                    .expect("set append kv_sep");
            })
        },
    );
    append_kv_sep_group.finish();

    let mut overwrite_inline_group = c.benchmark_group("set_overwrite/inline");
    overwrite_inline_group.measurement_time(Duration::from_secs(8));
    let overwrite_inline_dir = tempfile::tempdir().expect("create overwrite inline tempdir");
    let overwrite_inline_cache = open_namespace(
        overwrite_inline_dir.path(),
        config_with_threshold(64 * 1024),
        "open overwrite inline cache",
    );
    let overwrite_inline_value = "x".repeat(512);
    overwrite_inline_group.throughput(Throughput::Bytes(overwrite_inline_value.len() as u64));
    let overwrite_inline_keys: Vec<String> = (0..OVERWRITE_KEY_SPACE)
        .map(|slot| format!("inline-overwrite-{slot}"))
        .collect();

    for key in &overwrite_inline_keys {
        overwrite_inline_cache
            .set(key.as_str(), &overwrite_inline_value, None)
            .expect("seed overwrite inline keys");
    }

    let mut overwrite_inline_index = 0_usize;
    overwrite_inline_group.bench_function(
        BenchmarkId::new("value_bytes", overwrite_inline_value.len()),
        |b| {
            b.iter(|| {
                overwrite_inline_index = overwrite_inline_index.wrapping_add(1);
                let key =
                    &overwrite_inline_keys[overwrite_inline_index % overwrite_inline_keys.len()];
                overwrite_inline_cache
                    .set(
                        black_box(key.as_bytes()),
                        black_box(&overwrite_inline_value),
                        None,
                    )
                    .expect("set overwrite inline");
            })
        },
    );
    overwrite_inline_group.finish();

    let mut overwrite_kv_sep_group = c.benchmark_group("set_overwrite/kv_sep");
    overwrite_kv_sep_group.measurement_time(Duration::from_secs(8));
    let overwrite_kv_sep_dir = tempfile::tempdir().expect("create overwrite kv_sep tempdir");
    let overwrite_kv_sep_cache = open_namespace(
        overwrite_kv_sep_dir.path(),
        config_with_threshold(64),
        "open overwrite kv_sep cache",
    );
    let overwrite_kv_sep_value = "y".repeat(128 * 1024);
    overwrite_kv_sep_group.throughput(Throughput::Bytes(overwrite_kv_sep_value.len() as u64));
    let overwrite_kv_sep_keys: Vec<String> = (0..OVERWRITE_KEY_SPACE)
        .map(|slot| format!("kv_sep-overwrite-{slot}"))
        .collect();

    for key in &overwrite_kv_sep_keys {
        overwrite_kv_sep_cache
            .set(key.as_str(), &overwrite_kv_sep_value, None)
            .expect("seed overwrite kv_sep keys");
    }

    let mut overwrite_kv_sep_index = 0_usize;
    overwrite_kv_sep_group.bench_function(
        BenchmarkId::new("value_bytes", overwrite_kv_sep_value.len()),
        |b| {
            b.iter(|| {
                overwrite_kv_sep_index = overwrite_kv_sep_index.wrapping_add(1);
                let key =
                    &overwrite_kv_sep_keys[overwrite_kv_sep_index % overwrite_kv_sep_keys.len()];
                overwrite_kv_sep_cache
                    .set(
                        black_box(key.as_bytes()),
                        black_box(&overwrite_kv_sep_value),
                        None,
                    )
                    .expect("set overwrite kv_sep");
            })
        },
    );
    overwrite_kv_sep_group.finish();
}

fn bench_get(c: &mut Criterion) {
    const WARM_KEY_SPACE: usize = 1024;

    let mut hot_inline_group = c.benchmark_group("get_hot_one_key/inline");
    hot_inline_group.measurement_time(Duration::from_secs(8));
    let hot_inline_dir = tempfile::tempdir().expect("create hot inline tempdir");
    let hot_inline_cache = open_namespace(
        hot_inline_dir.path(),
        config_with_threshold(64 * 1024),
        "open hot inline cache",
    );
    let hot_inline_value = "x".repeat(512);
    hot_inline_cache
        .set("inline-key", &hot_inline_value, None)
        .expect("seed hot inline");
    hot_inline_group.throughput(Throughput::Bytes(hot_inline_value.len() as u64));

    hot_inline_group.bench_function(
        BenchmarkId::new("value_bytes", hot_inline_value.len()),
        |b| {
            b.iter(|| {
                let value: Option<String> = hot_inline_cache
                    .get(black_box("inline-key"))
                    .expect("get hot inline");
                black_box(value.as_deref().map_or(0, str::len));
            })
        },
    );
    hot_inline_group.finish();

    let mut hot_kv_sep_group = c.benchmark_group("get_hot_one_key/kv_sep");
    hot_kv_sep_group.measurement_time(Duration::from_secs(8));
    let hot_kv_sep_dir = tempfile::tempdir().expect("create hot kv_sep tempdir");
    let hot_kv_sep_cache = open_namespace(
        hot_kv_sep_dir.path(),
        config_with_threshold(64),
        "open hot kv_sep cache",
    );
    let hot_kv_sep_value = "y".repeat(128 * 1024);
    hot_kv_sep_cache
        .set("kv_sep-key", &hot_kv_sep_value, None)
        .expect("seed hot kv_sep");
    hot_kv_sep_group.throughput(Throughput::Bytes(hot_kv_sep_value.len() as u64));

    hot_kv_sep_group.bench_function(
        BenchmarkId::new("value_bytes", hot_kv_sep_value.len()),
        |b| {
            b.iter(|| {
                let value: Option<String> = hot_kv_sep_cache
                    .get(black_box("kv_sep-key"))
                    .expect("get hot kv_sep");
                black_box(value.as_deref().map_or(0, str::len));
            })
        },
    );
    hot_kv_sep_group.finish();

    let mut warm_inline_group = c.benchmark_group("get_warm_many_keys/inline");
    warm_inline_group.measurement_time(Duration::from_secs(8));
    let warm_inline_dir = tempfile::tempdir().expect("create warm inline tempdir");
    let warm_inline_cache = open_namespace(
        warm_inline_dir.path(),
        config_with_threshold(64 * 1024),
        "open warm inline cache",
    );
    let warm_inline_value = "x".repeat(512);
    let warm_inline_keys: Vec<String> = (0..WARM_KEY_SPACE)
        .map(|slot| format!("inline-warm-key-{slot}"))
        .collect();
    for key in &warm_inline_keys {
        warm_inline_cache
            .set(key.as_str(), &warm_inline_value, None)
            .expect("seed warm inline");
    }
    warm_inline_group.throughput(Throughput::Bytes(warm_inline_value.len() as u64));

    let mut warm_inline_index = 0_usize;
    warm_inline_group.bench_function(
        BenchmarkId::new("value_bytes", warm_inline_value.len()),
        |b| {
            b.iter(|| {
                warm_inline_index = warm_inline_index.wrapping_add(1);
                let key = &warm_inline_keys[warm_inline_index % warm_inline_keys.len()];
                let value: Option<String> = warm_inline_cache
                    .get(black_box(key.as_str()))
                    .expect("get warm inline");
                black_box(value.as_deref().map_or(0, str::len));
            })
        },
    );
    warm_inline_group.finish();

    let mut warm_kv_sep_group = c.benchmark_group("get_warm_many_keys/kv_sep");
    warm_kv_sep_group.measurement_time(Duration::from_secs(8));
    let warm_kv_sep_dir = tempfile::tempdir().expect("create warm kv_sep tempdir");
    let warm_kv_sep_cache = open_namespace(
        warm_kv_sep_dir.path(),
        config_with_threshold(64),
        "open warm kv_sep cache",
    );
    let warm_kv_sep_value = "y".repeat(128 * 1024);
    let warm_kv_sep_keys: Vec<String> = (0..WARM_KEY_SPACE)
        .map(|slot| format!("kv_sep-warm-key-{slot}"))
        .collect();
    for key in &warm_kv_sep_keys {
        warm_kv_sep_cache
            .set(key.as_str(), &warm_kv_sep_value, None)
            .expect("seed warm kv_sep");
    }
    warm_kv_sep_group.throughput(Throughput::Bytes(warm_kv_sep_value.len() as u64));

    let mut warm_kv_sep_index = 0_usize;
    warm_kv_sep_group.bench_function(
        BenchmarkId::new("value_bytes", warm_kv_sep_value.len()),
        |b| {
            b.iter(|| {
                warm_kv_sep_index = warm_kv_sep_index.wrapping_add(1);
                let key = &warm_kv_sep_keys[warm_kv_sep_index % warm_kv_sep_keys.len()];
                let value: Option<String> = warm_kv_sep_cache
                    .get(black_box(key.as_str()))
                    .expect("get warm kv_sep");
                black_box(value.as_deref().map_or(0, str::len));
            })
        },
    );
    warm_kv_sep_group.finish();
}

fn bench_contains_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_key");
    group.measurement_time(Duration::from_secs(6));

    let dir = tempfile::tempdir().expect("create tempdir");
    let cache = open_namespace(dir.path(), config_with_threshold(64 * 1024), "open cache");
    cache
        .set("present", &"value".to_string(), None)
        .expect("seed key");
    let hit_keys: Vec<String> = (0..1024).map(|slot| format!("present-{slot}")).collect();
    for key in &hit_keys {
        cache
            .set(key.as_str(), &"value".to_string(), None)
            .expect("seed hit-many key");
    }
    let miss_keys: Vec<String> = (0..1024).map(|slot| format!("missing-{slot}")).collect();

    group.bench_function("hit", |b| {
        b.iter(|| {
            let exists = cache
                .contains_key(black_box("present"))
                .expect("contains hit");
            black_box(exists);
        })
    });

    group.bench_function("miss", |b| {
        b.iter(|| {
            let exists = cache
                .contains_key(black_box("missing"))
                .expect("contains miss");
            black_box(exists);
        })
    });

    let mut hit_many_index = 0_usize;
    group.bench_function("hit_many", |b| {
        b.iter(|| {
            hit_many_index = hit_many_index.wrapping_add(1);
            let key = &hit_keys[hit_many_index % hit_keys.len()];
            let exists = cache
                .contains_key(black_box(key.as_str()))
                .expect("contains hit_many");
            black_box(exists);
        })
    });

    let mut miss_many_index = 0_usize;
    group.bench_function("miss_many", |b| {
        b.iter(|| {
            miss_many_index = miss_many_index.wrapping_add(1);
            let key = &miss_keys[miss_many_index % miss_keys.len()];
            let exists = cache
                .contains_key(black_box(key.as_str()))
                .expect("contains miss_many");
            black_box(exists);
        })
    });

    group.finish();
}

fn bench_concurrent(c: &mut Criterion) {
    const INLINE_SET_OPS_PER_THREAD: usize = 128;
    const KV_SEP_SET_OPS_PER_THREAD: usize = 16;
    const KEYS_PER_THREAD: usize = 64;
    const GET_OPS_PER_THREAD: usize = 256;

    let mut inline_set_group = c.benchmark_group("concurrent_set_new_key/inline");
    inline_set_group.measurement_time(Duration::from_secs(8));

    let inline_set_payload = Arc::new("z".repeat(512));

    for &threads in &[2_usize, 4, 8] {
        let set_dir = tempfile::tempdir().expect("create concurrent set tempdir");
        let set_cache = Arc::new(open_namespace(
            set_dir.path(),
            config_with_threshold(64 * 1024),
            "open concurrent set cache",
        ));
        let set_counter = Arc::new(AtomicU64::new(0));
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("build concurrent set pool"),
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
                                cache
                                    .set(
                                        black_box(key.as_bytes()),
                                        black_box(payload.as_ref()),
                                        None,
                                    )
                                    .expect("concurrent set");
                            }
                        });
                    });
                })
            },
        );
    }
    inline_set_group.finish();

    let mut kv_sep_set_group = c.benchmark_group("concurrent_set_new_key/kv_sep");
    kv_sep_set_group.measurement_time(Duration::from_secs(8));
    let kv_sep_set_payload = Arc::new("b".repeat(128 * 1024));
    for &threads in &[2_usize, 4, 8] {
        let set_dir = tempfile::tempdir().expect("create concurrent kv_sep set tempdir");
        let set_cache = Arc::new(open_namespace(
            set_dir.path(),
            config_with_threshold(64),
            "open concurrent kv_sep set cache",
        ));
        let set_counter = Arc::new(AtomicU64::new(0));
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("build concurrent kv_sep set pool"),
        );
        kv_sep_set_group.throughput(Throughput::Bytes(
            (kv_sep_set_payload.len() * KV_SEP_SET_OPS_PER_THREAD * threads) as u64,
        ));
        kv_sep_set_group.bench_with_input(
            BenchmarkId::new("threads", threads),
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
                                cache
                                    .set(
                                        black_box(key.as_bytes()),
                                        black_box(payload.as_ref()),
                                        None,
                                    )
                                    .expect("concurrent kv_sep set");
                            }
                        });
                    });
                })
            },
        );
    }
    kv_sep_set_group.finish();

    let inline_value = "q".repeat(512);
    let mut inline_sharded_get_group = c.benchmark_group("concurrent_get_many_keys_sharded/inline");
    inline_sharded_get_group.measurement_time(Duration::from_secs(8));
    for &threads in &[2_usize, 4, 8] {
        let get_dir = tempfile::tempdir().expect("create concurrent inline get tempdir");
        let get_cache = Arc::new(open_namespace(
            get_dir.path(),
            config_with_threshold(64 * 1024),
            "open concurrent inline get cache",
        ));
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("build concurrent inline get pool"),
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
                get_cache
                    .set(key.as_str(), &inline_value, None)
                    .expect("seed concurrent inline keys");
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
                                let value: Option<String> = cache
                                    .get(black_box(keys[slot].as_str()))
                                    .expect("concurrent get inline");
                                black_box(value.as_deref().map_or(0, str::len));
                            }
                        });
                    });
                })
            },
        );
    }
    inline_sharded_get_group.finish();

    let mut inline_shared_get_group = c.benchmark_group("concurrent_get_shared_many_keys/inline");
    inline_shared_get_group.measurement_time(Duration::from_secs(8));
    for &threads in &[2_usize, 4, 8] {
        let get_dir = tempfile::tempdir().expect("create concurrent inline shared get tempdir");
        let get_cache = Arc::new(open_namespace(
            get_dir.path(),
            config_with_threshold(64 * 1024),
            "open concurrent inline shared get cache",
        ));
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("build concurrent inline shared get pool"),
        );
        let shared_keys: Vec<String> = (0..(KEYS_PER_THREAD * threads))
            .map(|slot| format!("inline-shared-key-{threads}-{slot}"))
            .collect();
        for key in &shared_keys {
            get_cache
                .set(key.as_str(), &inline_value, None)
                .expect("seed concurrent inline shared keys");
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
                                let value: Option<String> = cache
                                    .get(black_box(keys[slot].as_str()))
                                    .expect("concurrent get inline shared");
                                black_box(value.as_deref().map_or(0, str::len));
                            }
                        });
                    });
                })
            },
        );
    }
    inline_shared_get_group.finish();

    let kv_sep_value = "w".repeat(128 * 1024);
    let mut kv_sep_sharded_get_group = c.benchmark_group("concurrent_get_many_keys_sharded/kv_sep");
    kv_sep_sharded_get_group.measurement_time(Duration::from_secs(8));
    for &threads in &[2_usize, 4, 8] {
        let get_dir = tempfile::tempdir().expect("create concurrent kv_sep get tempdir");
        let get_cache = Arc::new(open_namespace(
            get_dir.path(),
            config_with_threshold(64),
            "open concurrent kv_sep get cache",
        ));
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("build concurrent kv_sep get pool"),
        );

        let kv_sep_keys_by_thread: Vec<Vec<String>> = (0..threads)
            .map(|tid| {
                (0..KEYS_PER_THREAD)
                    .map(|slot| format!("kv_sep-key-{threads}-{}", tid * KEYS_PER_THREAD + slot))
                    .collect()
            })
            .collect();

        for keys in &kv_sep_keys_by_thread {
            for key in keys {
                get_cache
                    .set(key.as_str(), &kv_sep_value, None)
                    .expect("seed concurrent kv_sep keys");
            }
        }
        let kv_sep_keys_by_thread = Arc::new(kv_sep_keys_by_thread);

        kv_sep_sharded_get_group.throughput(Throughput::Bytes(
            (kv_sep_value.len() * GET_OPS_PER_THREAD * threads) as u64,
        ));
        kv_sep_sharded_get_group.bench_with_input(
            BenchmarkId::new("threads", threads),
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
                                let value: Option<String> = cache
                                    .get(black_box(keys[slot].as_str()))
                                    .expect("concurrent get kv_sep");
                                black_box(value.as_deref().map_or(0, str::len));
                            }
                        });
                    });
                })
            },
        );
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
