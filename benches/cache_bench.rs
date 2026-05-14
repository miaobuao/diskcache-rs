use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use diskcache::{CacheNamespace, DiskCache, NamespaceConfig};
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

fn bench_set(c: &mut Criterion) {
    const OVERWRITE_KEY_SPACE: usize = 1024;

    let mut append_inline_group = c.benchmark_group("set_new_key/inline");
    append_inline_group.measurement_time(Duration::from_secs(8));
    let append_inline_dir = tempfile::tempdir().expect("create append inline tempdir");
    let append_inline_cache = open_namespace(
        append_inline_dir.path(),
        NamespaceConfig {
            inline_threshold_bytes: 64 * 1024,
        },
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

    let mut append_blob_group = c.benchmark_group("set_new_key/blob");
    append_blob_group.measurement_time(Duration::from_secs(8));
    let append_blob_dir = tempfile::tempdir().expect("create append blob tempdir");
    let append_blob_cache = open_namespace(
        append_blob_dir.path(),
        NamespaceConfig {
            inline_threshold_bytes: 64,
        },
        "open append blob cache",
    );
    let append_blob_value = "y".repeat(128 * 1024);
    append_blob_group.throughput(Throughput::Bytes(append_blob_value.len() as u64));

    let mut append_blob_index = 0_u64;
    append_blob_group.bench_function(
        BenchmarkId::new("value_bytes", append_blob_value.len()),
        |b| {
            b.iter(|| {
                append_blob_index = append_blob_index.wrapping_add(1);
                let key = format!("blob-append-{append_blob_index}");
                append_blob_cache
                    .set(
                        black_box(key.as_bytes()),
                        black_box(&append_blob_value),
                        None,
                    )
                    .expect("set append blob");
            })
        },
    );
    append_blob_group.finish();

    let mut overwrite_inline_group = c.benchmark_group("set_overwrite/inline");
    overwrite_inline_group.measurement_time(Duration::from_secs(8));
    let overwrite_inline_dir = tempfile::tempdir().expect("create overwrite inline tempdir");
    let overwrite_inline_cache = open_namespace(
        overwrite_inline_dir.path(),
        NamespaceConfig {
            inline_threshold_bytes: 64 * 1024,
        },
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

    let mut overwrite_blob_group = c.benchmark_group("set_overwrite/blob");
    overwrite_blob_group.measurement_time(Duration::from_secs(8));
    let overwrite_blob_dir = tempfile::tempdir().expect("create overwrite blob tempdir");
    let overwrite_blob_cache = open_namespace(
        overwrite_blob_dir.path(),
        NamespaceConfig {
            inline_threshold_bytes: 64,
        },
        "open overwrite blob cache",
    );
    let overwrite_blob_value = "y".repeat(128 * 1024);
    overwrite_blob_group.throughput(Throughput::Bytes(overwrite_blob_value.len() as u64));
    let overwrite_blob_keys: Vec<String> = (0..OVERWRITE_KEY_SPACE)
        .map(|slot| format!("blob-overwrite-{slot}"))
        .collect();

    for key in &overwrite_blob_keys {
        overwrite_blob_cache
            .set(key.as_str(), &overwrite_blob_value, None)
            .expect("seed overwrite blob keys");
    }

    let mut overwrite_blob_index = 0_usize;
    overwrite_blob_group.bench_function(
        BenchmarkId::new("value_bytes", overwrite_blob_value.len()),
        |b| {
            b.iter(|| {
                overwrite_blob_index = overwrite_blob_index.wrapping_add(1);
                let key = &overwrite_blob_keys[overwrite_blob_index % overwrite_blob_keys.len()];
                overwrite_blob_cache
                    .set(
                        black_box(key.as_bytes()),
                        black_box(&overwrite_blob_value),
                        None,
                    )
                    .expect("set overwrite blob");
            })
        },
    );
    overwrite_blob_group.finish();
}

fn bench_get(c: &mut Criterion) {
    const WARM_KEY_SPACE: usize = 1024;

    let mut hot_inline_group = c.benchmark_group("get_hot_one_key/inline");
    hot_inline_group.measurement_time(Duration::from_secs(8));
    let hot_inline_dir = tempfile::tempdir().expect("create hot inline tempdir");
    let hot_inline_cache = open_namespace(
        hot_inline_dir.path(),
        NamespaceConfig {
            inline_threshold_bytes: 64 * 1024,
        },
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

    let mut hot_blob_group = c.benchmark_group("get_hot_one_key/blob");
    hot_blob_group.measurement_time(Duration::from_secs(8));
    let hot_blob_dir = tempfile::tempdir().expect("create hot blob tempdir");
    let hot_blob_cache = open_namespace(
        hot_blob_dir.path(),
        NamespaceConfig {
            inline_threshold_bytes: 64,
        },
        "open hot blob cache",
    );
    let hot_blob_value = "y".repeat(128 * 1024);
    hot_blob_cache
        .set("blob-key", &hot_blob_value, None)
        .expect("seed hot blob");
    hot_blob_group.throughput(Throughput::Bytes(hot_blob_value.len() as u64));

    hot_blob_group.bench_function(BenchmarkId::new("value_bytes", hot_blob_value.len()), |b| {
        b.iter(|| {
            let value: Option<String> = hot_blob_cache
                .get(black_box("blob-key"))
                .expect("get hot blob");
            black_box(value.as_deref().map_or(0, str::len));
        })
    });
    hot_blob_group.finish();

    let mut warm_inline_group = c.benchmark_group("get_warm_many_keys/inline");
    warm_inline_group.measurement_time(Duration::from_secs(8));
    let warm_inline_dir = tempfile::tempdir().expect("create warm inline tempdir");
    let warm_inline_cache = open_namespace(
        warm_inline_dir.path(),
        NamespaceConfig {
            inline_threshold_bytes: 64 * 1024,
        },
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

    let mut warm_blob_group = c.benchmark_group("get_warm_many_keys/blob");
    warm_blob_group.measurement_time(Duration::from_secs(8));
    let warm_blob_dir = tempfile::tempdir().expect("create warm blob tempdir");
    let warm_blob_cache = open_namespace(
        warm_blob_dir.path(),
        NamespaceConfig {
            inline_threshold_bytes: 64,
        },
        "open warm blob cache",
    );
    let warm_blob_value = "y".repeat(128 * 1024);
    let warm_blob_keys: Vec<String> = (0..WARM_KEY_SPACE)
        .map(|slot| format!("blob-warm-key-{slot}"))
        .collect();
    for key in &warm_blob_keys {
        warm_blob_cache
            .set(key.as_str(), &warm_blob_value, None)
            .expect("seed warm blob");
    }
    warm_blob_group.throughput(Throughput::Bytes(warm_blob_value.len() as u64));

    let mut warm_blob_index = 0_usize;
    warm_blob_group.bench_function(
        BenchmarkId::new("value_bytes", warm_blob_value.len()),
        |b| {
            b.iter(|| {
                warm_blob_index = warm_blob_index.wrapping_add(1);
                let key = &warm_blob_keys[warm_blob_index % warm_blob_keys.len()];
                let value: Option<String> = warm_blob_cache
                    .get(black_box(key.as_str()))
                    .expect("get warm blob");
                black_box(value.as_deref().map_or(0, str::len));
            })
        },
    );
    warm_blob_group.finish();
}

fn bench_contains_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_key");
    group.measurement_time(Duration::from_secs(6));

    let dir = tempfile::tempdir().expect("create tempdir");
    let cache = open_namespace(
        dir.path(),
        NamespaceConfig {
            inline_threshold_bytes: 64 * 1024,
        },
        "open cache",
    );
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
    const BLOB_SET_OPS_PER_THREAD: usize = 16;
    const KEYS_PER_THREAD: usize = 64;
    const GET_OPS_PER_THREAD: usize = 256;

    let mut inline_set_group = c.benchmark_group("concurrent_set_new_key/inline");
    inline_set_group.measurement_time(Duration::from_secs(8));

    let inline_set_payload = Arc::new("z".repeat(512));

    for &threads in &[2_usize, 4, 8] {
        let set_dir = tempfile::tempdir().expect("create concurrent set tempdir");
        let set_cache = Arc::new(open_namespace(
            set_dir.path(),
            NamespaceConfig {
                inline_threshold_bytes: 64 * 1024,
            },
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

    let mut blob_set_group = c.benchmark_group("concurrent_set_new_key/blob");
    blob_set_group.measurement_time(Duration::from_secs(8));
    let blob_set_payload = Arc::new("b".repeat(128 * 1024));
    for &threads in &[2_usize, 4, 8] {
        let set_dir = tempfile::tempdir().expect("create concurrent blob set tempdir");
        let set_cache = Arc::new(open_namespace(
            set_dir.path(),
            NamespaceConfig {
                inline_threshold_bytes: 64,
            },
            "open concurrent blob set cache",
        ));
        let set_counter = Arc::new(AtomicU64::new(0));
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("build concurrent blob set pool"),
        );
        blob_set_group.throughput(Throughput::Bytes(
            (blob_set_payload.len() * BLOB_SET_OPS_PER_THREAD * threads) as u64,
        ));
        blob_set_group.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                let cache = Arc::clone(&set_cache);
                let payload = Arc::clone(&blob_set_payload);
                let counter = Arc::clone(&set_counter);
                let pool = Arc::clone(&pool);

                b.iter(|| {
                    let batch_size = (threads * BLOB_SET_OPS_PER_THREAD) as u64;
                    let batch_start = counter.fetch_add(batch_size, Ordering::Relaxed);

                    pool.install(|| {
                        (0..threads).into_par_iter().for_each(|tid| {
                            let thread_base = batch_start + (tid * BLOB_SET_OPS_PER_THREAD) as u64;
                            for offset in 0..BLOB_SET_OPS_PER_THREAD {
                                let key =
                                    format!("concurrent-blob-set-{}", thread_base + offset as u64);
                                cache
                                    .set(
                                        black_box(key.as_bytes()),
                                        black_box(payload.as_ref()),
                                        None,
                                    )
                                    .expect("concurrent blob set");
                            }
                        });
                    });
                })
            },
        );
    }
    blob_set_group.finish();

    let inline_value = "q".repeat(512);
    let mut inline_sharded_get_group = c.benchmark_group("concurrent_get_many_keys_sharded/inline");
    inline_sharded_get_group.measurement_time(Duration::from_secs(8));
    for &threads in &[2_usize, 4, 8] {
        let get_dir = tempfile::tempdir().expect("create concurrent inline get tempdir");
        let get_cache = Arc::new(open_namespace(
            get_dir.path(),
            NamespaceConfig {
                inline_threshold_bytes: 64 * 1024,
            },
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
            NamespaceConfig {
                inline_threshold_bytes: 64 * 1024,
            },
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

    let blob_value = "w".repeat(128 * 1024);
    let mut blob_sharded_get_group = c.benchmark_group("concurrent_get_many_keys_sharded/blob");
    blob_sharded_get_group.measurement_time(Duration::from_secs(8));
    for &threads in &[2_usize, 4, 8] {
        let get_dir = tempfile::tempdir().expect("create concurrent blob get tempdir");
        let get_cache = Arc::new(open_namespace(
            get_dir.path(),
            NamespaceConfig {
                inline_threshold_bytes: 64,
            },
            "open concurrent blob get cache",
        ));
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("build concurrent blob get pool"),
        );

        let blob_keys_by_thread: Vec<Vec<String>> = (0..threads)
            .map(|tid| {
                (0..KEYS_PER_THREAD)
                    .map(|slot| format!("blob-key-{threads}-{}", tid * KEYS_PER_THREAD + slot))
                    .collect()
            })
            .collect();

        for keys in &blob_keys_by_thread {
            for key in keys {
                get_cache
                    .set(key.as_str(), &blob_value, None)
                    .expect("seed concurrent blob keys");
            }
        }
        let blob_keys_by_thread = Arc::new(blob_keys_by_thread);

        blob_sharded_get_group.throughput(Throughput::Bytes(
            (blob_value.len() * GET_OPS_PER_THREAD * threads) as u64,
        ));
        blob_sharded_get_group.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                let cache = Arc::clone(&get_cache);
                let keys_by_thread = Arc::clone(&blob_keys_by_thread);
                let pool = Arc::clone(&pool);

                b.iter(|| {
                    pool.install(|| {
                        (0..threads).into_par_iter().for_each(|tid| {
                            let keys = &keys_by_thread[tid];
                            for op in 0..GET_OPS_PER_THREAD {
                                let slot = (op + tid * 31) % KEYS_PER_THREAD;
                                let value: Option<String> = cache
                                    .get(black_box(keys[slot].as_str()))
                                    .expect("concurrent get blob");
                                black_box(value.as_deref().map_or(0, str::len));
                            }
                        });
                    });
                })
            },
        );
    }
    blob_sharded_get_group.finish();
}

criterion_group!(
    benches,
    bench_set,
    bench_get,
    bench_contains_key,
    bench_concurrent
);
criterion_main!(benches);
