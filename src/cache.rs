use std::path::{Path, PathBuf};
use std::time::Duration;

use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    de::Pool,
    rancor::{Error, Strategy},
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};

use crate::blob_store::BlobStore;
use crate::codec;
use crate::envelope::{RecordEnvelope, RecordV1, StoredValueV1};
use crate::error::{DiskCacheError, Result};
use crate::ttl_filter::{is_expired, make_factory_selector, now_ms};

const CACHE_KEYSPACE: &str = "cache";

#[derive(Debug, Clone)]
pub struct DiskCacheConfig {
    pub inline_threshold_bytes: usize,
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        Self {
            inline_threshold_bytes: 64 * 1024,
        }
    }
}

pub struct DiskCache {
    db: Database,
    keyspace: Keyspace,
    blob_store: BlobStore,
    config: DiskCacheConfig,
}

impl DiskCache {
    pub fn open(path: impl AsRef<Path>, config: DiskCacheConfig) -> Result<Self> {
        let root = path.as_ref();
        std::fs::create_dir_all(root)?;

        let db_dir = root.join("db");
        let blobs_dir = root.join("blobs");

        std::fs::create_dir_all(&db_dir)?;
        std::fs::create_dir_all(&blobs_dir)?;

        let db = Database::builder(&db_dir)
            .with_compaction_filter_factories(make_factory_selector(CACHE_KEYSPACE))
            .open()?;
        let keyspace = db.keyspace(CACHE_KEYSPACE, KeyspaceCreateOptions::default)?;

        let blob_store = BlobStore::new(blobs_dir)?;
        blob_store.cleanup_tmp_files()?;

        Ok(Self {
            db,
            keyspace,
            blob_store,
            config,
        })
    }

    pub fn set<K, V>(&self, key: K, value: &V, ttl: Option<Duration>) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, Error>>,
    {
        let key_bytes = key.as_ref();
        let payload = codec::serialize_value(value)?;

        let new_stored = if payload.len() > self.config.inline_threshold_bytes {
            let blob = self.blob_store.write_blob(key_bytes, &payload)?;
            StoredValueV1::BlobRef {
                rel_path: blob.rel_path,
                len: blob.len,
                checksum: blob.checksum,
            }
        } else {
            StoredValueV1::Inline { bytes: payload }
        };

        let expires_at_ms = ttl.map(|dur| now_ms().saturating_add(dur.as_millis() as u64));
        let record = RecordEnvelope::V1(RecordV1 {
            expires_at_ms,
            value: new_stored,
        });
        let encoded_record = codec::serialize_value(&record)?;
        let new_blob_rel_path = extract_blob_path(&record.as_v1().value).map(str::to_string);

        let old_record = self
            .keyspace
            .get(key_bytes)?
            .and_then(|v| codec::deserialize_value::<RecordEnvelope>(&v).ok());

        if let Err(err) = self.keyspace.insert(key_bytes, encoded_record) {
            if let Some(rel_path) = new_blob_rel_path {
                self.blob_store.delete_blob_best_effort(&rel_path);
            }
            return Err(DiskCacheError::Fjall(err));
        }

        if let Some(old) = old_record {
            self.maybe_delete_old_blob(old.as_v1().value.clone(), &record.as_v1().value);
        }

        Ok(())
    }

    pub fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]>,
        V: Archive,
        V::Archived:
            for<'a> CheckBytes<HighValidator<'a, Error>> + Deserialize<V, Strategy<Pool, Error>>,
    {
        let key_bytes = key.as_ref();
        let Some(bytes) = self.keyspace.get(key_bytes)? else {
            return Ok(None);
        };

        let record: RecordEnvelope = codec::deserialize_value(&bytes)?;
        let record_v1 = record.as_v1();

        if is_expired(record_v1.expires_at_ms, now_ms()) {
            let _ = self.remove(key_bytes);
            return Ok(None);
        }

        let payload = match &record_v1.value {
            StoredValueV1::Inline { bytes } => bytes.clone(),
            StoredValueV1::BlobRef {
                rel_path,
                len,
                checksum,
            } => {
                let bytes = self.blob_store.read_blob(rel_path, *checksum)?;
                if bytes.len() as u64 != *len {
                    return Err(DiskCacheError::Deserialize(format!(
                        "blob length mismatch for {rel_path}: expected {len}, got {}",
                        bytes.len()
                    )));
                }
                bytes
            }
        };

        let value = codec::deserialize_value::<V>(&payload)?;
        Ok(Some(value))
    }

    pub fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref();
        let old_record = self
            .keyspace
            .get(key_bytes)?
            .and_then(|v| codec::deserialize_value::<RecordEnvelope>(&v).ok());

        self.keyspace.remove(key_bytes)?;

        if let Some(old) = old_record {
            if let Some(rel_path) = extract_blob_path(&old.as_v1().value) {
                self.blob_store.delete_blob_best_effort(rel_path);
            }
        }

        Ok(())
    }

    pub fn contains_key<K>(&self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        let Some(bytes) = self.keyspace.get(key.as_ref())? else {
            return Ok(false);
        };
        let Ok(record) = codec::deserialize_value::<RecordEnvelope>(&bytes) else {
            return Ok(false);
        };

        Ok(!is_expired(record.as_v1().expires_at_ms, now_ms()))
    }

    pub fn persist(&self) -> Result<()> {
        self.db.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }

    pub fn blob_root(&self) -> &Path {
        self.blob_store.root_dir()
    }

    pub fn blob_path_for_key<K>(&self, key: K) -> PathBuf
    where
        K: AsRef<[u8]>,
    {
        self.blob_store
            .blob_full_path(&crate::blob_store::build_rel_path(key.as_ref()))
    }

    fn maybe_delete_old_blob(&self, old: StoredValueV1, new: &StoredValueV1) {
        let Some(old_rel_path) = extract_blob_path(&old) else {
            return;
        };

        match new {
            StoredValueV1::BlobRef { rel_path, .. } if rel_path == old_rel_path => {}
            _ => self.blob_store.delete_blob_best_effort(old_rel_path),
        }
    }
}

fn extract_blob_path(value: &StoredValueV1) -> Option<&str> {
    match value {
        StoredValueV1::BlobRef { rel_path, .. } => Some(rel_path.as_str()),
        StoredValueV1::Inline { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    struct DemoValue {
        name: String,
        points: Vec<u32>,
    }

    fn new_cache(threshold: usize) -> (tempfile::TempDir, DiskCache) {
        let dir = tempfile::tempdir().expect("create tempdir");
        let cache = DiskCache::open(
            dir.path(),
            DiskCacheConfig {
                inline_threshold_bytes: threshold,
            },
        )
        .expect("open cache");
        (dir, cache)
    }

    #[test]
    fn inline_roundtrip() {
        let (_dir, cache) = new_cache(1024);
        let value = DemoValue {
            name: "alice".to_string(),
            points: vec![1, 2, 3],
        };

        cache.set("a", &value, None).expect("set");
        let got: Option<DemoValue> = cache.get("a").expect("get");
        assert_eq!(Some(value), got);
    }

    #[test]
    fn blob_roundtrip_and_remove() {
        let (_dir, cache) = new_cache(8);
        let value = "x".repeat(10_000);

        cache.set("big", &value, None).expect("set");
        let path = cache.blob_path_for_key("big");
        assert!(path.exists());

        let got: Option<String> = cache.get("big").expect("get");
        assert_eq!(Some(value), got);

        cache.remove("big").expect("remove");
        assert!(!path.exists());
    }

    #[test]
    fn overwrite_deletes_old_blob() {
        let (_dir, cache) = new_cache(8);

        cache
            .set("k", &"a".repeat(20_000), None)
            .expect("first set");
        let path = cache.blob_path_for_key("k");
        assert!(path.exists());

        cache
            .set("k", &"small".to_string(), None)
            .expect("overwrite");

        assert!(!path.exists());
    }

    #[test]
    fn ttl_expired_returns_none() {
        let (_dir, cache) = new_cache(1024);

        cache
            .set("ttl", &"hello".to_string(), Some(Duration::from_millis(10)))
            .expect("set ttl");
        std::thread::sleep(Duration::from_millis(20));

        let got: Option<String> = cache.get("ttl").expect("get");
        assert_eq!(None, got);
    }

    #[test]
    fn cleans_part_files_on_open() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let blobs_tmp = dir.path().join("blobs").join(".tmp");
        std::fs::create_dir_all(&blobs_tmp).expect("create tmp");
        let part = blobs_tmp.join("stale.part");
        std::fs::write(&part, b"abc").expect("write part");

        let _cache = DiskCache::open(dir.path(), DiskCacheConfig::default()).expect("open");
        assert!(!part.exists());
    }
}
