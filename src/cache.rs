use std::path::{Path, PathBuf};
use std::sync::Mutex;
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
use uuid::Uuid;

use crate::blob_store::BlobStore;
use crate::codec;
use crate::envelope::{RecordEnvelope, RecordV1, StoredValueV1};
use crate::error::{DiskCacheError, Result};
use crate::ttl_filter::{is_expired, make_factory_selector, now_ms};

const META_KEYSPACE: &str = "meta";
const NAMESPACE_KEYSPACE_PREFIX: &str = "ns_";

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct NamespaceConfig {
    pub inline_threshold_bytes: usize,
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        Self {
            inline_threshold_bytes: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct NamespaceMeta {
    id: String,
    config: NamespaceConfig,
}

pub struct DiskCache {
    db: Database,
    meta: Keyspace,
    blobs_root: PathBuf,
    namespace_lock: Mutex<()>,
}

pub struct CacheNamespace {
    _db: Database,
    keyspace: Keyspace,
    blob_store: BlobStore,
    config: NamespaceConfig,
}

impl DiskCache {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref();
        std::fs::create_dir_all(root)?;

        let db_dir = root.join("db");
        let blobs_root = root.join("blobs");

        std::fs::create_dir_all(&db_dir)?;
        std::fs::create_dir_all(&blobs_root)?;

        let db = Database::builder(&db_dir)
            .with_compaction_filter_factories(make_factory_selector(NAMESPACE_KEYSPACE_PREFIX))
            .open()?;
        let meta = db.keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)?;

        Ok(Self {
            db,
            meta,
            blobs_root,
            namespace_lock: Mutex::new(()),
        })
    }

    pub fn namespace(
        &self,
        name: impl AsRef<str>,
        config: NamespaceConfig,
    ) -> Result<CacheNamespace> {
        let name = validate_namespace_name(name.as_ref())?;
        let _guard = self
            .namespace_lock
            .lock()
            .expect("namespace lock is poisoned");

        let meta = match self.namespace_meta(name)? {
            Some(mut meta) => {
                meta.config = config;
                self.meta
                    .insert(name.as_bytes(), codec::serialize_value(&meta)?)?;
                meta
            }
            None => {
                let meta = NamespaceMeta {
                    id: Uuid::new_v4().simple().to_string(),
                    config,
                };
                self.meta
                    .insert(name.as_bytes(), codec::serialize_value(&meta)?)?;
                meta
            }
        };

        self.open_namespace(&meta)
    }

    pub fn delete_namespace(&self, name: impl AsRef<str>) -> Result<bool> {
        let name = validate_namespace_name(name.as_ref())?;
        let _guard = self
            .namespace_lock
            .lock()
            .expect("namespace lock is poisoned");

        let Some(meta) = self.namespace_meta(name)? else {
            return Ok(false);
        };

        let keyspace = self.db.keyspace(
            &namespace_keyspace_name(&meta.id),
            KeyspaceCreateOptions::default,
        )?;
        self.db.delete_keyspace(keyspace)?;
        self.meta.remove(name.as_bytes())?;

        let blob_dir = self.namespace_blob_root(&meta.id);
        if blob_dir.exists() {
            std::fs::remove_dir_all(blob_dir)?;
        }

        Ok(true)
    }

    pub fn list_namespaces(&self) -> Result<Vec<String>> {
        self.meta
            .iter()
            .map(|item| {
                let key = item.key()?;
                String::from_utf8(key.as_ref().to_vec())
                    .map_err(|err| DiskCacheError::Deserialize(err.to_string()))
            })
            .collect()
    }

    pub fn persist(&self) -> Result<()> {
        self.db.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }

    pub fn blob_root(&self) -> &Path {
        &self.blobs_root
    }

    fn namespace_meta(&self, name: &str) -> Result<Option<NamespaceMeta>> {
        self.meta
            .get(name.as_bytes())?
            .map(|meta| codec::deserialize_value::<NamespaceMeta>(&meta))
            .transpose()
    }

    fn open_namespace(&self, meta: &NamespaceMeta) -> Result<CacheNamespace> {
        let keyspace = self.db.keyspace(
            &namespace_keyspace_name(&meta.id),
            KeyspaceCreateOptions::default,
        )?;
        let blob_store = BlobStore::new(self.namespace_blob_root(&meta.id))?;
        blob_store.cleanup_tmp_files()?;

        Ok(CacheNamespace {
            _db: self.db.clone(),
            keyspace,
            blob_store,
            config: meta.config.clone(),
        })
    }

    fn namespace_blob_root(&self, id: &str) -> PathBuf {
        self.blobs_root.join(id)
    }
}

impl CacheNamespace {
    pub fn config(&self) -> &NamespaceConfig {
        &self.config
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

        if let Some(old) = old_record
            && let Some(rel_path) = extract_blob_path(&old.as_v1().value)
        {
            self.blob_store.delete_blob_best_effort(rel_path);
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

    pub fn clear(&self) -> Result<usize> {
        let keys = self
            .keyspace
            .iter()
            .map(|item| item.key().map_err(DiskCacheError::Fjall))
            .collect::<Result<Vec<_>>>()?;

        let count = keys.len();
        for key in keys {
            self.remove(key.as_ref())?;
        }

        Ok(count)
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

fn validate_namespace_name(name: &str) -> Result<&str> {
    if name.is_empty() {
        return Err(DiskCacheError::InvalidNamespaceName(
            "namespace name cannot be empty".to_string(),
        ));
    }

    Ok(name)
}

fn namespace_keyspace_name(id: &str) -> String {
    format!("{NAMESPACE_KEYSPACE_PREFIX}{id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    struct DemoValue {
        name: String,
        points: Vec<u32>,
    }

    fn new_cache(_threshold: usize) -> (tempfile::TempDir, DiskCache) {
        let dir = tempfile::tempdir().expect("create tempdir");
        let cache = DiskCache::open(dir.path()).expect("open cache");
        (dir, cache)
    }

    fn config(threshold: usize) -> NamespaceConfig {
        NamespaceConfig {
            inline_threshold_bytes: threshold,
        }
    }

    #[test]
    fn namespace_roundtrip() {
        let (_dir, cache) = new_cache(1024);
        let users = cache.namespace("users", config(1024)).expect("open users");
        let value = DemoValue {
            name: "alice".to_string(),
            points: vec![1, 2, 3],
        };

        users.set("a", &value, None).expect("set");
        let got: Option<DemoValue> = users.get("a").expect("get");
        assert_eq!(Some(value), got);
    }

    #[test]
    fn namespaces_isolate_same_key() {
        let (_dir, cache) = new_cache(1024);
        let users = cache.namespace("users", config(1024)).expect("open users");
        let sessions = cache
            .namespace("sessions", config(1024))
            .expect("open sessions");

        users
            .set("1", &"alice".to_string(), None)
            .expect("set users");
        sessions
            .set("1", &"session-token".to_string(), None)
            .expect("set sessions");

        let user: Option<String> = users.get("1").expect("get users");
        let session: Option<String> = sessions.get("1").expect("get sessions");

        assert_eq!(Some("alice".to_string()), user);
        assert_eq!(Some("session-token".to_string()), session);
    }

    #[test]
    fn blob_roundtrip_and_remove() {
        let (_dir, cache) = new_cache(8);
        let users = cache.namespace("users", config(8)).expect("open users");
        let value = "x".repeat(10_000);

        users.set("big", &value, None).expect("set");
        let path = users.blob_path_for_key("big");
        assert!(path.exists());

        let got: Option<String> = users.get("big").expect("get");
        assert_eq!(Some(value), got);

        users.remove("big").expect("remove");
        assert!(!path.exists());
    }

    #[test]
    fn namespaces_use_independent_inline_thresholds() {
        let (_dir, cache) = new_cache(1024);
        let inline = cache
            .namespace("inline", config(1024))
            .expect("open inline");
        let blob = cache.namespace("blob", config(8)).expect("open blob");
        let value = "x".repeat(128);

        inline.set("k", &value, None).expect("set inline");
        blob.set("k", &value, None).expect("set blob");

        assert!(!inline.blob_path_for_key("k").exists());
        assert!(blob.blob_path_for_key("k").exists());
    }

    #[test]
    fn namespace_call_updates_stored_config_for_new_handle() {
        let (_dir, cache) = new_cache(1024);
        let users = cache.namespace("users", config(1024)).expect("open users");
        let value = "x".repeat(128);

        users.set("before", &value, None).expect("set before");
        assert!(!users.blob_path_for_key("before").exists());

        let updated = cache.namespace("users", config(8)).expect("reopen users");
        updated.set("after", &value, None).expect("set after");

        assert_eq!(8, updated.config().inline_threshold_bytes);
        assert!(updated.blob_path_for_key("after").exists());
    }

    #[test]
    fn namespace_config_persists_in_meta() {
        let dir = tempfile::tempdir().expect("create tempdir");
        {
            let cache = DiskCache::open(dir.path()).expect("open cache");
            let users = cache.namespace("users", config(8)).expect("open users");
            users.set("k", &"x".repeat(128), None).expect("set");
            cache.persist().expect("persist");
        }

        let cache = DiskCache::open(dir.path()).expect("reopen cache");
        let meta = cache
            .namespace_meta("users")
            .expect("read meta")
            .expect("users meta");
        assert_eq!(8, meta.config.inline_threshold_bytes);

        let users = cache
            .namespace("users", config(1024))
            .expect("reopen users with new config");
        assert_eq!(1024, users.config().inline_threshold_bytes);
    }

    #[test]
    fn same_key_blob_paths_are_isolated_by_namespace_directory() {
        let (_dir, cache) = new_cache(8);
        let left = cache.namespace("left", config(8)).expect("open left");
        let right = cache.namespace("right", config(8)).expect("open right");

        left.set("shared", &"a".repeat(20_000), None)
            .expect("set left");
        right
            .set("shared", &"b".repeat(20_000), None)
            .expect("set right");

        let left_path = left.blob_path_for_key("shared");
        let right_path = right.blob_path_for_key("shared");
        assert_ne!(left_path, right_path);
        assert!(left_path.exists());
        assert!(right_path.exists());

        left.remove("shared").expect("remove left");
        assert!(!left_path.exists());
        assert!(right_path.exists());

        let got: Option<String> = right.get("shared").expect("get right");
        assert_eq!(Some("b".repeat(20_000)), got);
    }

    #[test]
    fn overwrite_deletes_old_blob() {
        let (_dir, cache) = new_cache(8);
        let users = cache.namespace("users", config(8)).expect("open users");

        users
            .set("k", &"a".repeat(20_000), None)
            .expect("first set");
        let path = users.blob_path_for_key("k");
        assert!(path.exists());

        users
            .set("k", &"small".to_string(), None)
            .expect("overwrite");

        assert!(!path.exists());
    }

    #[test]
    fn ttl_expired_returns_none() {
        let (_dir, cache) = new_cache(1024);
        let sessions = cache
            .namespace("sessions", config(1024))
            .expect("open sessions");

        sessions
            .set("ttl", &"hello".to_string(), Some(Duration::from_millis(10)))
            .expect("set ttl");
        std::thread::sleep(Duration::from_millis(20));

        let got: Option<String> = sessions.get("ttl").expect("get");
        assert_eq!(None, got);
    }

    #[test]
    fn clear_namespace_removes_only_that_namespace() {
        let (_dir, cache) = new_cache(8);
        let users = cache.namespace("users", config(8)).expect("open users");
        let sessions = cache
            .namespace("sessions", config(8))
            .expect("open sessions");

        users
            .set("1", &"u".repeat(20_000), None)
            .expect("set user 1");
        users
            .set("2", &"small".to_string(), None)
            .expect("set user 2");
        sessions
            .set("1", &"s".repeat(20_000), None)
            .expect("set session");

        let user_blob = users.blob_path_for_key("1");
        let session_blob = sessions.blob_path_for_key("1");

        assert_eq!(2, users.clear().expect("clear users"));
        assert!(!user_blob.exists());
        assert!(session_blob.exists());
        assert!(!users.contains_key("1").expect("contains user 1"));
        assert!(!users.contains_key("2").expect("contains user 2"));
        assert!(sessions.contains_key("1").expect("contains session"));
    }

    #[test]
    fn list_and_delete_namespaces() {
        let (dir, cache) = new_cache(8);
        let users = cache.namespace("users", config(8)).expect("open users");
        let sessions = cache
            .namespace("sessions", config(8))
            .expect("open sessions");

        users.set("1", &"u".repeat(20_000), None).expect("set user");
        sessions
            .set("1", &"s".repeat(20_000), None)
            .expect("set session");
        let users_blob_root = users.blob_root().to_path_buf();

        let mut names = cache.list_namespaces().expect("list namespaces");
        names.sort();
        assert_eq!(vec!["sessions".to_string(), "users".to_string()], names);

        assert!(cache.delete_namespace("users").expect("delete users"));
        assert!(!cache.delete_namespace("users").expect("delete users again"));
        assert!(!users_blob_root.exists());

        let reopened_users = cache.namespace("users", config(8)).expect("reopen users");
        let missing: Option<String> = reopened_users.get("1").expect("get deleted user");
        let session: Option<String> = sessions.get("1").expect("get session");
        assert_eq!(None, missing);
        assert_eq!(Some("s".repeat(20_000)), session);

        assert!(dir.path().join("blobs").exists());
    }

    #[test]
    fn rejects_empty_namespace_name() {
        let (_dir, cache) = new_cache(1024);
        assert!(matches!(
            cache.namespace("", NamespaceConfig::default()),
            Err(DiskCacheError::InvalidNamespaceName(_))
        ));
    }

    #[test]
    fn cleans_part_files_on_namespace_open() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let cache = DiskCache::open(dir.path()).expect("open");
        let users = cache
            .namespace("users", NamespaceConfig::default())
            .expect("open users");
        let part = users.blob_root().join(".tmp").join("stale.part");
        std::fs::write(&part, b"abc").expect("write part");
        drop(users);

        let _users = cache
            .namespace("users", NamespaceConfig::default())
            .expect("reopen users");
        assert!(!part.exists());
    }
}
