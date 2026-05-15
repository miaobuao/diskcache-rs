use std::path::Path;
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

use crate::codec;
use crate::envelope::{RecordEnvelope, RecordV1, StoredValueV1};
use crate::error::{DiskCacheError, Result};
use crate::ttl_filter::{is_expired, make_factory_selector, now_ms};

mod namespace;
pub use namespace::NamespaceConfig;

const META_KEYSPACE: &str = "meta";
const NAMESPACE_KEYSPACE_PREFIX: &str = "ns_";

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct NamespaceMeta {
    id: String,
}

pub struct DiskCache {
    db: Database,
    meta: Keyspace,
    namespace_lock: Mutex<()>,
}

pub struct CacheNamespace {
    _db: Database,
    keyspace: Keyspace,
    config: NamespaceConfig,
}

impl DiskCache {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref();
        std::fs::create_dir_all(root)?;

        let db_dir = root.join("db");

        std::fs::create_dir_all(&db_dir)?;

        let db = Database::builder(&db_dir)
            .with_compaction_filter_factories(make_factory_selector(NAMESPACE_KEYSPACE_PREFIX))
            .open()?;
        let meta = db.keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)?;

        Ok(Self {
            db,
            meta,
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
            Some(meta) => meta,
            None => {
                let meta = NamespaceMeta {
                    id: Uuid::new_v4().simple().to_string(),
                };
                self.meta
                    .insert(name.as_bytes(), codec::serialize_value(&meta)?)?;
                meta
            }
        };

        self.open_namespace(&meta, config)
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

    fn namespace_meta(&self, name: &str) -> Result<Option<NamespaceMeta>> {
        self.meta
            .get(name.as_bytes())?
            .map(|meta| codec::deserialize_value::<NamespaceMeta>(&meta))
            .transpose()
    }

    fn open_namespace(
        &self,
        meta: &NamespaceMeta,
        config: NamespaceConfig,
    ) -> Result<CacheNamespace> {
        let keyspace_create_options = config.keyspace_create_options.clone();
        let keyspace = self
            .db
            .keyspace(&namespace_keyspace_name(&meta.id), move || {
                keyspace_create_options.clone()
            })?;

        Ok(CacheNamespace {
            _db: self.db.clone(),
            keyspace,
            config,
        })
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
        let threshold = u32::try_from(threshold).expect("threshold should fit in u32 for tests");
        NamespaceConfig {
            keyspace_create_options: KeyspaceCreateOptions::default().with_kv_separation(Some(
                fjall::KvSeparationOptions::default().separation_threshold(threshold),
            )),
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
    fn large_value_roundtrip_and_remove() {
        let (_dir, cache) = new_cache(8);
        let users = cache.namespace("users", config(8)).expect("open users");
        let value = "x".repeat(10_000);

        users.set("big", &value, None).expect("set");

        let got: Option<String> = users.get("big").expect("get");
        assert_eq!(Some(value), got);

        users.remove("big").expect("remove");
    }

    #[test]
    fn namespaces_use_independent_kv_separation_thresholds() {
        let (_dir, cache) = new_cache(1024);
        let inline = cache
            .namespace("inline", config(1024))
            .expect("open inline");
        let separated = cache
            .namespace("separated", config(8))
            .expect("open separated");
        let value = "x".repeat(128);

        inline.set("k", &value, None).expect("set inline");
        separated.set("k", &value, None).expect("set separated");

        let inline_value: Option<String> = inline.get("k").expect("get inline");
        let separated_value: Option<String> = separated.get("k").expect("get separated");
        assert_eq!(Some(value.clone()), inline_value);
        assert_eq!(Some(value), separated_value);
    }

    #[test]
    fn namespace_call_updates_stored_config_for_new_handle() {
        let (_dir, cache) = new_cache(1024);
        let users = cache.namespace("users", config(1024)).expect("open users");
        let value = "x".repeat(128);

        users.set("before", &value, None).expect("set before");

        let updated = cache.namespace("users", config(8)).expect("reopen users");
        updated.set("after", &value, None).expect("set after");

        let before: Option<String> = updated.get("before").expect("get before");
        let after: Option<String> = updated.get("after").expect("get after");
        assert_eq!(Some(value.clone()), before);
        assert_eq!(Some(value), after);
    }

    #[test]
    fn namespace_meta_persists_id_only() {
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
        assert!(!meta.id.is_empty());
        let meta_id = meta.id.clone();

        let _users = cache
            .namespace("users", config(1024))
            .expect("reopen users with different config");
        let meta_after = cache
            .namespace_meta("users")
            .expect("read meta after reopen")
            .expect("users meta after reopen");
        assert_eq!(meta_id, meta_after.id);
    }

    #[test]
    fn same_key_large_values_are_isolated_by_namespace() {
        let (_dir, cache) = new_cache(8);
        let left = cache.namespace("left", config(8)).expect("open left");
        let right = cache.namespace("right", config(8)).expect("open right");

        left.set("shared", &"a".repeat(20_000), None)
            .expect("set left");
        right
            .set("shared", &"b".repeat(20_000), None)
            .expect("set right");

        left.remove("shared").expect("remove left");

        let got: Option<String> = right.get("shared").expect("get right");
        assert_eq!(Some("b".repeat(20_000)), got);
    }

    #[test]
    fn overwrite_large_value_roundtrip() {
        let (_dir, cache) = new_cache(8);
        let users = cache.namespace("users", config(8)).expect("open users");

        users
            .set("k", &"a".repeat(20_000), None)
            .expect("first set");

        users
            .set("k", &"small".to_string(), None)
            .expect("overwrite");

        let got: Option<String> = users.get("k").expect("get");
        assert_eq!(Some("small".to_string()), got);
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

        assert_eq!(2, users.clear().expect("clear users"));
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

        let mut names = cache.list_namespaces().expect("list namespaces");
        names.sort();
        assert_eq!(vec!["sessions".to_string(), "users".to_string()], names);

        assert!(cache.delete_namespace("users").expect("delete users"));
        assert!(!cache.delete_namespace("users").expect("delete users again"));

        let reopened_users = cache.namespace("users", config(8)).expect("reopen users");
        let missing: Option<String> = reopened_users.get("1").expect("get deleted user");
        let session: Option<String> = sessions.get("1").expect("get session");
        assert_eq!(None, missing);
        assert_eq!(Some("s".repeat(20_000)), session);
        assert!(dir.path().join("db").exists());
    }

    #[test]
    fn rejects_empty_namespace_name() {
        let (_dir, cache) = new_cache(1024);
        assert!(matches!(
            cache.namespace("", config(1024)),
            Err(DiskCacheError::InvalidNamespaceName(_))
        ));
    }

    #[test]
    fn set_writes_v1_inline_record() {
        let (_dir, cache) = new_cache(1024);
        let users = cache.namespace("users", config(1024)).expect("open users");

        users.set("shape", &"value".to_string(), None).expect("set");

        let bytes = users
            .keyspace
            .get(b"shape")
            .expect("read raw record")
            .expect("raw record");
        let record: RecordEnvelope = codec::deserialize_value(&bytes).expect("decode record");
        let RecordEnvelope::V1(record) = record;
        let StoredValueV1::Inline { bytes } = record.value;
        let value: String = codec::deserialize_value(&bytes).expect("decode value");

        assert_eq!(None, record.expires_at_ms);
        assert_eq!("value", value);
    }
}
