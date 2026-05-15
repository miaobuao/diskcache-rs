use super::*;
use fjall::{KeyspaceCreateOptions, KvSeparationOptions};

#[derive(Clone)]
pub struct NamespaceConfig {
    pub keyspace_create_options: KeyspaceCreateOptions,
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        Self {
            keyspace_create_options: KeyspaceCreateOptions::default().with_kv_separation(Some(
                KvSeparationOptions::default().separation_threshold(4 * 1024),
            )),
        }
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
        let new_stored = StoredValueV1::Inline { bytes: payload };

        let expires_at_ms = ttl.map(|dur| now_ms().saturating_add(dur.as_millis() as u64));
        let record = RecordEnvelope::V1(RecordV1 {
            expires_at_ms,
            value: new_stored,
        });
        let encoded_record = codec::serialize_value(&record)?;

        let old_record = self
            .keyspace
            .get(key_bytes)?
            .and_then(|v| codec::deserialize_value::<RecordEnvelope>(&v).ok());

        self.keyspace.insert(key_bytes, encoded_record)?;

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
