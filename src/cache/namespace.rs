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
                KvSeparationOptions::default().separation_threshold(1024),
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

        self.keyspace.insert(key_bytes, encoded_record)?;

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

        let StoredValueV1::Inline { bytes } = &record_v1.value;

        let value = codec::deserialize_value::<V>(bytes)?;
        Ok(Some(value))
    }

    pub fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.keyspace.remove(key.as_ref())?;
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
}
