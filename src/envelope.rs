use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum RecordEnvelope {
    V1(RecordV1),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct RecordV1 {
    pub expires_at_ms: Option<u64>,
    pub value: StoredValueV1,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum StoredValueV1 {
    Inline {
        bytes: Vec<u8>,
    },
    BlobRef {
        rel_path: String,
        len: u64,
        checksum: u32,
    },
}

impl RecordEnvelope {
    pub fn as_v1(&self) -> &RecordV1 {
        match self {
            Self::V1(v1) => v1,
        }
    }
}
