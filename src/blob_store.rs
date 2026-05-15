use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::error::{DiskCacheError, Result};

const BASE36_ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

#[derive(Debug, Clone)]
pub struct BlobStore {
    root_dir: PathBuf,
    tmp_dir: PathBuf,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct BlobWriteResult {
    pub rel_path: String,
    pub len: u64,
    pub checksum: u32,
}

impl BlobStore {
    pub fn new(root_dir: PathBuf) -> Result<Self> {
        let tmp_dir = root_dir.join(".tmp");
        fs::create_dir_all(&root_dir)?;
        fs::create_dir_all(&tmp_dir)?;
        Ok(Self { root_dir, tmp_dir })
    }

    pub fn cleanup_tmp_files(&self) -> Result<()> {
        if !self.tmp_dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(&self.tmp_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("part") {
                let _ = fs::remove_file(path);
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn write_blob(&self, key: &[u8], bytes: &[u8]) -> Result<BlobWriteResult> {
        let rel_path = build_rel_path(key);
        let final_path = self.root_dir.join(&rel_path);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp_name = format!("{}.part", Uuid::new_v4());
        let tmp_path = self.tmp_dir.join(tmp_name);

        fs::write(&tmp_path, bytes)?;
        fs::rename(&tmp_path, &final_path)?;

        Ok(BlobWriteResult {
            rel_path,
            len: bytes.len() as u64,
            checksum: crc32fast::hash(bytes),
        })
    }

    pub fn read_blob(&self, rel_path: &str, expected_checksum: u32) -> Result<Vec<u8>> {
        let path = self.root_dir.join(rel_path);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Err(DiskCacheError::BlobMissing(path));
            }
            Err(err) => return Err(err.into()),
        };
        let actual = crc32fast::hash(&bytes);
        if actual != expected_checksum {
            return Err(DiskCacheError::BlobChecksumMismatch {
                path,
                expected: expected_checksum,
                actual,
            });
        }

        Ok(bytes)
    }

    pub fn delete_blob_best_effort(&self, rel_path: &str) {
        let path = self.root_dir.join(rel_path);
        let _ = fs::remove_file(path);
    }

    pub fn blob_full_path(&self, rel_path: &str) -> PathBuf {
        self.root_dir.join(rel_path)
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }
}

pub fn build_rel_path(key: &[u8]) -> String {
    let primary_hash = crc32fast::hash(key);
    let h36 = to_base36(primary_hash as u64);
    let padded = left_pad_to_len(&h36, 4);

    let first = &padded[padded.len() - 2..];
    let second = &padded[padded.len() - 4..padded.len() - 2];

    let digest = blake3::hash(key);
    let digest_hex = digest.to_hex();
    let short = &digest_hex.as_str()[..8];

    format!("{}/{}/{}-{}.val", first, second, padded, short)
}

fn left_pad_to_len(s: &str, min_len: usize) -> String {
    if s.len() >= min_len {
        return s.to_string();
    }

    let mut out = String::with_capacity(min_len);
    for _ in 0..(min_len - s.len()) {
        out.push('0');
    }
    out.push_str(s);
    out
}

fn to_base36(mut n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }

    let mut buf = Vec::new();
    while n > 0 {
        let idx = (n % 36) as usize;
        buf.push(BASE36_ALPHABET[idx] as char);
        n /= 36;
    }
    buf.iter().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::build_rel_path;

    #[test]
    fn builds_two_level_path() {
        let p = build_rel_path(b"user:1001");
        let parts: Vec<&str> = p.split('/').collect();
        assert_eq!(3, parts.len());
        assert_eq!(2, parts[0].len());
        assert_eq!(2, parts[1].len());
        assert!(parts[2].ends_with(".val"));
    }
}
