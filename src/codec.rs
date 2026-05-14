use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    de::Pool,
    rancor::{Error, Strategy},
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};

use crate::error::{DiskCacheError, Result};

pub fn serialize_value<T>(value: &T) -> Result<Vec<u8>>
where
    T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, Error>>,
{
    let bytes =
        rkyv::to_bytes::<Error>(value).map_err(|err| DiskCacheError::Serialize(err.to_string()))?;
    Ok(bytes.to_vec())
}

pub fn deserialize_value<T>(bytes: &[u8]) -> Result<T>
where
    T: Archive,
    T::Archived:
        for<'a> CheckBytes<HighValidator<'a, Error>> + Deserialize<T, Strategy<Pool, Error>>,
{
    rkyv::from_bytes::<T, Error>(bytes).map_err(|err| DiskCacheError::Deserialize(err.to_string()))
}
