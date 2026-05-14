use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fjall::compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict};

use crate::codec;
use crate::envelope::RecordEnvelope;

#[derive(Debug)]
pub struct TtlFilterFactory;

impl Factory for TtlFilterFactory {
    fn name(&self) -> &str {
        "diskcache_ttl"
    }

    fn make_filter(&self, _ctx: &Context) -> Box<dyn CompactionFilter> {
        Box::new(TtlFilter)
    }
}

#[derive(Debug)]
struct TtlFilter;

impl CompactionFilter for TtlFilter {
    fn filter_item(
        &mut self,
        item: ItemAccessor<'_>,
        _ctx: &Context,
    ) -> fjall::compaction::filter::CompactionFilterResult {
        let value = item.value()?;
        let envelope = match codec::deserialize_value::<RecordEnvelope>(&value) {
            Ok(v) => v,
            Err(_) => return Ok(Verdict::Keep),
        };

        if is_expired(envelope.as_v1().expires_at_ms, now_ms()) {
            return Ok(Verdict::Remove);
        }

        Ok(Verdict::Keep)
    }
}

pub fn make_factory_selector(
    keyspace_name: &'static str,
) -> Arc<dyn Fn(&str) -> Option<Arc<dyn Factory>> + Send + Sync> {
    Arc::new(move |name: &str| {
        if name == keyspace_name {
            Some(Arc::new(TtlFilterFactory))
        } else {
            None
        }
    })
}

pub fn now_ms() -> u64 {
    let Ok(dur) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    dur.as_millis() as u64
}

pub fn is_expired(expires_at_ms: Option<u64>, now_ms: u64) -> bool {
    match expires_at_ms {
        Some(ts) => now_ms >= ts,
        None => false,
    }
}
