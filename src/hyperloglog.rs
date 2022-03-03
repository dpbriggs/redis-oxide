use crate::ops::RVec;
use crate::types::{Key, ReturnValue, StateRef, Value};
use crate::{make_reader, op_variants};

op_variants! {
    HyperLogLogOps,
    PfAdd(Key, RVec<Value>),
    PfCount(RVec<Key>),
    PfMerge(Key, RVec<Key>)
}

make_reader!(hyperloglogs, read_hyperloglogs);

// Error ratio from http://antirez.com/news/75
const HYPERLOGLOG_ERROR_RATIO: f64 = 0.0081;

fn default_hyperloglog() -> amadeus_streaming::HyperLogLog<Value> {
    amadeus_streaming::HyperLogLog::new(HYPERLOGLOG_ERROR_RATIO)
}

pub async fn hyperloglog_interact(hyperloglog_op: HyperLogLogOps, state: StateRef) -> ReturnValue {
    match hyperloglog_op {
        HyperLogLogOps::PfAdd(key, values) => {
            let mut pf_ref = state
                .hyperloglogs
                .entry(key)
                .or_insert_with(default_hyperloglog);
            let curr_card = pf_ref.len() as i64;
            values.into_iter().for_each(|e| pf_ref.push(&e));
            let new_card = pf_ref.len() as i64;
            ReturnValue::IntRes((new_card != curr_card).into())
        }
        HyperLogLogOps::PfCount(keys) => {
            // If there's only key, read that. redis appears to return zero if it doesn't exist.
            if keys.len() == 1 {
                return read_hyperloglogs!(state, &keys[0])
                    .map(|pf| pf.len() as i64)
                    .unwrap_or(0)
                    .into();
            }
            let res = keys
                .iter()
                .filter_map(|key| read_hyperloglogs!(state, key))
                .fold(default_hyperloglog(), |mut acc, curr_pf| {
                    acc.union(&curr_pf);
                    acc
                })
                .len() as i64;
            ReturnValue::IntRes(res)
        }
        HyperLogLogOps::PfMerge(dest_key, source_keys) => {
            let mut dest_pf = state
                .hyperloglogs
                .entry(dest_key)
                .or_insert_with(default_hyperloglog);
            source_keys
                .iter()
                .filter_map(|key| read_hyperloglogs!(state, key))
                .for_each(|ref pf| dest_pf.union(pf));
            ReturnValue::Ok
        }
    }
}
