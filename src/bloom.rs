use crate::make_reader;
use crate::types::{Key, ReturnValue, StateRef, Value, RedisBool};
use growable_bloom_filter::GrowableBloom;

#[derive(Debug, Clone)]
pub enum BloomOps {
    // Key Value
    BInsert(Key, Value),
    BContains(Key, Value),
}

const DESIRED_FAILURE_RATE: f64 = 0.05;
const EST_INSERTS: usize = 10;

make_reader!(blooms, read_blooms);

pub async fn bloom_interact(bloom_op: BloomOps, state: StateRef) -> ReturnValue {
    match bloom_op {
        BloomOps::BInsert(bloom_key, value) => {
            let mut blooms = state.blooms.write();
            (*blooms)
                .entry(bloom_key)
                .or_insert_with(|| GrowableBloom::new(DESIRED_FAILURE_RATE, EST_INSERTS))
                .insert(value);
            ReturnValue::Ok
        }
        BloomOps::BContains(bloom_key, value) => read_blooms!(state, &bloom_key)
            .map(|bloom| bloom.contains(value) as RedisBool)
            .unwrap_or(0)
            .into(),
    }
}

#[cfg(test)]
mod test_bloom {
    use crate::bloom::{BloomOps, bloom_interact};
    use crate::types::{ReturnValue, State};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_insert() {
        let (key, v) = (b"key".to_vec(), b"v".to_vec());
        let eng = Arc::new(State::default());
        let res = bloom_interact(BloomOps::BInsert(key, v), eng.clone()).await;
        assert_eq!(res, ReturnValue::Ok);
    }

    #[tokio::test]
    async fn test_contains() {
        let (key, v) = (b"key".to_vec(), b"v".to_vec());
        let eng = Arc::new(State::default());
        let res = bloom_interact(BloomOps::BContains(key.clone(), v.clone()), eng.clone()).await;
        assert_eq!(res, ReturnValue::IntRes(0));
        bloom_interact(BloomOps::BInsert(key.clone(), v.clone()), eng.clone()).await;
        let res = bloom_interact(BloomOps::BContains(key, v), eng.clone()).await;
        assert_eq!(res, ReturnValue::IntRes(1));
    }
}
