use crate::make_reader;
use crate::types::{InteractionRes, Key, ReturnValue, State, StateInteration, Value};
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

impl StateInteration for BloomOps {
    fn interact(self, state: State) -> InteractionRes {
        match self {
            BloomOps::BInsert(bloom_key, value) => {
                let mut blooms = state.blooms.write();
                (*blooms)
                    .entry(bloom_key)
                    .or_insert_with(|| GrowableBloom::new(DESIRED_FAILURE_RATE, EST_INSERTS))
                    .insert(value);
                ReturnValue::Ok.into()
            }
            BloomOps::BContains(bloom_key, value) => read_blooms!(state, &bloom_key)
                .map(|bloom| if bloom.contains(value) { 1 } else { 0 })
                .unwrap_or(0)
                .into(),
        }
    }
}
#[cfg(test)]
mod test_bloom {
    use crate::bloom::BloomOps;
    use crate::types::{InteractionRes, Key, ReturnValue, State, Value};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_insert(key: Key, v: Value) {
            let eng = State::default();
            let res = eng.clone().exec_op(BloomOps::BInsert(key, v));
            if let InteractionRes::Immediate(e) = res {
                assert_eq!(e, ReturnValue::Ok);
            } else {
                panic!("Should have returned immediate!")
            }
        }
        #[test]
        fn test_contains(key: Key, v: Value) {
            let eng = State::default();
            let res = eng.clone().exec_op(BloomOps::BContains(key.clone(), v.clone()));
            if let InteractionRes::Immediate(e) = res {
                assert_eq!(e, ReturnValue::IntRes(0));
            } else {
                panic!("Should have returned immediate!")
            }
            eng.clone().exec_op(BloomOps::BInsert(key.clone(), v.clone()));
            let res = eng.clone().exec_op(BloomOps::BContains(key, v));
            if let InteractionRes::Immediate(e) = res {
                assert_eq!(e, ReturnValue::IntRes(1));
            } else {
                panic!("Should have returned immediate!")
            }

        }
    }
}
