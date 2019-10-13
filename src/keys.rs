use crate::types::{Count, InteractionRes, Key, ReturnValue, StateInteration, StateRef, Value};

// use futures::future::Future;
// use futures::future::IntoFuture;
// use std::time::{Duration, Instant};
// use tokio::timer::Delay;

#[derive(Debug, Clone)]
pub enum KeyOps {
    // Key Value
    Set(Key, Value),
    MSet(Vec<(Key, Value)>),
    Get(Key),
    MGet(Vec<Key>),
    Del(Vec<Key>),
    Rename(Key, Key),
    RenameNx(Key, Key),
    Test(Key),
}

impl StateInteration for KeyOps {
    fn interact(self, state: StateRef) -> InteractionRes {
        match self {
            KeyOps::Get(key) => state
                .kv
                .read()
                .get(&key)
                .map_or(ReturnValue::Nil.into(), |v| {
                    ReturnValue::StringRes(v.to_vec()).into()
                }),
            KeyOps::MGet(keys) => {
                let kv = state.kv.read();
                let vals = keys
                    .iter()
                    .map(|key| match kv.get(key) {
                        Some(v) => ReturnValue::StringRes(v.to_vec()),
                        None => ReturnValue::Nil,
                    })
                    .collect();
                ReturnValue::Array(vals).into()
            }
            KeyOps::Set(key, value) => {
                state.kv.write().insert(key.clone(), value);
                // let state_ptr = state.clone();
                // TODO: Parse ttl information
                // let primative_ttl = Delay::new(Instant::now() + Duration::from_millis(3000))
                //     .and_then(move |_| {
                //         KeyOps::Del(vec![key]).interact(state_ptr);
                //         // state_ptr().exec(Ops::Del(vec![key]));
                //         Ok(())
                //     })
                //     .map_err(|e| panic!("delay errored; err={:?}", e));
                // await!(primative_ttl.into_future());
                // InteractionRes::ImmediateWithWork(ReturnValue::Ok.into(), Box::new(primative_ttl))
                // ReturnValue::Ok
                ReturnValue::Ok.into()
            }
            KeyOps::MSet(key_vals) => {
                let mut kv = state.kv.write();
                for (key, val) in key_vals.into_iter() {
                    kv.insert(key, val);
                }
                ReturnValue::Ok.into()
            }
            KeyOps::Del(keys) => {
                let deleted = keys
                    .iter()
                    .map(|x| state.kv.write().remove(x))
                    .filter(Option::is_some)
                    .count();
                ReturnValue::IntRes(deleted as Count).into()
            }
            KeyOps::Rename(key, new_key) => {
                let mut keys = state.kv.write();
                match keys.remove(&key) {
                    Some(value) => {
                        keys.insert(new_key, value);
                        ReturnValue::Ok.into()
                    }
                    None => ReturnValue::Error(b"no such key").into(),
                }
            }
            KeyOps::RenameNx(key, new_key) => {
                let mut keys = state.kv.write();
                if keys.contains_key(&new_key) {
                    return ReturnValue::IntRes(0).into();
                }
                match keys.remove(&key) {
                    Some(value) => {
                        keys.insert(new_key, value);
                        ReturnValue::IntRes(1).into()
                    }
                    None => ReturnValue::Error(b"no such key").into(),
                }
            }
            KeyOps::Test(key) => {
                println!("{}", String::from_utf8_lossy(&key));
                ReturnValue::Ok.into()
            }
        }
    }
}

#[cfg(test)]
mod test_keys {
    use crate::keys::KeyOps;
    use crate::types::StateInteration;
    use crate::types::{InteractionRes, ReturnValue, State, Value};
    use proptest::prelude::*;
    use std::sync::Arc;

    fn ir(k: ReturnValue) -> InteractionRes {
        InteractionRes::Immediate(k)
    }

    fn assert_eq(left: InteractionRes, right: InteractionRes) {
        let left = match left {
            InteractionRes::Immediate(e) => e,
            _ => panic!("Cannot compare futures!"),
        };
        let right = match right {
            InteractionRes::Immediate(e) => e,
            _ => panic!("Cannot compare futures!"),
        };
        assert_eq!(left, right)
    }

    proptest! {
        #[test]
        fn test_get(v: Value) {
            let eng = Arc::new(State::default());
            assert_eq(ir(ReturnValue::Nil), KeyOps::Get(v.clone()).interact(eng.clone()));
            KeyOps::Set(v.clone(), v.clone()).interact(eng.clone());
            assert_eq(ir(ReturnValue::StringRes(v.clone())), KeyOps::Get(v.clone()).interact(eng.clone()));
        }
        #[test]
        fn test_set(l: Value, r: Value) {
            let eng = Arc::new(State::default());
            KeyOps::Set(l.clone(), r.clone()).interact(eng.clone());
            assert_eq(ir(ReturnValue::StringRes(r.clone())), KeyOps::Get(l.clone()).interact(eng.clone()));
        }
        #[test]
        fn test_del(l: Value, unused: Value) {
            let eng = Arc::new(State::default());
            KeyOps::Set(l.clone(), l.clone()).interact(eng.clone());

            assert_eq(ir(ReturnValue::IntRes(1)), KeyOps::Del(vec![l.clone()]).interact(eng.clone()));
            assert_eq(ir(ReturnValue::IntRes(0)), KeyOps::Del(vec![unused]).interact(eng.clone()));
        }
        #[test]
        fn test_rename(old: Value, v: Value, new: Value) {
            let eng = Arc::new(State::default());
            KeyOps::Set(old.clone(), v.clone()).interact(eng.clone());
            // TODO: Make testing Exec_OpionRes tractable
            // assert(ir(eng.clone().exec_op(gp(KeyOps::Rename(new.clone()), old.clone()))).is_error());
            KeyOps::Rename(old.clone(), new.clone()).interact(eng.clone());
            assert_eq(ir(ReturnValue::StringRes(v.clone())), KeyOps::Get(new).interact(eng.clone()));
        }
    }
}
