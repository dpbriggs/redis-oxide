use crate::types::{Count, InteractionRes, Key, ReturnValue, State, StateInteration, Value};

// use futures::future::Future;
// use futures::future::IntoFuture;
// use std::time::{Duration, Instant};
// use tokio::timer::Delay;

#[derive(Debug, Clone)]
pub enum KeyOps {
    // Key Value
    Set(Key, Value),
    Get(Key),
    Del(Vec<Key>),
    Rename(Key, Key),
}

impl StateInteration for KeyOps {
    fn interact(self, state: State) -> InteractionRes {
        match self {
            KeyOps::Get(key) => state
                .kv
                .read()
                .get(&key)
                .map_or(ReturnValue::Nil.into(), |v| {
                    ReturnValue::StringRes(v.to_vec()).into()
                }),
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
        }
    }
}

#[cfg(test)]
mod test_keys {
    use crate::keys::KeyOps;
    use crate::ops::Ops;
    use crate::types::{InteractionRes, ReturnValue, State, Value};
    use proptest::prelude::*;

    fn gp(k: KeyOps) -> Ops {
        Ops::Keys(k)
    }

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
            let eng = State::default();
            assert_eq(ir(ReturnValue::Nil), eng.clone().interact(gp(KeyOps::Get(v.clone()))));
            eng.clone().interact(gp(KeyOps::Set(v.clone(), v.clone())));
            assert_eq(ir(ReturnValue::StringRes(v.clone())), eng.interact(gp(KeyOps::Get(v.clone()))));
        }
        #[test]
        fn test_set(l: Value, r: Value) {
            let eng = State::default();
            eng.clone().interact(gp(KeyOps::Set(l.clone(), r.clone())));
            assert_eq(ir(ReturnValue::StringRes(r.clone())), eng.interact(gp(KeyOps::Get(l.clone()))));
        }
        #[test]
        fn test_del(l: Value, unused: Value) {
            let eng = State::default();
            eng.clone().interact(gp(KeyOps::Set(l.clone(), l.clone())));
            assert_eq(ir(ReturnValue::IntRes(1)), eng.clone().interact(gp(KeyOps::Del(vec![l.clone()]))));
            assert_eq(ir(ReturnValue::IntRes(0)), eng.interact(gp(KeyOps::Del(vec![unused]))));
        }
        #[test]
        fn test_rename(old: Value, v: Value, new: Value) {
            let eng = State::default();
            eng.clone().interact(gp(KeyOps::Set(old.clone(), v.clone())));
            // TODO: Make testing InteractionRes tractable
            // assert(ir(eng.clone().interact(gp(KeyOps::Rename(new.clone()), old.clone()))).is_error());
            eng.clone().interact(gp(KeyOps::Rename(old.clone(), new.clone())));
            assert_eq(ir(ReturnValue::StringRes(v.clone())), eng.clone().interact(gp(KeyOps::Get(new))));
        }
    }
}
