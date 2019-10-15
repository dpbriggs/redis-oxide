use crate::types::{Count, InteractionRes, Key, ReturnValue, StateRef, Value};

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

pub async fn key_interact(key_op: KeyOps, state: StateRef) -> InteractionRes {
    match key_op {
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

#[cfg(test)]
mod test_keys {
    use crate::keys::{KeyOps, key_interact};
    use crate::types::{InteractionRes, ReturnValue, State};
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

    #[tokio::test]
    async fn test_get() {
        let v = b"hello".to_vec();
        let eng = Arc::new(State::default());
        assert_eq(ir(ReturnValue::Nil), key_interact(KeyOps::Get(v.clone()), eng.clone()).await);
        key_interact(KeyOps::Set(v.clone(), v.clone()), eng.clone()).await;
        assert_eq(ir(ReturnValue::StringRes(v.clone())), key_interact(KeyOps::Get(v.clone()), eng.clone()).await);
    }

    #[tokio::test]
    async fn test_set() {
        let (l, r) = (b"l".to_vec(), b"r".to_vec());
        let eng = Arc::new(State::default());
        key_interact(KeyOps::Set(l.clone(), r.clone()), eng.clone()).await;
        assert_eq(ir(ReturnValue::StringRes(r.clone())), key_interact(KeyOps::Get(l.clone()), eng.clone()).await);
    }

    #[tokio::test]
    async fn test_del() {
        let (l, unused) = (b"l".to_vec(), b"unused".to_vec());
        let eng = Arc::new(State::default());
        key_interact(KeyOps::Set(l.clone(), l.clone()), eng.clone()).await;

        assert_eq(ir(ReturnValue::IntRes(1)), key_interact(KeyOps::Del(vec![l.clone()]), eng.clone()).await);
        assert_eq(ir(ReturnValue::IntRes(0)), key_interact(KeyOps::Del(vec![unused]), eng.clone()).await);
    }

    #[tokio::test]
    async fn test_rename() {
        let (old, v, new) = (b"old".to_vec(), b"v".to_vec(), b"new".to_vec());
        let eng = Arc::new(State::default());
        key_interact(KeyOps::Set(old.clone(), v.clone()), eng.clone()).await;
        // TODO: Make testing Exec_OpionRes tractable
        // assert(ir(eng.clone().exec_op(gp(KeyOps::Rename(new.clone()), old.clone()))).is_error());
        key_interact(KeyOps::Rename(old.clone(), new.clone()), eng.clone()).await;
        assert_eq(ir(ReturnValue::StringRes(v.clone())), key_interact(KeyOps::Get(new), eng.clone()).await);
    }

    mod bench {
        use crate::types::State;
        use std::sync::Arc;
        use test::Bencher;
        use crate::keys::{KeyOps, key_interact};

        #[bench]
        fn set_key(b: &mut Bencher) {
            // use tokio::runtime::Runtime;
            let eng = Arc::new(State::default());
            b.iter(|| async {
                key_interact(KeyOps::Set(b"foo".to_vec(), b"bar".to_vec()), eng.clone()).await
            });
        }
        #[bench]
        fn set_key_large(b: &mut Bencher) {
            let eng = Arc::new(State::default());
            let key: Vec<u8> = "X".repeat(10000).as_bytes().to_vec();
            b.iter(|| async {
                key_interact(KeyOps::Set(b"foo".to_vec(), key.clone()), eng.clone()).await
            });
        }
        #[bench]
        fn get_key(b: &mut Bencher) {
            let eng = Arc::new(State::default());
            b.iter(|| async {
                key_interact(KeyOps::Set(b"foo".to_vec(), b"bar".to_vec()), eng.clone()).await;
                key_interact(KeyOps::Get(b"foo".to_vec()), eng.clone()).await;
            });
        }
    }
}
