use crate::logger::LOGGER;
use crate::op_variants;
use crate::types::{Count, Key, ReturnValue, StateRef, Value};

op_variants! {
    KeyOps,
    Set(Key, Value),
    MSet(Vec<(Key, Value)>),
    Get(Key),
    MGet(Vec<Key>),
    Del(Vec<Key>),
    Rename(Key, Key),
    RenameNx(Key, Key),
    Test(Key)
}

pub async fn key_interact(key_op: KeyOps, state: StateRef) -> ReturnValue {
    match key_op {
        KeyOps::Get(key) => state
            .kv
            .get(&key)
            .map_or(ReturnValue::Nil, |v| ReturnValue::StringRes(v.to_vec())),
        KeyOps::MGet(keys) => {
            let vals = keys
                .iter()
                .map(|key| match state.kv.get(key) {
                    Some(v) => ReturnValue::StringRes(v.to_vec()),
                    None => ReturnValue::Nil,
                })
                .collect();
            ReturnValue::Array(vals)
        }
        KeyOps::Set(key, value) => {
            state.kv.insert(key, value);
            ReturnValue::Ok
        }
        KeyOps::MSet(key_vals) => {
            let kv = &state.kv;
            for (key, val) in key_vals.into_iter() {
                kv.insert(key, val);
            }
            ReturnValue::Ok
        }
        KeyOps::Del(keys) => {
            let deleted = keys
                .iter()
                .map(|x| state.kv.remove(x))
                .filter(Option::is_some)
                .count();
            ReturnValue::IntRes(deleted as Count)
        }
        KeyOps::Rename(key, new_key) => match state.kv.remove(&key) {
            Some((_, value)) => {
                state.kv.insert(new_key, value);
                ReturnValue::Ok
            }
            None => ReturnValue::Error(b"no such key"),
        },
        KeyOps::RenameNx(key, new_key) => {
            if state.kv.contains_key(&new_key) {
                return ReturnValue::IntRes(0);
            }
            match state.kv.remove(&key) {
                Some((_, value)) => {
                    state.kv.insert(new_key, value);
                    ReturnValue::IntRes(1)
                }
                None => ReturnValue::Error(b"no such key"),
            }
        }
        KeyOps::Test(_key) => {
            // let value: Value = state
            //     .kv
            //     .get(&key)
            //     .map(|r| r.value().clone())
            //     .unwrap_or_else(|| b"hi".to_vec());
            // info!(
            //     LOGGER,
            //     "{} = {}",
            //     String::from_utf8_lossy(&key),
            //     String::from_utf8_lossy(&value)
            // );
            ReturnValue::Ok
        }
    }
}

#[cfg(test)]
mod test_keys {
    use crate::keys::{key_interact, KeyOps};
    use crate::types::{ReturnValue, State};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_get() {
        let v = b"hello".to_vec();
        let eng = Arc::new(State::default());
        assert_eq!(
            ReturnValue::Nil,
            key_interact(KeyOps::Get(v.clone()), eng.clone()).await
        );
        key_interact(KeyOps::Set(v.clone(), v.clone()), eng.clone()).await;
        assert_eq!(
            ReturnValue::StringRes(v.clone()),
            key_interact(KeyOps::Get(v.clone()), eng.clone()).await
        );
    }

    #[tokio::test]
    async fn test_set() {
        let (l, r) = (b"l".to_vec(), b"r".to_vec());
        let eng = Arc::new(State::default());
        key_interact(KeyOps::Set(l.clone(), r.clone()), eng.clone()).await;
        assert_eq!(
            ReturnValue::StringRes(r.clone()),
            key_interact(KeyOps::Get(l.clone()), eng.clone()).await
        );
    }

    #[tokio::test]
    async fn test_del() {
        let (l, unused) = (b"l".to_vec(), b"unused".to_vec());
        let eng = Arc::new(State::default());
        key_interact(KeyOps::Set(l.clone(), l.clone()), eng.clone()).await;

        assert_eq!(
            ReturnValue::IntRes(1),
            key_interact(KeyOps::Del(vec![l.clone()]), eng.clone()).await
        );
        assert_eq!(
            ReturnValue::IntRes(0),
            key_interact(KeyOps::Del(vec![unused]), eng.clone()).await
        );
    }

    #[tokio::test]
    async fn test_rename() {
        let (old, v, new) = (b"old".to_vec(), b"v".to_vec(), b"new".to_vec());
        let eng = Arc::new(State::default());
        key_interact(KeyOps::Set(old.clone(), v.clone()), eng.clone()).await;
        // TODO: Make testing Exec_OpionRes tractable
        // assert(ir(eng.clone().exec_op(gp(KeyOps::Rename(new.clone()), old.clone()))).is_error());
        key_interact(KeyOps::Rename(old.clone(), new.clone()), eng.clone()).await;
        assert_eq!(
            ReturnValue::StringRes(v.clone()),
            key_interact(KeyOps::Get(new), eng.clone()).await
        );
    }
}
