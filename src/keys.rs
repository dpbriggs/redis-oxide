use crate::types::{Key, State, UpdateRes, UpdateState, Value};

#[derive(Debug, Clone)]
pub enum KeyOps {
    // Key Value
    Set(Key, Value),
    Get(Key),
    Del(Vec<Key>),
    Rename(Key, Key),
}

impl UpdateState for KeyOps {
    fn update(self, engine: State) -> UpdateRes {
        match self {
            KeyOps::Get(key) => engine
                .kv
                .read()
                .unwrap()
                .get(&key)
                .map_or(UpdateRes::Nil, |v| UpdateRes::StringRes(v.to_vec())),
            KeyOps::Set(key, value) => {
                engine.kv.write().unwrap().insert(key.clone(), value);
                UpdateRes::Ok
            }
            KeyOps::Del(keys) => {
                let deleted = keys
                    .iter()
                    .map(|x| engine.kv.write().unwrap().remove(x))
                    .filter(Option::is_some)
                    .count();
                UpdateRes::UIntRes(deleted)
            }
            KeyOps::Rename(key, new_key) => {
                let mut keys = engine.kv.write().unwrap();
                match keys.remove(&key) {
                    Some(value) => {
                        keys.insert(new_key, value);
                        UpdateRes::Ok
                    }
                    None => UpdateRes::Error(b"no such key"),
                }
            }
        }
    }
}

#[cfg(test)]
mod test_keys {
    use crate::keys::KeyOps;
    use crate::ops::Ops;
    use crate::types::{State, UpdateRes, Value};
    use proptest::prelude::*;

    fn gp(k: KeyOps) -> Ops {
        Ops::Keys(k)
    }

    proptest! {
        #[test]
        fn test_get(v: Value) {
            let eng = State::default();
            assert_eq!(UpdateRes::Nil, eng.clone().update_state(gp(KeyOps::Get(v.clone()))));
            eng.clone().update_state(gp(KeyOps::Set(v.clone(), v.clone())));
            assert_eq!(UpdateRes::StringRes(v.clone()), eng.update_state(gp(KeyOps::Get(v.clone()))));
        }
        #[test]
        fn test_set(l: Value, r: Value) {
            let eng = State::default();
            eng.clone().update_state(gp(KeyOps::Set(l.clone(), r.clone())));
            assert_eq!(UpdateRes::StringRes(r.clone()), eng.update_state(gp(KeyOps::Get(l.clone()))));
        }
        #[test]
        fn test_del(l: Value, unused: Value) {
            let eng = State::default();
            eng.clone().update_state(gp(KeyOps::Set(l.clone(), l.clone())));
            assert_eq!(UpdateRes::UIntRes(1), eng.clone().update_state(gp(KeyOps::Del(vec![l.clone()]))));
            assert_eq!(UpdateRes::UIntRes(0), eng.update_state(gp(KeyOps::Del(vec![unused]))));
        }
        #[test]
        fn test_rename(old: Value, v: Value, new: Value) {
            let eng = State::default();
            eng.clone().update_state(gp(KeyOps::Set(old.clone(), v.clone())));
            assert!(eng.clone().update_state(gp(KeyOps::Rename(new.clone(), old.clone()))).is_error());
            eng.clone().update_state(gp(KeyOps::Rename(old.clone(), new.clone())));
            assert_eq!(UpdateRes::StringRes(v.clone()), eng.clone().update_state(gp(KeyOps::Get(new))));
        }
    }
}
