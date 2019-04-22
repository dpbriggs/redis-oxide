use crate::types::{InteractionRes, Key, State, StateInteration, Value};

#[derive(Debug, Clone)]
pub enum KeyOps {
    // Key Value
    Set(Key, Value),
    Get(Key),
    Del(Vec<Key>),
    Rename(Key, Key),
}

impl StateInteration for KeyOps {
    fn interact(self, engine: State) -> InteractionRes {
        match self {
            KeyOps::Get(key) => engine
                .kv
                .read()
                .unwrap()
                .get(&key)
                .map_or(InteractionRes::Nil, |v| {
                    InteractionRes::StringRes(v.to_vec())
                }),
            KeyOps::Set(key, value) => {
                engine.kv.write().unwrap().insert(key.clone(), value);
                InteractionRes::Ok
            }
            KeyOps::Del(keys) => {
                let deleted = keys
                    .iter()
                    .map(|x| engine.kv.write().unwrap().remove(x))
                    .filter(Option::is_some)
                    .count();
                InteractionRes::UIntRes(deleted)
            }
            KeyOps::Rename(key, new_key) => {
                let mut keys = engine.kv.write().unwrap();
                match keys.remove(&key) {
                    Some(value) => {
                        keys.insert(new_key, value);
                        InteractionRes::Ok
                    }
                    None => InteractionRes::Error(b"no such key"),
                }
            }
        }
    }
}

#[cfg(test)]
mod test_keys {
    use crate::keys::KeyOps;
    use crate::ops::Ops;
    use crate::types::{InteractionRes, State, Value};
    use proptest::prelude::*;

    fn gp(k: KeyOps) -> Ops {
        Ops::Keys(k)
    }

    proptest! {
        #[test]
        fn test_get(v: Value) {
            let eng = State::default();
            assert_eq!(InteractionRes::Nil, eng.clone().interact(gp(KeyOps::Get(v.clone()))));
            eng.clone().interact(gp(KeyOps::Set(v.clone(), v.clone())));
            assert_eq!(InteractionRes::StringRes(v.clone()), eng.interact(gp(KeyOps::Get(v.clone()))));
        }
        #[test]
        fn test_set(l: Value, r: Value) {
            let eng = State::default();
            eng.clone().interact(gp(KeyOps::Set(l.clone(), r.clone())));
            assert_eq!(InteractionRes::StringRes(r.clone()), eng.interact(gp(KeyOps::Get(l.clone()))));
        }
        #[test]
        fn test_del(l: Value, unused: Value) {
            let eng = State::default();
            eng.clone().interact(gp(KeyOps::Set(l.clone(), l.clone())));
            assert_eq!(InteractionRes::UIntRes(1), eng.clone().interact(gp(KeyOps::Del(vec![l.clone()]))));
            assert_eq!(InteractionRes::UIntRes(0), eng.interact(gp(KeyOps::Del(vec![unused]))));
        }
        #[test]
        fn test_rename(old: Value, v: Value, new: Value) {
            let eng = State::default();
            eng.clone().interact(gp(KeyOps::Set(old.clone(), v.clone())));
            assert!(eng.clone().interact(gp(KeyOps::Rename(new.clone(), old.clone()))).is_error());
            eng.clone().interact(gp(KeyOps::Rename(old.clone(), new.clone())));
            assert_eq!(InteractionRes::StringRes(v.clone()), eng.clone().interact(gp(KeyOps::Get(new))));
        }
    }
}
