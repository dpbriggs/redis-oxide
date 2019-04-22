use crate::types::{InteractionRes, Key, State, StateInteration, Value};

#[derive(Debug, Clone)]
pub enum HashOps {
    // Hash ops
    HGet(Key, Key),
    HSet(Key, Key, Value),
    HExists(Key, Key),
    HGetAll(Key),
    HMGet(Key, Vec<Key>),
}

macro_rules! read_hashes {
    ($state:expr) => {
        $state.hashes.read().unwrap()
    };
    ($state:expr, $key:expr) => {
        $state.hashes.read().unwrap().get($key)
    };
    ($state:expr, $key:expr, $var_name:ident) => {
        let __temp_name = $state.hashes.read().unwrap();
        let $var_name = __temp_name.get($key);
    };
}

macro_rules! write_hashes {
    ($state:expr) => {
        $state.hashes.write().unwrap()
    };
    ($state:expr, $key:expr) => {
        $state.hashes.write().unwrap().get($key)
    };
    ($state: expr, $key:expr, $var_name:ident) => {
        let mut __temp_name = $state.hashes.write().unwrap();
        let $var_name = __temp_name.get_mut($key).unwrap();
    };
}

impl StateInteration for HashOps {
    fn interact(self, state: State) -> InteractionRes {
        match self {
            HashOps::HGet(key, field) => read_hashes!(state)
                .get(&key)
                .and_then(|hashes| hashes.get(&field))
                .map_or(InteractionRes::Nil, |v| {
                    InteractionRes::StringRes(v.clone())
                }),
            HashOps::HSet(key, field, value) => {
                state.create_hashes_if_necessary(&key);
                write_hashes!(state, &key, hash);
                hash.insert(field, value);
                InteractionRes::Ok
            }
            HashOps::HExists(key, field) => read_hashes!(state)
                .get(&key)
                .map(|hashes| hashes.contains_key(&field))
                .map_or(InteractionRes::IntRes(0), |v: bool| {
                    InteractionRes::IntRes(if v { 1 } else { 0 })
                }),
            HashOps::HGetAll(key) => {
                read_hashes!(state, &key, hash);
                if hash.is_none() {
                    return InteractionRes::MultiStringRes(vec![]);
                }
                let mut ret = Vec::new();
                for (key, val) in hash.unwrap().iter() {
                    ret.push(key.clone());
                    ret.push(val.clone());
                }
                InteractionRes::MultiStringRes(ret)
            }
            HashOps::HMGet(key, fields) => match read_hashes!(state, &key) {
                None => InteractionRes::Array(
                    std::iter::repeat_with(|| InteractionRes::Nil)
                        .take(fields.len())
                        .collect(),
                ),
                Some(hash) => InteractionRes::Array(
                    fields
                        .iter()
                        .map(|field| {
                            hash.get(field).map_or(InteractionRes::Nil, |v| {
                                InteractionRes::StringRes(v.clone())
                            })
                        })
                        .collect(),
                ),
            },
        }
    }
}
