use crate::types::{Count, InteractionRes, Key, ReturnValue, State, StateInteration, Value};

#[derive(Debug, Clone)]
pub enum HashOps {
    // Hash ops
    HGet(Key, Key),
    HSet(Key, Key, Value),
    HExists(Key, Key),
    HGetAll(Key),
    HMGet(Key, Vec<Key>),
    HKeys(Key),
    HMSet(Key, Vec<(Key, Value)>),
    // HIncrBy(Key, Key, Count),
    HLen(Key),
    HDel(Key, Vec<Key>),
    HVals(Key),
}

macro_rules! read_hashes {
    ($state:expr) => {
        $state.hashes.read()
    };
    ($state:expr, $key:expr) => {
        $state.hashes.read().get($key)
    };
    ($state:expr, $key:expr, $var_name:ident) => {
        let __temp_name = $state.hashes.read();
        let $var_name = __temp_name.get($key);
    };
}

macro_rules! write_hashes {
    ($state:expr) => {
        $state.hashes.write()
    };
    ($state:expr, $key:expr) => {
        $state.hashes.write().get_mut($key)
    };
    ($state: expr, $key:expr, $var_name:ident) => {
        let mut __temp_name = $state.hashes.write();
        let $var_name = __temp_name.get_mut($key);
    };
}

impl StateInteration for HashOps {
    fn interact(self, state: State) -> InteractionRes {
        match self {
            HashOps::HGet(key, field) => read_hashes!(state)
                .get(&key)
                .and_then(|hashes| hashes.get(&field))
                .map_or(ReturnValue::Nil, |v| ReturnValue::StringRes(v.clone())),
            HashOps::HSet(key, field, value) => {
                state.create_hashes_if_necessary(&key);
                write_hashes!(state, &key, hash);
                hash.unwrap().insert(field, value);
                ReturnValue::Ok
            }
            HashOps::HExists(key, field) => read_hashes!(state)
                .get(&key)
                .map(|hashes| hashes.contains_key(&field))
                .map_or(ReturnValue::IntRes(0), |v: bool| {
                    ReturnValue::IntRes(if v { 1 } else { 0 })
                }),
            HashOps::HGetAll(key) => {
                read_hashes!(state, &key, hash);
                if hash.is_none() {
                    return ReturnValue::MultiStringRes(vec![]).into();
                }
                let mut ret = Vec::new();
                for (key, val) in hash.unwrap().iter() {
                    ret.push(key.clone());
                    ret.push(val.clone());
                }
                ReturnValue::MultiStringRes(ret)
            }
            HashOps::HMGet(key, fields) => ReturnValue::Array(match read_hashes!(state, &key) {
                None => std::iter::repeat_with(|| ReturnValue::Nil)
                    .take(fields.len())
                    .collect(),
                Some(hash) => fields
                    .iter()
                    .map(|field| {
                        hash.get(field)
                            .map_or(ReturnValue::Nil, |v| ReturnValue::StringRes(v.clone()))
                    })
                    .collect(),
            }),
            HashOps::HKeys(key) => match read_hashes!(state, &key) {
                Some(hash) => {
                    ReturnValue::Array(hash.keys().cloned().map(ReturnValue::StringRes).collect())
                }
                None => ReturnValue::Array(vec![]),
            },
            HashOps::HMSet(key, key_values) => {
                state.create_hashes_if_necessary(&key);
                write_hashes!(state, &key, hash);
                hash.unwrap().extend(key_values);
                ReturnValue::Ok
            } // HashOps::HIncrBy(key, field, count) => {
            //     // TODO
            //     // state.create_hashes_if_necessary(&key);
            //     // write_hashes!(state, &key, hashes);
            //     // let hashes = hashes.unwrap();
            //     // match hashes.get(&field) {
            //     //     Some(hash) => hash,
            //     //     None => 0
            //     // }
            //     ReturnValue::Ok
            // }
            HashOps::HLen(key) => match read_hashes!(state, &key) {
                Some(hash) => ReturnValue::IntRes(hash.len() as Count),
                None => ReturnValue::IntRes(0),
            },
            HashOps::HDel(key, fields) => match write_hashes!(state, &key) {
                Some(hash) => {
                    let res = fields.iter().filter_map(|field| hash.remove(field)).count();
                    ReturnValue::IntRes(res as Count)
                }
                None => ReturnValue::IntRes(0),
            },
            HashOps::HVals(key) => match read_hashes!(state, &key) {
                Some(hash) => {
                    ReturnValue::Array(hash.values().cloned().map(ReturnValue::StringRes).collect())
                }
                None => ReturnValue::Array(vec![]),
            },
        }
        .into()
    }
}
