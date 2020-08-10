use crate::op_variants;
use crate::types::{Count, Key, ReturnValue, StateRef, Value};
use crate::{make_reader, make_writer};
use smallvec::SmallVec;
use std::collections::hash_map::Entry;

op_variants! {
    HashOps,
    HGet(Key, Key),
    HSet(Key, Key, Value),
    HExists(Key, Key),
    HGetAll(Key),
    HMGet(Key, Vec<Key>),
    HKeys(Key),
    HMSet(Key, Vec<(Key, Value)>),
    HIncrBy(Key, Key, Count),
    HLen(Key),
    HDel(Key, SmallVec<[Key; 4]>),
    HVals(Key),
    HStrLen(Key, Key),
    HSetNX(Key, Key, Value)
}

make_reader!(hashes, read_hashes);
make_writer!(hashes, write_hashes);

pub async fn hash_interact(hash_op: HashOps, state: StateRef) -> ReturnValue {
    match hash_op {
        HashOps::HGet(key, field) => match read_hashes!(state, &key) {
            None => ReturnValue::Nil,
            Some(hash) => hash
                .get(&field)
                .map_or(ReturnValue::Nil, |f| ReturnValue::StringRes(f.clone())),
        },
        HashOps::HSet(key, field, value) => {
            state.hashes.entry(key).or_default().insert(field, value);
            ReturnValue::Ok
        }
        HashOps::HExists(key, field) => read_hashes!(state)
            .get(&key)
            .map(|hashes| hashes.contains_key(&field))
            .map_or(ReturnValue::IntRes(0), |v: bool| {
                ReturnValue::IntRes(if v { 1 } else { 0 })
            }),

        HashOps::HGetAll(key) => match read_hashes!(state, &key) {
            Some(hash) => {
                let mut ret = Vec::with_capacity(hash.len() * 2);
                for (key, value) in hash.iter() {
                    ret.push(key.clone());
                    ret.push(value.clone());
                }
                ReturnValue::MultiStringRes(ret)
            }
            None => ReturnValue::MultiStringRes(Vec::with_capacity(0)),
        },
        // HashOps::HGetAll(key) => {
        //     read_hashes!(state, &key, hash);
        //     if hash.is_none() {
        //         return ;
        //     }
        //     let mut ret = Vec::new();
        //     for (key, val) in hash.unwrap().iter() {
        //         ret.push(key.clone());
        //         ret.push(val.clone());
        //     }
        //     ReturnValue::MultiStringRes(ret)
        // }
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
            state.hashes.entry(key).or_default().extend(key_values);
            ReturnValue::Ok
        }
        HashOps::HIncrBy(key, field, count) => {
            let mut hash = state.hashes.entry(key).or_default();
            let mut curr_value = match hash.get(&field) {
                Some(value) => {
                    let i64_repr = std::str::from_utf8(value)
                        .map(|e| e.parse::<i64>())
                        .unwrap();
                    if i64_repr.is_err() {
                        return ReturnValue::Error(b"Bad Type!");
                    }
                    i64_repr.unwrap()
                }
                None => 0,
            };
            curr_value += count;
            let new_value = Value::from(curr_value.to_string());
            hash.insert(field, new_value);
            ReturnValue::Ok
        }
        HashOps::HLen(key) => read_hashes!(state, &key)
            .map_or(0, |hash| hash.len() as Count)
            .into(),

        // HashOps::HLen(key) => read_hashes!(state, &key)
        //     .map(|hash| hash.len() as Count)
        //     .unwrap_or(0)
        //     .into(),
        // HashOps::HLen(key) => match read_hashes!(state, &key) {
        //     Some(hash) => ReturnValue::IntRes(hash.len() as Count),
        //     None => ReturnValue::IntRes(0),
        // },
        HashOps::HDel(key, fields) => match write_hashes!(state, &key) {
            Some(mut hash) => {
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
        // XXX: For some reason there's lifetime issues when doing the usual combinator chain.
        HashOps::HStrLen(key, field) => match read_hashes!(state, &key) {
            None => ReturnValue::Nil,
            Some(hash) => hash
                .get(&field)
                .map_or(ReturnValue::Nil, |f| ReturnValue::IntRes(f.len() as Count)),
        },
        HashOps::HSetNX(key, field, value) => {
            if let Entry::Vacant(ent) = state.hashes.entry(key).or_default().entry(field) {
                ent.insert(value);
                ReturnValue::IntRes(1)
            } else {
                ReturnValue::IntRes(0)
            }
        }
    }
}
