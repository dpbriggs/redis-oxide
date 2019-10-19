use crate::types::{Count, Key, ReturnValue, StateRef, Value};
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub enum SetOps {
    // Set Operations
    SAdd(Key, Vec<Value>),
    SCard(Key),
    SDiff(Vec<Value>),
    SDiffStore(Key, Vec<Value>),
    SInter(Vec<Value>),
    SInterStore(Key, Vec<Value>),
    SIsMember(Key, Value),
    SMembers(Key),
    SMove(Key, Key, Value),
    SPop(Key, Option<Count>),
    SRandMembers(Key, Option<Count>),
    SRem(Key, Vec<Value>),
    SUnion(Vec<Value>),
    SUnionStore(Key, Vec<Value>),
}

pub enum SetAction {
    Diff,
    Union,
    Inter,
}

make_reader!(sets, read_sets);
make_writer!(sets, write_sets);

fn many_set_op(state: &StateRef, keys: Vec<Key>, op: SetAction) -> Option<HashSet<Value>> {
    let state_sets = write_sets!(state);
    let sets: Vec<&HashSet<Key>> = keys.iter().filter_map(|key| state_sets.get(key)).collect();
    if sets.is_empty() {
        return None;
    }
    let mut head: HashSet<Key> = (*sets.first().unwrap()).to_owned();
    for set in sets.iter().skip(1).cloned() {
        head = match op {
            SetAction::Diff => head.difference(set).cloned().collect(),
            SetAction::Union => head.union(set).cloned().collect(),
            SetAction::Inter => head.intersection(set).cloned().collect(),
        }
    }
    Some(head)
}

pub async fn set_interact(set_op: SetOps, state: StateRef) -> ReturnValue {
    match set_op {
        SetOps::SAdd(set_key, vals) => {
            let mut set_lock = state.sets.write();
            let set = set_lock.entry(set_key).or_default();
            vals.into_iter()
                .fold(0, |acc, val| acc + set.insert(val) as Count)
                .into()
        }
        SetOps::SMembers(set_key) => read_sets!(state, &set_key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_else(Vec::new)
            .into(),
        SetOps::SCard(set_key) => read_sets!(state, &set_key)
            .map(|set| set.len() as Count)
            .unwrap_or(0)
            .into(),
        SetOps::SRem(set_key, vals) => write_sets!(state, &set_key)
            .map(|set| {
                vals.into_iter()
                    .fold(0, |acc, val| acc + set.insert(val) as Count)
            })
            .unwrap_or(0)
            .into(),
        SetOps::SDiff(keys) => many_set_op(&state, keys, SetAction::Diff)
            .map(|set| set.into_iter().collect())
            .unwrap_or_else(Vec::new)
            .into(),
        SetOps::SUnion(keys) => many_set_op(&state, keys, SetAction::Union)
            .map(|set| set.into_iter().collect())
            .unwrap_or_else(Vec::new)
            .into(),
        SetOps::SInter(keys) => many_set_op(&state, keys, SetAction::Inter)
            .map(|set| set.into_iter().collect())
            .unwrap_or_else(Vec::new)
            .into(),
        SetOps::SDiffStore(to_store, keys) => match many_set_op(&state, keys, SetAction::Diff) {
            Some(hash_set) => {
                let hash_set_size = hash_set.len();
                write_sets!(state).insert(to_store, hash_set);
                ReturnValue::IntRes(hash_set_size as Count)
            }
            None => ReturnValue::IntRes(0),
        },
        SetOps::SUnionStore(to_store, keys) => match many_set_op(&state, keys, SetAction::Union) {
            Some(hash_set) => {
                let hash_set_size = hash_set.len();
                write_sets!(state).insert(to_store, hash_set);
                ReturnValue::IntRes(hash_set_size as Count)
            }
            None => ReturnValue::IntRes(0),
        },
        SetOps::SInterStore(to_store, keys) => match many_set_op(&state, keys, SetAction::Inter) {
            Some(hash_set) => {
                let hash_set_size = hash_set.len();
                write_sets!(state).insert(to_store, hash_set);
                ReturnValue::IntRes(hash_set_size as Count)
            }
            None => ReturnValue::IntRes(0),
        },
        // There's some surprising complexity behind this command
        SetOps::SPop(key, count) => {
            let mut sets = write_sets!(state);
            let set = match sets.get_mut(&key) {
                Some(s) => s,
                None => return ReturnValue::Nil,
            };
            if set.is_empty() && count.is_some() {
                return ReturnValue::MultiStringRes(vec![]);
            } else if set.is_empty() {
                return ReturnValue::Nil;
            }
            let count = count.unwrap_or(1);
            if count < 0 {
                return ReturnValue::Error(b"Count cannot be less than 0!");
            }
            let eles: Vec<Value> = set.iter().take(count as usize).cloned().collect();
            for ele in eles.iter() {
                set.remove(ele);
            }
            ReturnValue::MultiStringRes(eles)
        }
        SetOps::SIsMember(key, member) => match read_sets!(state, &key) {
            Some(set) => match set.get(&member) {
                Some(_) => ReturnValue::IntRes(1),
                None => ReturnValue::IntRes(0),
            },
            None => ReturnValue::IntRes(0),
        },
        SetOps::SMove(src, dest, member) => {
            let sets = read_sets!(state);
            if !sets.contains_key(&src) || !sets.contains_key(&dest) {
                return ReturnValue::IntRes(0);
            }

            let mut sets = write_sets!(state);
            let src_set = sets.get_mut(&src).unwrap();
            match src_set.take(&member) {
                Some(res) => {
                    sets.get_mut(&dest).unwrap().insert(res);
                    ReturnValue::IntRes(1)
                }
                None => ReturnValue::IntRes(0),
            }
        }
        SetOps::SRandMembers(key, count) => match read_sets!(state, &key) {
            Some(set) => {
                let count = count.unwrap_or(1);
                if count < 0 {
                    return ReturnValue::MultiStringRes(
                        set.iter().cycle().take(-count as usize).cloned().collect(),
                    );
                };
                ReturnValue::MultiStringRes(set.iter().take(count as usize).cloned().collect())
            }
            None => ReturnValue::Nil,
        },
    }
}
