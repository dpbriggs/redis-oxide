use crate::op_variants;
use crate::ops::RVec;
use crate::types::{Count, Key, ReturnValue, StateRef, Value};
use std::collections::HashSet;

op_variants! {
    SetOps,
    SAdd(Key, RVec<Value>),
    SCard(Key),
    SDiff(RVec<Value>),
    SDiffStore(Key, RVec<Value>),
    SInter(RVec<Value>),
    SInterStore(Key, RVec<Value>),
    SIsMember(Key, Value),
    SMembers(Key),
    SMove(Key, Key, Value),
    SPop(Key, Option<Count>),
    SRandMembers(Key, Option<Count>),
    SRem(Key, RVec<Value>),
    SUnion(RVec<Value>),
    SUnionStore(Key, RVec<Value>)
}

pub enum SetAction {
    Diff,
    Union,
    Inter,
}

make_reader!(sets, read_sets);
make_writer!(sets, write_sets);

fn many_set_op(state: &StateRef, keys: RVec<Key>, op: SetAction) -> Option<HashSet<Value>> {
    let sets_that_exist: Vec<_> = keys
        .iter()
        .filter(|&k| state.sets.contains_key(k))
        .collect();
    if sets_that_exist.is_empty() {
        return None;
    }
    #[allow(clippy::mutable_key_type)]
    let mut head: HashSet<Key> = state
        .sets
        .get_mut(sets_that_exist[0])
        .unwrap()
        .value_mut()
        .clone();
    // TODO: Make this _way_ cleaner.
    for set_key in sets_that_exist.into_iter().skip(1) {
        head = match op {
            SetAction::Diff => head
                .difference(state.sets.get(set_key).unwrap().value())
                .cloned()
                .collect(),
            SetAction::Union => head
                .union(state.sets.get(set_key).unwrap().value())
                .cloned()
                .collect(),
            SetAction::Inter => head
                .intersection(state.sets.get(set_key).unwrap().value())
                .cloned()
                .collect(),
        }
    }
    Some(head)
}

pub async fn set_interact(set_op: SetOps, state: StateRef) -> ReturnValue {
    match set_op {
        SetOps::SAdd(set_key, vals) => {
            let mut set = state.sets.entry(set_key).or_default();
            vals.into_iter()
                .fold(0, |acc, val| acc + set.insert(val) as Count)
                .into()
        }
        SetOps::SMembers(set_key) => read_sets!(state, &set_key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_else(RVec::new)
            .into(),
        SetOps::SCard(set_key) => read_sets!(state, &set_key)
            .map(|set| set.len() as Count)
            .unwrap_or(0)
            .into(),
        SetOps::SRem(set_key, vals) => write_sets!(state, &set_key)
            .map(|mut set| {
                vals.into_iter()
                    .fold(0, |acc, val| acc + set.insert(val) as Count)
            })
            .unwrap_or(0)
            .into(),
        SetOps::SDiff(keys) => many_set_op(&state, keys, SetAction::Diff)
            .map(|set| set.into_iter().collect())
            .unwrap_or_else(RVec::new)
            .into(),
        SetOps::SUnion(keys) => many_set_op(&state, keys, SetAction::Union)
            .map(|set| set.into_iter().collect())
            .unwrap_or_else(RVec::new)
            .into(),
        SetOps::SInter(keys) => many_set_op(&state, keys, SetAction::Inter)
            .map(|set| set.into_iter().collect())
            .unwrap_or_else(RVec::new)
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
            let mut set = match state.sets.get_mut(&key) {
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

            // TODO: Why are we allowed to unwrap here? It may not be alive at this time.
            let mut src_set = state.sets.get_mut(&src).unwrap();
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
