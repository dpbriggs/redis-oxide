use crate::types::{Count, InteractionRes, Key, State, StateInteration, Value};
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

fn many_set_op(engine: &State, keys: Vec<Key>, op: SetAction) -> Option<HashSet<Value>> {
    let engine_sets = engine.sets.read().unwrap();
    let sets: Vec<HashSet<Key>> = keys
        .iter()
        .filter_map(|key| engine_sets.get(key))
        .cloned()
        .collect();
    if sets.is_empty() {
        return None;
    }
    let mut head: HashSet<Key> = (*sets.first().unwrap()).to_owned();
    for set in sets.iter().skip(1).cloned() {
        head = match op {
            SetAction::Diff => head.difference(&set).cloned().collect(),
            SetAction::Union => head.union(&set).cloned().collect(),
            SetAction::Inter => head.intersection(&set).cloned().collect(),
        }
    }
    Some(head)
}

impl StateInteration for SetOps {
    fn interact(self, engine: State) -> InteractionRes {
        match self {
            SetOps::SAdd(set_key, vals) => {
                engine.create_set_if_necessary(&set_key);
                let mut sets = engine.sets.write().unwrap();
                let set = sets.get_mut(&set_key).unwrap();

                let mut vals_inserted = 0;
                for val in vals {
                    if set.insert(val) {
                        vals_inserted += 1;
                    }
                }
                InteractionRes::UIntRes(vals_inserted)
            }
            SetOps::SMembers(set_key) => match engine.sets.read().unwrap().get(&set_key) {
                Some(hs) => InteractionRes::MultiStringRes(hs.iter().cloned().collect()),
                None => InteractionRes::MultiStringRes(vec![]),
            },
            SetOps::SCard(set_key) => match engine.sets.read().unwrap().get(&set_key) {
                Some(hs) => InteractionRes::UIntRes(hs.len()),
                None => InteractionRes::UIntRes(0),
            },
            SetOps::SRem(set_key, vals) => match engine.sets.write().unwrap().get_mut(&set_key) {
                Some(hs) => {
                    let mut vals_removed = 0;
                    for val in vals {
                        if hs.remove(&val) {
                            vals_removed += 1;
                        }
                    }
                    InteractionRes::UIntRes(vals_removed)
                }
                None => InteractionRes::UIntRes(0),
            },
            SetOps::SDiff(keys) => match many_set_op(&engine, keys, SetAction::Diff) {
                Some(hash_set) => {
                    InteractionRes::MultiStringRes(hash_set.iter().cloned().collect())
                }
                None => InteractionRes::MultiStringRes(vec![]),
            },
            SetOps::SUnion(keys) => match many_set_op(&engine, keys, SetAction::Union) {
                Some(hash_set) => {
                    InteractionRes::MultiStringRes(hash_set.iter().cloned().collect())
                }
                None => InteractionRes::MultiStringRes(vec![]),
            },
            SetOps::SInter(keys) => match many_set_op(&engine, keys, SetAction::Inter) {
                Some(hash_set) => {
                    InteractionRes::MultiStringRes(hash_set.iter().cloned().collect())
                }
                None => InteractionRes::MultiStringRes(vec![]),
            },
            SetOps::SDiffStore(to_store, keys) => match many_set_op(&engine, keys, SetAction::Diff)
            {
                Some(hash_set) => {
                    let hash_set_size = hash_set.len();
                    engine.sets.write().unwrap().insert(to_store, hash_set);
                    InteractionRes::UIntRes(hash_set_size)
                }
                None => InteractionRes::UIntRes(0),
            },
            SetOps::SUnionStore(to_store, keys) => {
                match many_set_op(&engine, keys, SetAction::Union) {
                    Some(hash_set) => {
                        let hash_set_size = hash_set.len();
                        engine.sets.write().unwrap().insert(to_store, hash_set);
                        InteractionRes::UIntRes(hash_set_size)
                    }
                    None => InteractionRes::UIntRes(0),
                }
            }
            SetOps::SInterStore(to_store, keys) => {
                match many_set_op(&engine, keys, SetAction::Inter) {
                    Some(hash_set) => {
                        let hash_set_size = hash_set.len();
                        engine.sets.write().unwrap().insert(to_store, hash_set);
                        InteractionRes::UIntRes(hash_set_size)
                    }
                    None => InteractionRes::UIntRes(0),
                }
            }
            // There's some surprising complexity behind this command
            SetOps::SPop(key, count) => {
                let mut sets = engine.sets.write().unwrap();
                let set = match sets.get_mut(&key) {
                    Some(s) => s,
                    None => return InteractionRes::Nil,
                };
                if set.is_empty() && count.is_some() {
                    return InteractionRes::MultiStringRes(vec![]);
                } else if set.is_empty() {
                    return InteractionRes::Nil;
                }
                let count = count.unwrap_or(1);
                if count < 0 {
                    return InteractionRes::Error(b"Count cannot be less than 0!");
                }
                let eles: Vec<Value> = set.iter().take(count as usize).cloned().collect();
                for ele in eles.iter() {
                    set.remove(ele);
                }
                InteractionRes::MultiStringRes(eles)
            }
            SetOps::SIsMember(key, member) => match engine.sets.read().unwrap().get(&key) {
                Some(set) => match set.get(&member) {
                    Some(_) => InteractionRes::UIntRes(1),
                    None => InteractionRes::UIntRes(0),
                },
                None => InteractionRes::UIntRes(0),
            },
            SetOps::SMove(src, dest, member) => {
                let sets = engine.sets.read().unwrap();
                if !sets.contains_key(&src) || !sets.contains_key(&dest) {
                    return InteractionRes::UIntRes(0);
                }
                let mut sets = engine.sets.write().unwrap();
                let src_set = sets.get_mut(&src).unwrap();
                match src_set.take(&member) {
                    Some(res) => {
                        sets.get_mut(&dest).unwrap().insert(res);
                        InteractionRes::UIntRes(1)
                    }
                    None => InteractionRes::UIntRes(0),
                }
            }
            SetOps::SRandMembers(key, count) => match engine.sets.read().unwrap().get(&key) {
                Some(set) => {
                    let count = count.unwrap_or(1);
                    if count < 0 {
                        return InteractionRes::MultiStringRes(
                            set.iter().cycle().take(-count as usize).cloned().collect(),
                        );
                    };
                    InteractionRes::MultiStringRes(
                        set.iter().take(count as usize).cloned().collect(),
                    )
                }
                None => InteractionRes::Nil,
            },
        }
    }
}
