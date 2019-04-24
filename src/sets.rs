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
macro_rules! read_sets {
    ($state:expr) => {
        $state.sets.read()
    };
    ($state:expr, $key:expr) => {
        $state.sets.read().get($key)
    };
    ($state:expr, $key:expr, $var_name:ident) => {
        let __temp_name = $state.sets.read();
        let $var_name = __temp_name.get($key);
    };
}

macro_rules! write_sets {
    ($state:expr) => {
        $state.sets.write()
    };
    ($state:expr, $key:expr) => {
        $state.sets.write().get_mut($key)
    };
    ($state: expr, $key:expr, $var_name:ident) => {
        let mut __temp_name = $state.sets.write();
        let $var_name = __temp_name.get_mut($key).unwrap();
    };
}

fn many_set_op(state: &State, keys: Vec<Key>, op: SetAction) -> Option<HashSet<Value>> {
    let state_sets = write_sets!(state);
    let sets: Vec<HashSet<Key>> = keys
        .iter()
        .filter_map(|key| state_sets.get(key))
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
    fn interact(self, state: State) -> InteractionRes {
        match self {
            SetOps::SAdd(set_key, vals) => {
                state.create_set_if_necessary(&set_key);
                write_sets!(state, &set_key, set);
                let mut vals_inserted = 0;
                for val in vals {
                    if set.insert(val) {
                        vals_inserted += 1;
                    }
                }
                InteractionRes::IntRes(vals_inserted)
            }
            SetOps::SMembers(set_key) => match read_sets!(state, &set_key) {
                Some(hs) => InteractionRes::MultiStringRes(hs.iter().cloned().collect()),
                None => InteractionRes::MultiStringRes(vec![]),
            },
            SetOps::SCard(set_key) => match read_sets!(state, &set_key) {
                Some(hs) => InteractionRes::IntRes(hs.len() as Count),
                None => InteractionRes::IntRes(0),
            },
            SetOps::SRem(set_key, vals) => match write_sets!(state, &set_key) {
                Some(hs) => {
                    let mut vals_removed = 0;
                    for val in vals {
                        if hs.remove(&val) {
                            vals_removed += 1;
                        }
                    }
                    InteractionRes::IntRes(vals_removed)
                }
                None => InteractionRes::IntRes(0),
            },
            SetOps::SDiff(keys) => match many_set_op(&state, keys, SetAction::Diff) {
                Some(hash_set) => {
                    InteractionRes::MultiStringRes(hash_set.iter().cloned().collect())
                }
                None => InteractionRes::MultiStringRes(vec![]),
            },
            SetOps::SUnion(keys) => match many_set_op(&state, keys, SetAction::Union) {
                Some(hash_set) => {
                    InteractionRes::MultiStringRes(hash_set.iter().cloned().collect())
                }
                None => InteractionRes::MultiStringRes(vec![]),
            },
            SetOps::SInter(keys) => match many_set_op(&state, keys, SetAction::Inter) {
                Some(hash_set) => {
                    InteractionRes::MultiStringRes(hash_set.iter().cloned().collect())
                }
                None => InteractionRes::MultiStringRes(vec![]),
            },
            SetOps::SDiffStore(to_store, keys) => {
                match many_set_op(&state, keys, SetAction::Diff) {
                    Some(hash_set) => {
                        let hash_set_size = hash_set.len();
                        write_sets!(state).insert(to_store, hash_set);
                        InteractionRes::IntRes(hash_set_size as Count)
                    }
                    None => InteractionRes::IntRes(0),
                }
            }
            SetOps::SUnionStore(to_store, keys) => {
                match many_set_op(&state, keys, SetAction::Union) {
                    Some(hash_set) => {
                        let hash_set_size = hash_set.len();
                        write_sets!(state).insert(to_store, hash_set);
                        InteractionRes::IntRes(hash_set_size as Count)
                    }
                    None => InteractionRes::IntRes(0),
                }
            }
            SetOps::SInterStore(to_store, keys) => {
                match many_set_op(&state, keys, SetAction::Inter) {
                    Some(hash_set) => {
                        let hash_set_size = hash_set.len();
                        write_sets!(state).insert(to_store, hash_set);
                        InteractionRes::IntRes(hash_set_size as Count)
                    }
                    None => InteractionRes::IntRes(0),
                }
            }
            // There's some surprising complexity behind this command
            SetOps::SPop(key, count) => {
                let mut sets = write_sets!(state);
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
            SetOps::SIsMember(key, member) => match read_sets!(state, &key) {
                Some(set) => match set.get(&member) {
                    Some(_) => InteractionRes::IntRes(1),
                    None => InteractionRes::IntRes(0),
                },
                None => InteractionRes::IntRes(0),
            },
            SetOps::SMove(src, dest, member) => {
                let sets = read_sets!(state);
                if !sets.contains_key(&src) || !sets.contains_key(&dest) {
                    return InteractionRes::IntRes(0);
                }

                let mut sets = write_sets!(state);
                let src_set = sets.get_mut(&src).unwrap();
                match src_set.take(&member) {
                    Some(res) => {
                        sets.get_mut(&dest).unwrap().insert(res);
                        InteractionRes::IntRes(1)
                    }
                    None => InteractionRes::IntRes(0),
                }
            }
            SetOps::SRandMembers(key, count) => match read_sets!(state, &key) {
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
