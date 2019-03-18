// use rand::Rng;
use crate::ops::Ops;
use crate::types::{EngineRes, Key, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::{Arc, RwLock};

type KeyString = HashMap<Key, Value>;
type KeySet = HashMap<Key, HashSet<Value>>;
type KeyList = HashMap<Key, VecDeque<Value>>;

#[derive(Default, Debug, Clone)]
pub struct Engine {
    kv: Arc<RwLock<KeyString>>,
    sets: Arc<RwLock<KeySet>>,
    lists: Arc<RwLock<KeyList>>,
}

impl fmt::Display for EngineRes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EngineRes::Ok => write!(f, "OK"),
            EngineRes::StringRes(s) => write!(f, "{:?}", s),
            EngineRes::UIntRes(i) => write!(f, "{:?}", i),
            EngineRes::MultiStringRes(ss) => write!(f, "{:?}", ss),
            EngineRes::Nil => write!(f, "(nil)"),
            EngineRes::Error(e) => write!(f, "ERR {:?}", e),
        }
    }
}

enum SetOp {
    Diff,
    Union,
    Inter,
}

impl Engine {
    fn many_set_op(&self, keys: Vec<Key>, op: SetOp) -> Option<HashSet<Value>> {
        let engine_sets = self.sets.read().unwrap();
        let sets: Vec<HashSet<Key>> = keys
            .iter()
            .filter_map(|key| engine_sets.get(key))
            .cloned()
            .collect();
        if sets.is_empty() {
            return None;
        }
        // TODO: Figure this mess of cloning
        let mut head: HashSet<Key> = (*sets.first().unwrap()).to_owned();
        for set in sets.iter().skip(1).cloned() {
            head = match op {
                SetOp::Diff => head.difference(&set).cloned().collect(),
                SetOp::Union => head.union(&set).cloned().collect(),
                SetOp::Inter => head.intersection(&set).cloned().collect(),
            }
        }
        Some(head)
    }

    fn create_list_if_necessary(&self, list_key: &Key) {
        if !self.lists.read().unwrap().contains_key(list_key) {
            self.lists
                .write()
                .unwrap()
                .insert(list_key.clone(), VecDeque::new());
        }
    }

    fn create_set_if_necessary(&self, set_key: &Key) {
        if !self.sets.read().unwrap().contains_key(set_key) {
            self.sets
                .write()
                .unwrap()
                .insert(set_key.clone(), HashSet::new());
        }
    }

    pub fn exec(self, action: Ops) -> EngineRes {
        match action {
            Ops::Get(key) => self
                .kv
                .read()
                .unwrap()
                .get(&key)
                .map_or(EngineRes::Nil, |v| EngineRes::StringRes(v.to_vec())),
            Ops::Set(key, value) => {
                self.kv.write().unwrap().insert(key, value);
                EngineRes::Ok
            }
            Ops::Del(keys) => {
                let deleted = keys
                    .iter()
                    .map(|x| self.kv.write().unwrap().remove(x))
                    .filter(Option::is_some)
                    .count();
                EngineRes::UIntRes(deleted)
            }
            Ops::Rename(key, new_key) => match self.kv.write().unwrap().remove(&key) {
                Some(value) => {
                    self.kv.write().unwrap().insert(new_key, value);
                    EngineRes::Ok
                }
                None => EngineRes::Error(b"no such key"),
            },
            Ops::Pong => EngineRes::StringRes(b"PONG".to_vec()),
            Ops::Exists(keys) => EngineRes::UIntRes(
                keys.iter()
                    .map(|key| self.kv.read().unwrap().contains_key(key))
                    .filter(|exists| *exists)
                    .count(),
            ),
            Ops::Keys => EngineRes::MultiStringRes(
                self.kv
                    .read()
                    .unwrap()
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect(),
            ),
            Ops::SAdd(set_key, vals) => {
                self.create_set_if_necessary(&set_key);
                let mut sets = self.sets.write().unwrap();
                let set = sets.get_mut(&set_key).unwrap();

                let mut vals_inserted = 0;
                for val in vals {
                    if set.insert(val) {
                        vals_inserted += 1;
                    }
                }
                EngineRes::UIntRes(vals_inserted)
            }
            Ops::SMembers(set_key) => match self.sets.read().unwrap().get(&set_key) {
                Some(hs) => EngineRes::MultiStringRes(hs.iter().cloned().collect()),
                None => EngineRes::MultiStringRes(vec![]),
            },
            Ops::SCard(set_key) => match self.sets.read().unwrap().get(&set_key) {
                Some(hs) => EngineRes::UIntRes(hs.len()),
                None => EngineRes::UIntRes(0),
            },
            Ops::SRem(set_key, vals) => match self.sets.write().unwrap().get_mut(&set_key) {
                Some(hs) => {
                    let mut vals_removed = 0;
                    for val in vals {
                        if hs.remove(&val) {
                            vals_removed += 1;
                        }
                    }
                    EngineRes::UIntRes(vals_removed)
                }
                None => EngineRes::UIntRes(0),
            },
            Ops::SDiff(keys) => match self.many_set_op(keys, SetOp::Diff) {
                Some(hash_set) => EngineRes::MultiStringRes(hash_set.iter().cloned().collect()),
                None => EngineRes::MultiStringRes(vec![]),
            },
            Ops::SUnion(keys) => match self.many_set_op(keys, SetOp::Union) {
                Some(hash_set) => EngineRes::MultiStringRes(hash_set.iter().cloned().collect()),
                None => EngineRes::MultiStringRes(vec![]),
            },
            Ops::SInter(keys) => match self.many_set_op(keys, SetOp::Inter) {
                Some(hash_set) => EngineRes::MultiStringRes(hash_set.iter().cloned().collect()),
                None => EngineRes::MultiStringRes(vec![]),
            },
            Ops::SDiffStore(to_store, keys) => match self.many_set_op(keys, SetOp::Diff) {
                Some(hash_set) => {
                    let hash_set_size = hash_set.len();
                    self.sets.write().unwrap().insert(to_store, hash_set);
                    EngineRes::UIntRes(hash_set_size)
                }
                None => EngineRes::UIntRes(0),
            },
            Ops::SUnionStore(to_store, keys) => match self.many_set_op(keys, SetOp::Inter) {
                Some(hash_set) => {
                    let hash_set_size = hash_set.len();
                    self.sets.write().unwrap().insert(to_store, hash_set);
                    EngineRes::UIntRes(hash_set_size)
                }
                None => EngineRes::UIntRes(0),
            },
            Ops::SInterStore(to_store, keys) => match self.many_set_op(keys, SetOp::Inter) {
                Some(hash_set) => {
                    let hash_set_size = hash_set.len();
                    self.sets.write().unwrap().insert(to_store, hash_set);
                    EngineRes::UIntRes(hash_set_size)
                }
                None => EngineRes::UIntRes(0),
            },
            // There's some surprising complexity behind this command
            Ops::SPop(key, count) => {
                let mut sets = self.sets.write().unwrap();
                let set = match sets.get_mut(&key) {
                    Some(s) => s,
                    None => return EngineRes::Nil,
                };
                if set.is_empty() && count.is_some() {
                    return EngineRes::MultiStringRes(vec![]);
                } else if set.is_empty() {
                    return EngineRes::Nil;
                }
                let count = count.unwrap_or(1);
                if count < 0 {
                    return EngineRes::Error(b"Count cannot be less than 0!");
                }
                let eles: Vec<Value> = set.iter().take(count as usize).cloned().collect();
                for ele in eles.iter() {
                    set.remove(ele);
                }
                EngineRes::MultiStringRes(eles)
            }
            Ops::SIsMember(key, member) => match self.sets.read().unwrap().get(&key) {
                Some(set) => match set.get(&member) {
                    Some(_) => EngineRes::UIntRes(1),
                    None => EngineRes::UIntRes(0),
                },
                None => EngineRes::UIntRes(0),
            },
            Ops::SMove(src, dest, member) => {
                let sets = self.sets.read().unwrap();
                if !sets.contains_key(&src) || !sets.contains_key(&dest) {
                    return EngineRes::UIntRes(0);
                }
                let mut sets = self.sets.write().unwrap();
                let src_set = sets.get_mut(&src).unwrap();
                match src_set.take(&member) {
                    Some(res) => {
                        sets.get_mut(&dest).unwrap().insert(res);
                        EngineRes::UIntRes(1)
                    }
                    None => EngineRes::UIntRes(0),
                }
            }
            // TODO: Actually make this random
            Ops::SRandMembers(key, count) => match self.sets.read().unwrap().get(&key) {
                Some(set) => {
                    let count = count.unwrap_or(1);
                    if count < 0 {
                        return EngineRes::MultiStringRes(
                            set.iter().cycle().take(-count as usize).cloned().collect(),
                        );
                    };
                    EngineRes::MultiStringRes(set.iter().take(count as usize).cloned().collect())
                }
                None => EngineRes::Nil,
            },
            Ops::LPush(key, vals) => {
                self.create_list_if_necessary(&key);
                let mut lists = self.lists.write().unwrap();
                let list = lists.get_mut(&key).unwrap();
                for val in vals {
                    list.push_front(val)
                }
                EngineRes::UIntRes(list.len())
            }
            Ops::LPushX(key, val) => {
                if !self.lists.read().unwrap().contains_key(&key) {
                    return EngineRes::UIntRes(0);
                }
                self.create_list_if_necessary(&key);
                let mut lists = self.lists.write().unwrap();
                let list = lists.get_mut(&key).unwrap();
                list.push_front(val);
                EngineRes::UIntRes(list.len())
            }
            Ops::LLen(key) => match self.lists.read().unwrap().get(&key) {
                Some(l) => EngineRes::UIntRes(l.len()),
                None => EngineRes::UIntRes(0),
            },
            Ops::LPop(key) => match self
                .lists
                .write()
                .unwrap()
                .get_mut(&key)
                .and_then(VecDeque::pop_front)
            {
                Some(v) => EngineRes::StringRes(v),
                None => EngineRes::Nil,
            },
        }
    }
}
