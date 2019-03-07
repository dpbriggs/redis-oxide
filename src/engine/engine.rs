use crate::resp::ops::Key;
// use rand::Rng;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

type KeyString = HashMap<Key, String>;
type KeySet = HashMap<Key, HashSet<String>>;
type KeyList = HashMap<Key, VecDeque<String>>;

use crate::resp::ops::Ops;

#[derive(Default, Debug)]
pub struct Engine {
    kv: KeyString,
    sets: KeySet,
    lists: KeyList,
}

#[derive(Debug)]
pub enum EngineRes {
    Ok,
    StringRes(String),
    Error(&'static str),
    MultiStringRes(Vec<String>),
    UIntRes(usize),
    Nil,
}

impl fmt::Display for EngineRes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EngineRes::Ok => write!(f, "OK"),
            EngineRes::StringRes(s) => write!(f, "{}", s),
            EngineRes::UIntRes(i) => write!(f, "{}", i),
            EngineRes::MultiStringRes(ss) => write!(f, "{:?}", ss),
            EngineRes::Nil => write!(f, "(nil)"),
            EngineRes::Error(e) => write!(f, "ERR {}", e),
        }
    }
}

enum SetOp {
    Diff,
    Union,
    Inter,
}

impl Engine {
    fn many_set_op(&self, keys: Vec<String>, op: SetOp) -> Option<HashSet<String>> {
        let sets: Vec<HashSet<String>> = keys
            .iter()
            .filter_map(|key| self.sets.get(key))
            .cloned()
            .collect();
        if sets.is_empty() {
            return None;
        }
        // TODO: Figure this mess of cloning
        let mut head: HashSet<String> = (*sets.first().unwrap()).to_owned();
        for set in sets.iter().skip(1).cloned() {
            head = match op {
                SetOp::Diff => head.difference(&set).cloned().collect(),
                SetOp::Union => head.union(&set).cloned().collect(),
                SetOp::Inter => head.intersection(&set).cloned().collect(),
            }
        }
        Some(head)
    }

    fn get_or_create_hash_set(&mut self, set_key: &str) -> &mut HashSet<String> {
        if !self.sets.contains_key(set_key) {
            self.sets
                .insert(set_key.to_string().clone(), HashSet::new());
        }
        self.sets.get_mut(set_key).unwrap()
    }

    fn get_or_create_list(&mut self, key: &str) -> &mut VecDeque<String> {
        if !self.lists.contains_key(key) {
            self.lists.insert(key.to_string().clone(), VecDeque::new());
        }
        self.lists.get_mut(key).unwrap()
    }

    pub fn exec(&mut self, action: Ops) -> EngineRes {
        match action {
            Ops::Get(key) => self
                .kv
                .get(&key)
                .map_or(EngineRes::Nil, |v| EngineRes::StringRes(v.to_string())),
            Ops::Set(key, value) => {
                self.kv.insert(key, value);
                EngineRes::Ok
            }
            Ops::Del(keys) => {
                let deleted = keys
                    .iter()
                    .map(|x| self.kv.remove(x))
                    .filter(|x| x.is_some())
                    .count();
                EngineRes::UIntRes(deleted)
            }
            Ops::Rename(key, new_key) => match self.kv.remove(&key) {
                Some(value) => {
                    self.kv.insert(new_key, value);
                    EngineRes::Ok
                }
                None => EngineRes::Error("no such key"),
            },
            Ops::Pong => EngineRes::StringRes("PONG".to_string()),
            Ops::Exists(keys) => EngineRes::UIntRes(
                keys.iter()
                    .map(|key| self.kv.contains_key(key))
                    .filter(|exists| *exists)
                    .count(),
            ),
            Ops::Keys => {
                EngineRes::MultiStringRes(self.kv.iter().map(|(key, _)| key.clone()).collect())
            }
            Ops::SAdd(set_key, vals) => {
                let set = self.get_or_create_hash_set(&set_key);
                let mut vals_inserted = 0;
                for val in vals {
                    if set.insert(val) {
                        vals_inserted += 1;
                    }
                }
                EngineRes::UIntRes(vals_inserted)
            }
            Ops::SMembers(set_key) => match self.sets.get(&set_key) {
                Some(hs) => EngineRes::MultiStringRes(hs.iter().map(ToString::to_string).collect()),
                None => EngineRes::MultiStringRes(vec![]),
            },
            Ops::SCard(set_key) => match self.sets.get(&set_key) {
                Some(hs) => EngineRes::UIntRes(hs.len()),
                None => EngineRes::UIntRes(0),
            },
            Ops::SRem(set_key, vals) => match self.sets.get_mut(&set_key) {
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
                    self.sets.insert(to_store, hash_set);
                    EngineRes::UIntRes(hash_set_size)
                }
                None => EngineRes::UIntRes(0),
            },
            Ops::SUnionStore(to_store, keys) => match self.many_set_op(keys, SetOp::Inter) {
                Some(hash_set) => {
                    let hash_set_size = hash_set.len();
                    self.sets.insert(to_store, hash_set);
                    EngineRes::UIntRes(hash_set_size)
                }
                None => EngineRes::UIntRes(0),
            },
            Ops::SInterStore(to_store, keys) => match self.many_set_op(keys, SetOp::Inter) {
                Some(hash_set) => {
                    let hash_set_size = hash_set.len();
                    self.sets.insert(to_store, hash_set);
                    EngineRes::UIntRes(hash_set_size)
                }
                None => EngineRes::UIntRes(0),
            },
            // There's some surprising complexity behind this command
            Ops::SPop(key, count) => {
                let set = match self.sets.get_mut(&key) {
                    Some(s) => s,
                    None => return EngineRes::Nil,
                };
                if set.is_empty() && count.is_some() {
                    return EngineRes::MultiStringRes(vec![]);
                } else if set.is_empty() {
                    return EngineRes::Nil;
                }
                let count = count.unwrap_or(1);
                let eles: Vec<String> = set.iter().take(count).cloned().collect();
                for ele in eles.iter() {
                    set.remove(ele);
                }
                EngineRes::MultiStringRes(eles)
            }
            Ops::SIsMember(key, member) => match self.sets.get(&key) {
                Some(set) => match set.get(&member) {
                    Some(_) => EngineRes::UIntRes(1),
                    None => EngineRes::UIntRes(0),
                },
                None => EngineRes::UIntRes(0),
            },
            Ops::SMove(src, dest, member) => {
                if !self.sets.contains_key(&src) || !self.sets.contains_key(&dest) {
                    return EngineRes::UIntRes(0);
                }
                let src_set = self.sets.get_mut(&src).unwrap();
                match src_set.take(&member) {
                    Some(res) => {
                        self.sets.get_mut(&dest).unwrap().insert(res);
                        EngineRes::UIntRes(1)
                    }
                    None => EngineRes::UIntRes(0),
                }
            }
            // TODO: Actually make this random
            Ops::SRandMembers(key, count) => match self.sets.get(&key) {
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
                let list = self.get_or_create_list(&key);
                for val in vals {
                    list.push_front(val)
                }
                EngineRes::UIntRes(list.len())
            }
            Ops::LPushX(key, val) => {
                if !self.lists.contains_key(&key) {
                    return EngineRes::UIntRes(0);
                }
                let list = self.get_or_create_list(&key);
                list.push_front(val);
                EngineRes::UIntRes(list.len())
            }
            Ops::LLen(key) => match self.lists.get(&key) {
                Some(l) => EngineRes::UIntRes(l.len()),
                None => EngineRes::UIntRes(0),
            },
            Ops::LPop(key) => match self.lists.get_mut(&key).and_then(|x| x.pop_front()) {
                Some(v) => EngineRes::StringRes(v),
                None => EngineRes::Nil,
            },
        }
    }
}
