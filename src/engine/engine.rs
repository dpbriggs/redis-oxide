use crate::resp::ops::Key;
use std::collections::{HashMap, HashSet};
use std::fmt;

type KeyString = HashMap<Key, String>;
type KeySet = HashMap<Key, HashSet<String>>;
// type RedisObj = HashMap<Key, Value>;

use crate::resp::ops::Ops;

// #[derive(Debug)]
// enum Value {
//     String(KeyString),
//     Set(KeySet),
// }

#[derive(Default, Debug)]
pub struct Engine {
    kv: KeyString,
    sets: KeySet,
}

#[derive(Debug)]
pub enum EngineRes {
    Ok,
    StringRes(String),
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
        }
    }
}

impl Engine {
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
                if !self.sets.contains_key(&set_key) {
                    self.sets.insert(set_key.clone(), HashSet::new());
                }
                let foo: &mut HashSet<String> = self.sets.get_mut(&set_key).unwrap();
                let mut vals_inserted = 0;
                for val in vals {
                    if foo.insert(val) {
                        vals_inserted += 1;
                    }
                }
                EngineRes::UIntRes(vals_inserted)
            }
            Ops::SMembers(set_key) => match self.sets.get(&set_key) {
                Some(hs) => EngineRes::MultiStringRes(hs.iter().map(|x| x.to_string()).collect()),
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
            Ops::SDiff(keys) => {
                let sets: Vec<&HashSet<String>> =
                    keys.iter().filter_map(|key| self.sets.get(key)).collect();
                println!("{:?}", sets);
                if sets.is_empty() {
                    return EngineRes::MultiStringRes(vec![]);
                }
                // TODO: Figure this mess of cloning
                let mut head: HashSet<String> = sets.first().unwrap().clone().clone();
                for set in sets.iter().skip(1).cloned().cloned() {
                    head = head.difference(&set).cloned().collect();
                }
                EngineRes::MultiStringRes(head.iter().cloned().collect())
            }
        }
    }
}
