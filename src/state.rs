// use rand::Rng;
use crate::ops::Ops;
use crate::types::UpdateState;
use crate::types::{Database, State, UpdateRes};
use bincode::{serialize, Result as BinCodeResult};
use std::collections::{HashSet, VecDeque};
use std::fmt;

impl fmt::Display for UpdateRes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UpdateRes::Ok => write!(f, "OK"),
            UpdateRes::StringRes(s) => write!(f, "{:?}", s),
            UpdateRes::UIntRes(i) => write!(f, "{:?}", i),
            UpdateRes::MultiStringRes(ss) => write!(f, "{:?}", ss),
            UpdateRes::Nil => write!(f, "(nil)"),
            UpdateRes::Error(e) => write!(f, "ERR {:?}", e),
            // TODO: Figure out how make futures work
            // UpdateRes::FutureRes(v, _) => (*v).fmt(f),
            // UpdateRes::FutureResValue(_) => unreachable!(),
        }
    }
}

impl State {
    pub fn save_state(&self) -> BinCodeResult<Vec<u8>> {
        serialize(&Database {
            kv: serialize(&*self.kv.read().unwrap()).unwrap(),
            sets: serialize(&*self.sets.read().unwrap()).unwrap(),
            lists: serialize(&*self.lists.read().unwrap()).unwrap(),
        })
    }

    pub fn create_list_if_necessary(&self, list_key: &[u8]) {
        if !self.lists.read().unwrap().contains_key(list_key) {
            self.lists
                .write()
                .unwrap()
                .insert(list_key.to_vec(), VecDeque::new());
        }
    }

    pub fn create_set_if_necessary(&self, set_key: &[u8]) {
        if !self.sets.read().unwrap().contains_key(set_key) {
            self.sets
                .write()
                .unwrap()
                .insert(set_key.to_vec(), HashSet::new());
        }
    }

    pub fn update_state(self, action: Ops) -> UpdateRes {
        match action {
            Ops::Keys(key_op) => key_op.update(self),
            Ops::Lists(list_op) => list_op.update(self),
            Ops::Misc(misc_op) => misc_op.update(self),
            Ops::Sets(set_op) => set_op.update(self),
        }
    }
}
