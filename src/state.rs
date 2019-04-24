// use rand::Rng;
use crate::ops::Ops;
use crate::types::StateInteration;
use crate::types::{InteractionRes, State};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

impl fmt::Display for InteractionRes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InteractionRes::Ok => write!(f, "OK"),
            InteractionRes::StringRes(s) => write!(f, "{:?}", s),
            InteractionRes::IntRes(i) => write!(f, "{:?}", i),
            InteractionRes::MultiStringRes(ss) => write!(f, "{:?}", ss),
            InteractionRes::Nil => write!(f, "(nil)"),
            InteractionRes::Error(e) => write!(f, "ERR {:?}", e),
            InteractionRes::Array(a) => write!(f, "{:?}", a),
            // TODO: Figure out how make futures work
            // InteractionRes::FutureRes(v, _) => (*v).fmt(f),
            // InteractionRes::FutureResValue(_) => unreachable!(),
        }
    }
}

impl State {
    // pub fn load(dump_data: &[u8]) -> State {
    //     let database = deserialize::<Database>(dump_data).unwrap();
    //     let kv = deserialize::<KeyString>(&database.kv);
    //     println!("{:?}", kv);
    //     State::default()
    // }

    pub fn create_list_if_necessary(&self, list_key: &[u8]) {
        if !self.lists.read().contains_key(list_key) {
            self.lists
                .write()
                .insert(list_key.to_vec(), VecDeque::new());
        }
    }

    pub fn create_hashes_if_necessary(&self, hashes_key: &[u8]) {
        if !self.hashes.read().contains_key(hashes_key) {
            self.hashes
                .write()
                .insert(hashes_key.to_vec(), HashMap::new());
        }
    }

    pub fn create_set_if_necessary(&self, set_key: &[u8]) {
        if !self.sets.read().contains_key(set_key) {
            self.sets.write().insert(set_key.to_vec(), HashSet::new());
        }
    }

    pub fn interact(self, action: Ops) -> InteractionRes {
        match action {
            Ops::Keys(key_op) => key_op.interact(self),
            Ops::Lists(list_op) => list_op.interact(self),
            Ops::Misc(misc_op) => misc_op.interact(self),
            Ops::Sets(set_op) => set_op.interact(self),
            Ops::Hashes(hash_op) => hash_op.interact(self),
        }
    }
}
