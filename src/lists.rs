use crate::types::{Index, Key, State, UpdateRes, UpdateState, Value};
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub enum ListOps {
    // List Operations
    LIndex(Key, Index),
    LLen(Key),
    LPop(Key),
    LPush(Key, Vec<Value>),
    LPushX(Key, Value),
    LRange(Key, Index, Index),
    LSet(Key, Index, Value),
    LTrim(Key, Index, Index),
    RPop(Key),
    RPush(Key, Vec<Value>),
    RPushX(Key, Value),
    RPopLPush(Key, Key),
}

impl UpdateState for ListOps {
    fn update(self, engine: State) -> UpdateRes {
        match self {
            ListOps::LPush(key, vals) => {
                engine.create_list_if_necessary(&key);
                let mut lists = engine.lists.write().unwrap();
                let list = lists.get_mut(&key).unwrap();
                for val in vals {
                    list.push_front(val)
                }
                UpdateRes::UIntRes(list.len())
            }
            ListOps::LPushX(key, val) => {
                if !engine.lists.read().unwrap().contains_key(&key) {
                    return UpdateRes::UIntRes(0);
                }
                engine.create_list_if_necessary(&key);
                let mut lists = engine.lists.write().unwrap();
                let list = lists.get_mut(&key).unwrap();
                list.push_front(val);
                UpdateRes::UIntRes(list.len())
            }
            ListOps::LLen(key) => match engine.lists.read().unwrap().get(&key) {
                Some(l) => UpdateRes::UIntRes(l.len()),
                None => UpdateRes::UIntRes(0),
            },
            ListOps::LPop(key) => match engine
                .lists
                .write()
                .unwrap()
                .get_mut(&key)
                .and_then(VecDeque::pop_front)
            {
                Some(v) => UpdateRes::StringRes(v),
                None => UpdateRes::Nil,
            },
            ListOps::RPop(key) => match engine
                .lists
                .write()
                .unwrap()
                .get_mut(&key)
                .and_then(VecDeque::pop_back)
            {
                Some(v) => UpdateRes::StringRes(v),
                None => UpdateRes::Nil,
            },
            ListOps::RPush(key, vals) => {
                engine.create_list_if_necessary(&key);
                let mut lists = engine.lists.write().unwrap();
                let list = lists.get_mut(&key).unwrap();
                for val in vals {
                    list.push_back(val)
                }
                UpdateRes::UIntRes(list.len())
            }
            ListOps::RPushX(key, val) => {
                if !engine.lists.read().unwrap().contains_key(&key) {
                    return UpdateRes::UIntRes(0);
                }
                engine.create_list_if_necessary(&key);
                let mut lists = engine.lists.write().unwrap();
                let list = lists.get_mut(&key).unwrap();
                list.push_back(val);
                UpdateRes::UIntRes(list.len())
            }
            ListOps::LIndex(key, index) => match engine.lists.read().unwrap().get(&key) {
                Some(list) => {
                    let llen = list.len() as i64;
                    let real_index = if index < 0 { llen + index } else { index };
                    if !(0 <= real_index && real_index < llen) {
                        return UpdateRes::Error(b"Bad Range!");
                    }
                    let real_index = real_index as usize;
                    UpdateRes::StringRes(list[real_index].to_vec())
                }
                None => UpdateRes::Nil,
            },
            ListOps::LSet(key, index, value) => match engine.lists.write().unwrap().get_mut(&key) {
                Some(list) => {
                    let llen = list.len() as i64;
                    let real_index = if index < 0 { llen + index } else { index };
                    if !(0 <= real_index && real_index < llen) {
                        return UpdateRes::Error(b"Bad Range!");
                    }
                    let real_index = real_index as usize;
                    list[real_index] = value;
                    UpdateRes::Ok
                }
                None => UpdateRes::Error(b"No list at key!"),
            },
            ListOps::LRange(key, start_index, end_index) => {
                match engine.lists.read().unwrap().get(&key) {
                    Some(list) => {
                        let start_index = std::cmp::max(
                            0,
                            if start_index < 0 { 0 } else { start_index } as usize,
                        );
                        let end_index = std::cmp::min(
                            list.len(),
                            if end_index < 0 {
                                list.len() as i64 + end_index
                            } else {
                                end_index
                            } as usize,
                        );
                        let mut ret = Vec::new();
                        for (index, value) in list.iter().enumerate() {
                            if start_index <= index && index <= end_index {
                                ret.push(value.clone());
                            }
                            if index > end_index {
                                break;
                            }
                        }
                        UpdateRes::MultiStringRes(ret)
                    }
                    None => UpdateRes::MultiStringRes(vec![]),
                }
            }
            ListOps::LTrim(key, start_index, end_index) => {
                match engine.lists.write().unwrap().get_mut(&key) {
                    Some(list) => {
                        let start_index = std::cmp::max(
                            0,
                            if start_index < 0 { 0 } else { start_index } as usize,
                        );
                        let end_index = std::cmp::min(
                            list.len(),
                            if end_index < 0 {
                                list.len() as i64 + end_index
                            } else {
                                end_index
                            } as usize,
                        ) + 1;
                        // Deal with right side
                        list.truncate(end_index);
                        // Deal with left side
                        for _ in 0..start_index {
                            list.pop_front();
                        }
                        UpdateRes::Ok
                    }
                    None => UpdateRes::Ok,
                }
            }
            ListOps::RPopLPush(source, dest) => {
                if source != dest {
                    engine.create_list_if_necessary(&dest);
                }
                let mut lists = engine.lists.write().unwrap();
                match lists.get_mut(&source) {
                    None => UpdateRes::Nil,
                    Some(source_list) => match source_list.pop_back() {
                        None => UpdateRes::Nil,
                        Some(value) => {
                            if source == dest {
                                source_list.push_back(value.clone());
                            } else {
                                lists.get_mut(&dest).unwrap().push_back(value.clone());
                            }
                            UpdateRes::StringRes(value)
                        }
                    },
                }
            }
        }
    }
}
