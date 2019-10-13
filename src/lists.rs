use crate::blocking::KeyBlocking;
use crate::types::{
    Count, Index, InteractionRes, Key, ReturnValue, StateInteration, StateRef, Value,
};
use crate::{make_reader, make_writer};
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
    BLPop(Key),
    BRPop(Key),
}

make_reader!(lists, read_lists);
make_writer!(lists, write_lists);

impl StateInteration for ListOps {
    #[allow(clippy::cognitive_complexity)]
    fn interact(self, state: StateRef) -> InteractionRes {
        match self {
            ListOps::LPush(key, vals) => {
                state.create_list_if_necessary(&key);
                write_lists!(state, &key, list);
                for val in vals {
                    list.push_front(val);
                }
                state.wake_list(&key);
                ReturnValue::IntRes(list.len() as Count).into()
            }
            ListOps::LPushX(key, val) => {
                if !read_lists!(state).contains_key(&key) {
                    return ReturnValue::IntRes(0).into();
                }
                state.create_list_if_necessary(&key);
                write_lists!(state, &key, list);
                list.push_front(val);
                state.wake_list(&key);
                ReturnValue::IntRes(list.len() as Count).into()
            }
            ListOps::LLen(key) => match read_lists!(state, &key) {
                Some(l) => ReturnValue::IntRes(l.len() as Count).into(),
                None => ReturnValue::IntRes(0).into(),
            },
            ListOps::LPop(key) => match write_lists!(state, &key).and_then(VecDeque::pop_front) {
                Some(v) => ReturnValue::StringRes(v).into(),
                None => ReturnValue::Nil.into(),
            },
            ListOps::RPop(key) => match write_lists!(state, &key).and_then(VecDeque::pop_back) {
                Some(v) => ReturnValue::StringRes(v).into(),
                None => ReturnValue::Nil.into(),
            },
            ListOps::RPush(key, vals) => {
                state.create_list_if_necessary(&key);
                write_lists!(state, &key, list);
                for val in vals {
                    list.push_back(val)
                }
                ReturnValue::IntRes(list.len() as Count).into()
            }
            ListOps::RPushX(key, val) => {
                if !read_lists!(state).contains_key(&key) {
                    return ReturnValue::IntRes(0).into();
                }
                state.create_list_if_necessary(&key);
                write_lists!(state, &key, list);
                list.push_back(val);
                state.wake_list(&key);
                ReturnValue::IntRes(list.len() as Count).into()
            }
            ListOps::LIndex(key, index) => match write_lists!(state, &key) {
                Some(list) => {
                    let llen = list.len() as i64;
                    let real_index = if index < 0 { llen + index } else { index };
                    if !(0 <= real_index && real_index < llen) {
                        return ReturnValue::Error(b"Bad Range!").into();
                    }
                    let real_index = real_index as usize;
                    ReturnValue::StringRes(list[real_index].to_vec()).into()
                }
                None => ReturnValue::Nil.into(),
            },
            ListOps::LSet(key, index, value) => match write_lists!(state, &key) {
                Some(list) => {
                    let llen = list.len() as i64;
                    let real_index = if index < 0 { llen + index } else { index };
                    if !(0 <= real_index && real_index < llen) {
                        return ReturnValue::Error(b"Bad Range!").into();
                    }
                    let real_index = real_index as usize;
                    list[real_index] = value;
                    ReturnValue::Ok.into()
                }
                None => ReturnValue::Error(b"No list at key!").into(),
            },
            ListOps::LRange(key, start_index, end_index) => match read_lists!(state, &key) {
                Some(list) => {
                    let start_index =
                        std::cmp::max(0, if start_index < 0 { 0 } else { start_index } as usize);
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
                    ReturnValue::MultiStringRes(ret).into()
                }
                None => ReturnValue::MultiStringRes(vec![]).into(),
            },
            ListOps::LTrim(key, start_index, end_index) => {
                match write_lists!(state, &key) {
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
                        ReturnValue::Ok.into()
                    }
                    None => ReturnValue::Ok.into(),
                }
            }
            ListOps::RPopLPush(source, dest) => {
                if source != dest {
                    state.create_list_if_necessary(&dest);
                }
                let mut lists = write_lists!(state);
                match lists.get_mut(&source) {
                    None => ReturnValue::Nil.into(),
                    Some(source_list) => match source_list.pop_back() {
                        None => ReturnValue::Nil.into(),
                        Some(value) => {
                            if source == dest {
                                source_list.push_back(value.clone());
                            } else {
                                lists.get_mut(&dest).unwrap().push_back(value.clone());
                                state.wake_list(&dest);
                            }
                            ReturnValue::StringRes(value).into()
                        }
                    },
                }
            }
            ListOps::BLPop(key) => {
                let state_clone = state.clone();
                let key_clone = key.clone();
                let bl = move || {
                    write_lists!(state, &key)
                        .and_then(VecDeque::pop_front)
                        .map(ReturnValue::StringRes)
                };
                KeyBlocking::interaction_res(Box::new(bl), state_clone, key_clone)
            }
            ListOps::BRPop(key) => {
                let state_clone = state.clone();
                let key_clone = key.clone();
                let bl = move || {
                    write_lists!(state, &key)
                        .and_then(VecDeque::pop_back)
                        .map(ReturnValue::StringRes)
                };
                KeyBlocking::interaction_res(Box::new(bl), state_clone, key_clone)
            }
        }
    }
}
