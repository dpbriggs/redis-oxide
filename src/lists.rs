use crate::timeouts::blocking_key_timeout;
use crate::types::{Count, Index, Key, ReturnValue, StateRef, UTimeout, Value};
use crate::{make_reader, make_writer, op_variants};
use std::collections::VecDeque;

op_variants! {
    ListOps,
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
    BLPop(Key, UTimeout),
    BRPop(Key, UTimeout)
}

make_reader!(lists, read_lists);
make_writer!(lists, write_lists);

#[allow(clippy::cognitive_complexity)]
pub async fn list_interact(list_op: ListOps, state: StateRef) -> ReturnValue {
    match list_op {
        ListOps::LPush(key, vals) => {
            let mut list_lock = state.lists.write();
            let list = list_lock.entry(key.clone()).or_default();
            for val in vals {
                list.push_front(val);
            }
            state.wake_list(&key);
            ReturnValue::IntRes(list.len() as Count)
        }
        ListOps::LPushX(key, val) => {
            let mut list_lock = state.lists.write();
            match list_lock.get_mut(&key) {
                Some(list) => {
                    list.push_front(val);
                    state.wake_list(&key);
                    ReturnValue::IntRes(list.len() as Count)
                }
                None => ReturnValue::IntRes(0),
            }
        }
        ListOps::RPushX(key, val) => {
            let mut list_lock = state.lists.write();
            match list_lock.get_mut(&key) {
                Some(list) => {
                    list.push_back(val);
                    state.wake_list(&key);
                    ReturnValue::IntRes(list.len() as Count)
                }
                None => ReturnValue::IntRes(0),
            }
        }
        ListOps::LLen(key) => match read_lists!(state, &key) {
            Some(l) => ReturnValue::IntRes(l.len() as Count),
            None => ReturnValue::IntRes(0),
        },
        ListOps::LPop(key) => match write_lists!(state, &key).and_then(VecDeque::pop_front) {
            Some(v) => ReturnValue::StringRes(v),
            None => ReturnValue::Nil,
        },
        ListOps::RPop(key) => match write_lists!(state, &key).and_then(VecDeque::pop_back) {
            Some(v) => ReturnValue::StringRes(v),
            None => ReturnValue::Nil,
        },
        ListOps::RPush(key, vals) => {
            let mut list_lock = state.lists.write();
            let list = list_lock.entry(key).or_default();
            for val in vals {
                list.push_back(val)
            }
            ReturnValue::IntRes(list.len() as Count)
        }
        ListOps::LIndex(key, index) => match write_lists!(state, &key) {
            Some(list) => {
                let llen = list.len() as i64;
                let real_index = if index < 0 { llen + index } else { index };
                if !(0 <= real_index && real_index < llen) {
                    return ReturnValue::Error(b"Bad Range!");
                }
                let real_index = real_index as usize;
                ReturnValue::StringRes(list[real_index].clone())
            }
            None => ReturnValue::Nil,
        },
        ListOps::LSet(key, index, value) => match write_lists!(state, &key) {
            Some(list) => {
                let llen = list.len() as i64;
                let real_index = if index < 0 { llen + index } else { index };
                if !(0 <= real_index && real_index < llen) {
                    return ReturnValue::Error(b"Bad Range!");
                }
                let real_index = real_index as usize;
                list[real_index] = value;
                ReturnValue::Ok
            }
            None => ReturnValue::Error(b"No list at key!"),
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
                ReturnValue::MultiStringRes(ret)
            }
            None => ReturnValue::MultiStringRes(vec![]),
        },
        ListOps::LTrim(key, start_index, end_index) => {
            match write_lists!(state, &key) {
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
                    ) + 1;
                    // Deal with right side
                    list.truncate(end_index);
                    // Deal with left side
                    for _ in 0..start_index {
                        list.pop_front();
                    }
                    ReturnValue::Ok
                }
                None => ReturnValue::Ok,
            }
        }
        ListOps::RPopLPush(source, dest) => {
            let mut lists = write_lists!(state);
            match lists.get_mut(&source) {
                None => ReturnValue::Nil,
                Some(source_list) => match source_list.pop_back() {
                    None => ReturnValue::Nil,
                    Some(value) => {
                        if source == dest {
                            source_list.push_back(value.clone());
                        } else {
                            lists
                                .entry(dest.clone())
                                .or_default()
                                .push_back(value.clone());
                            state.wake_list(&dest);
                        }
                        ReturnValue::StringRes(value)
                    }
                },
            }
        }
        ListOps::BLPop(key, timeout) => {
            let state_clone = state.clone();
            let key_clone = key.clone();
            let bl = move || {
                write_lists!(state, &key)
                    .and_then(VecDeque::pop_front)
                    .map(ReturnValue::StringRes)
            };
            blocking_key_timeout(Box::new(bl), state_clone, key_clone, timeout).await
        }
        ListOps::BRPop(key, timeout) => {
            let state_clone = state.clone();
            let key_clone = key.clone();
            let bl = move || {
                write_lists!(state, &key)
                    .and_then(VecDeque::pop_back)
                    .map(ReturnValue::StringRes)
            };
            blocking_key_timeout(Box::new(bl), state_clone, key_clone, timeout).await
        }
    }
}
