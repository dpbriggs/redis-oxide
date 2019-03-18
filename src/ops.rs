use crate::types::RedisValue;
use std::convert::TryFrom;

use crate::types::{Count, Key, Value};

#[derive(Debug)]
pub enum Ops {
    // Key Value
    Set(Key, Value),
    Get(Key),
    Del(Vec<Key>),
    Rename(Key, Key),
    // Sets
    SAdd(Key, Vec<Value>),
    SRem(Key, Vec<Value>),
    SMembers(Key),
    SIsMember(Key, Value),
    SCard(Key),
    SDiff(Vec<Value>),
    SUnion(Vec<Value>),
    SInter(Vec<Value>),
    SDiffStore(Key, Vec<Value>),
    SUnionStore(Key, Vec<Value>),
    SInterStore(Key, Vec<Value>),
    SPop(Key, Option<Count>),
    SMove(Key, Key, Value),
    SRandMembers(Key, Option<Count>),
    // Lists
    LPush(Key, Vec<Value>),
    LPushX(Key, Value),
    LLen(Key),
    LPop(Key),
    // Misc
    Keys, // TODO: Add optional glob
    Exists(Vec<Key>),
    Pong,
}

#[derive(Debug)]
pub enum OpsError {
    InvalidStart,
    Noop,
    UnknownOp,
    NotEnoughArgs(usize),
    WrongNumberOfArgs(usize),
    InvalidType,
    SyntaxError,
}

impl TryFrom<RedisValue> for Vec<u8> {
    type Error = OpsError;

    fn try_from(r: RedisValue) -> Result<Value, Self::Error> {
        match r {
            RedisValue::SimpleString(s) => Ok(s),
            RedisValue::BulkString(s) => Ok(s),
            _ => Err(OpsError::InvalidType),
        }
    }
}

impl TryFrom<&RedisValue> for Vec<u8> {
    type Error = OpsError;

    fn try_from(r: &RedisValue) -> Result<Value, Self::Error> {
        Value::try_from(r.clone())
    }
}

impl TryFrom<RedisValue> for String {
    type Error = OpsError;

    fn try_from(r: RedisValue) -> Result<String, Self::Error> {
        match r {
            RedisValue::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            RedisValue::BulkString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            _ => Err(OpsError::InvalidType),
        }
    }
}

impl TryFrom<&RedisValue> for String {
    type Error = OpsError;

    fn try_from(r: &RedisValue) -> Result<String, Self::Error> {
        String::try_from(r.clone())
    }
}

impl TryFrom<&RedisValue> for Count {
    type Error = OpsError;

    fn try_from(r: &RedisValue) -> Result<Count, Self::Error> {
        match r {
            RedisValue::Int(e) => Ok(*e as Count),
            RedisValue::SimpleString(s) => {
                let s = String::from_utf8_lossy(&s);
                match s.parse::<Count>() {
                    Ok(i) => Ok(i),
                    Err(_) => Err(OpsError::InvalidType),
                }
            }
            _ => Err(OpsError::InvalidType),
        }
    }
}

fn translate_string(start: &[u8]) -> Result<Ops, OpsError> {
    let start = &String::from_utf8_lossy(start);
    match start.to_lowercase().as_ref() {
        "ping" => Ok(Ops::Pong),
        "keys" => Ok(Ops::Keys),
        _ => Err(OpsError::UnknownOp),
    }
}

fn all_strings(v: &[&RedisValue]) -> bool {
    v.iter().all(|x| match x {
        RedisValue::SimpleString(_) => true,
        RedisValue::BulkString(_) => true,
        _ => false,
    })
}

fn tails_as_strings(tail: &[&RedisValue]) -> Result<Vec<Value>, OpsError> {
    if !all_strings(&tail) {
        return Err(OpsError::InvalidType);
    }
    let keys: Vec<Value> = tail.iter().map(|x| Value::try_from(*x).unwrap()).collect();
    Ok(keys)
}

fn verify_size_lower(v: &[&RedisValue], min_size: usize) -> Result<(), OpsError> {
    if v.len() < min_size {
        return Err(OpsError::NotEnoughArgs(min_size));
    }
    Ok(())
}

fn verify_size(v: &[&RedisValue], size: usize) -> Result<(), OpsError> {
    if v.len() != size {
        return Err(OpsError::WrongNumberOfArgs(size));
    }
    Ok(())
}

fn get_key_and_val(array: &[RedisValue]) -> Result<(Key, Value), OpsError> {
    if array.len() < 3 {
        return Err(OpsError::WrongNumberOfArgs(3));
    }
    let key = Key::try_from(&array[1])?;
    let val = Value::try_from(&array[2])?;
    Ok((key, val))
}

fn get_key_and_tail(array: &[RedisValue]) -> Result<(Key, Vec<Value>), OpsError> {
    if array.len() < 3 {
        return Err(OpsError::WrongNumberOfArgs(3));
    }
    let set_key = Key::try_from(&array[1])?;
    let tail: Vec<_> = array.iter().skip(2).collect();
    let vals = tails_as_strings(&tail)?;
    Ok((set_key, vals))
}

fn translate_array(array: &[RedisValue]) -> Result<Ops, OpsError> {
    if array.is_empty() {
        return Err(OpsError::Noop);
    }
    let head = Value::try_from(&array[0])?;
    if let Ok(op) = translate_string(&head) {
        return Ok(op);
    }
    let tail: Vec<&RedisValue> = array.iter().skip(1).collect();
    let head = &String::from_utf8_lossy(&head);
    match head.to_lowercase().as_ref() {
        // Key-Value
        "set" => {
            let (key, val) = get_key_and_val(array)?;
            Ok(Ops::Set(key, val))
        }
        "get" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::Get(key))
        }
        "del" => {
            verify_size_lower(&tail, 1)?;
            let keys = tails_as_strings(&tail)?;
            Ok(Ops::Del(keys))
        }
        "rename" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let new_key = Key::try_from(tail[1])?;
            Ok(Ops::Rename(key, new_key))
        }
        "exists" => {
            verify_size_lower(&tail, 1)?;
            let keys = tails_as_strings(&tail)?;
            Ok(Ops::Exists(keys))
        }
        // Sets
        "sadd" => {
            let (set_key, vals) = get_key_and_tail(array)?;
            Ok(Ops::SAdd(set_key, vals))
        }
        "srem" => {
            let (set_key, vals) = get_key_and_tail(array)?;
            Ok(Ops::SRem(set_key, vals))
        }
        "smembers" => {
            verify_size(&tail, 1)?;
            let set_key = Key::try_from(tail[0])?;
            Ok(Ops::SMembers(set_key))
        }
        "scard" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::SCard(key))
        }
        "sdiff" => {
            verify_size_lower(&tail, 2)?;
            let keys = tails_as_strings(&tail)?;
            Ok(Ops::SDiff(keys))
        }
        "sunion" => {
            verify_size_lower(&tail, 2)?;
            let keys = tails_as_strings(&tail)?;
            Ok(Ops::SUnion(keys))
        }
        "sinter" => {
            verify_size_lower(&tail, 2)?;
            let keys = tails_as_strings(&tail)?;
            Ok(Ops::SInter(keys))
        }
        "sdiffstore" => {
            let (set_key, sets) = get_key_and_tail(array)?;
            Ok(Ops::SDiffStore(set_key, sets))
        }
        "sunionstore" => {
            let (set_key, sets) = get_key_and_tail(array)?;
            Ok(Ops::SUnionStore(set_key, sets))
        }
        "sinterstore" => {
            let (set_key, sets) = get_key_and_tail(array)?;
            Ok(Ops::SInterStore(set_key, sets))
        }
        "spop" => {
            verify_size_lower(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            let count = match tail.get(1) {
                Some(c) => Some(Count::try_from(*c)?),
                None => None,
            };
            Ok(Ops::SPop(key, count))
        }
        "sismember" => {
            let (key, member) = get_key_and_val(array)?;
            Ok(Ops::SIsMember(key, member))
        }
        "smove" => {
            verify_size(&tail, 3)?;
            let src = Key::try_from(tail[0])?;
            let dest = Key::try_from(tail[1])?;
            let member = Value::try_from(tail[2])?;
            Ok(Ops::SMove(src, dest, member))
        }
        "srandmember" => {
            verify_size_lower(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            let count = match tail.get(1) {
                Some(c) => Some(Count::try_from(*c)?),
                None => None,
            };
            Ok(Ops::SRandMembers(key, count))
        }
        "lpush" => {
            let (key, vals) = get_key_and_tail(array)?;
            Ok(Ops::LPush(key, vals))
        }
        "lpushx" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let val = Value::try_from(tail[1])?;
            Ok(Ops::LPushX(key, val))
        }
        "llen" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::LLen(key))
        }
        "lpop" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::LPop(key))
        }
        "linsert" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::LPop(key))
        }
        _ => Err(OpsError::UnknownOp),
    }
}

pub fn translate(rv: &RedisValue) -> Result<Ops, OpsError> {
    match rv {
        RedisValue::SimpleString(s_string) => translate_string(s_string),
        RedisValue::BulkString(s_string) => translate_string(s_string),
        RedisValue::Array(vals) => translate_array(vals),
        _ => Err(OpsError::UnknownOp),
    }
}
