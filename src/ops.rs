use std::convert::TryFrom;

use crate::hashes::HashOps;
use crate::keys::KeyOps;
use crate::lists::ListOps;
use crate::misc::MiscOps;
use crate::sets::SetOps;

use crate::types::{
    Count, Index, Key, RedisValue, Value, EMPTY_ARRAY, NULL_ARRAY, NULL_BULK_STRING,
};

#[derive(Debug, Clone)]
pub enum Ops {
    Keys(KeyOps),
    Sets(SetOps),
    Lists(ListOps),
    Misc(MiscOps),
    Hashes(HashOps),
}

#[derive(Debug)]
pub enum OpsError {
    InvalidStart,
    Noop,
    UnknownOp,
    NotEnoughArgs(usize, usize), // req, given
    WrongNumberOfArgs(usize, usize),
    InvalidType,
    SyntaxError,
}

impl From<OpsError> for RedisValue {
    fn from(op: OpsError) -> RedisValue {
        match op {
            OpsError::InvalidStart => RedisValue::Error(b"Invalid start!".to_vec()),
            OpsError::UnknownOp => RedisValue::Error(b"Unknown Operation!".to_vec()),
            OpsError::NotEnoughArgs(req, given) => {
                let f = format!("Not enough arguments, {} required, {} given!", req, given);
                RedisValue::Error(f.as_bytes().to_vec())
            }
            OpsError::WrongNumberOfArgs(required, given) => {
                let f = format!(
                    "Wrong number of arguments! ({} required, {} given)",
                    required, given
                );
                RedisValue::Error(f.as_bytes().to_vec())
            }
            OpsError::InvalidType => RedisValue::Error(b"Invalid Type!".to_vec()),
            OpsError::SyntaxError => RedisValue::Error(b"Syntax Error!".to_vec()),
            OpsError::Noop => RedisValue::Error(b"".to_vec()),
        }
    }
}

impl ToString for RedisValue {
    fn to_string(&self) -> String {
        match self {
            RedisValue::SimpleString(s) => format!("+{}\r\n", String::from_utf8_lossy(s)),
            RedisValue::Error(e) => format!("-{}\r\n", String::from_utf8_lossy(e)),
            RedisValue::BulkString(s) => {
                format!("${}\r\n{}\r\n", s.len(), String::from_utf8_lossy(s))
            }
            RedisValue::Int(i) => format!(":{}\r\n", i.to_string()),
            RedisValue::Array(a) => {
                if a.is_empty() {
                    return EMPTY_ARRAY.to_string();
                }
                let contents: String = a
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<String>>()
                    .join("");
                if contents.ends_with("\r\n") {
                    return format!("*{:?}\r\n{}", a.len(), contents);
                }
                format!("*{:?}\r\n{:?}\r\n", a.len(), contents)
            }
            RedisValue::NullBulkString => NULL_BULK_STRING.to_string(),
            RedisValue::NullArray => NULL_ARRAY.to_string(),
        }
    }
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
            RedisValue::SimpleString(s) | RedisValue::BulkString(s) => {
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
        "ping" => Ok(Ops::Misc(MiscOps::Pong)),
        "keys" => Ok(Ops::Misc(MiscOps::Keys)),
        "flushall" => Ok(Ops::Misc(MiscOps::FlushAll)),
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
        return Err(OpsError::NotEnoughArgs(min_size, v.len()));
    }
    Ok(())
}

fn verify_size(v: &[&RedisValue], size: usize) -> Result<(), OpsError> {
    if v.len() != size {
        return Err(OpsError::WrongNumberOfArgs(size, v.len()));
    }
    Ok(())
}

fn get_key_and_val(array: &[RedisValue]) -> Result<(Key, Value), OpsError> {
    if array.len() < 3 {
        return Err(OpsError::WrongNumberOfArgs(3, array.len()));
    }
    let key = Key::try_from(&array[1])?;
    let val = Value::try_from(&array[2])?;
    Ok((key, val))
}

fn get_key_and_tail(array: &[RedisValue]) -> Result<(Key, Vec<Value>), OpsError> {
    if array.len() < 3 {
        return Err(OpsError::WrongNumberOfArgs(3, array.len()));
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
            Ok(Ops::Keys(KeyOps::Set(key, val)))
            // Ok(Ops::Keys(KeyOp::Set(key, val)))
        }
        "get" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::Keys(KeyOps::Get(key)))
        }
        "del" => {
            verify_size_lower(&tail, 1)?;
            let keys = tails_as_strings(&tail)?;
            Ok(Ops::Keys(KeyOps::Del(keys)))
        }
        "rename" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let new_key = Key::try_from(tail[1])?;
            Ok(Ops::Keys(KeyOps::Rename(key, new_key)))
        }
        "exists" => {
            verify_size_lower(&tail, 1)?;
            let keys = tails_as_strings(&tail)?;
            Ok(Ops::Misc(MiscOps::Exists(keys)))
        }
        // Sets
        "sadd" => {
            let (set_key, vals) = get_key_and_tail(array)?;
            Ok(Ops::Sets(SetOps::SAdd(set_key, vals)))
        }
        "srem" => {
            let (set_key, vals) = get_key_and_tail(array)?;
            Ok(Ops::Sets(SetOps::SRem(set_key, vals)))
        }
        "smembers" => {
            verify_size(&tail, 1)?;
            let set_key = Key::try_from(tail[0])?;
            Ok(Ops::Sets(SetOps::SMembers(set_key)))
        }
        "scard" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::Sets(SetOps::SCard(key)))
        }
        "sdiff" => {
            verify_size_lower(&tail, 2)?;
            let keys = tails_as_strings(&tail)?;
            Ok(Ops::Sets(SetOps::SDiff(keys)))
        }
        "sunion" => {
            verify_size_lower(&tail, 2)?;
            let keys = tails_as_strings(&tail)?;
            Ok(Ops::Sets(SetOps::SUnion(keys)))
        }
        "sinter" => {
            verify_size_lower(&tail, 2)?;
            let keys = tails_as_strings(&tail)?;
            Ok(Ops::Sets(SetOps::SInter(keys)))
        }
        "sdiffstore" => {
            let (set_key, sets) = get_key_and_tail(array)?;
            Ok(Ops::Sets(SetOps::SDiffStore(set_key, sets)))
        }
        "sunionstore" => {
            let (set_key, sets) = get_key_and_tail(array)?;
            Ok(Ops::Sets(SetOps::SUnionStore(set_key, sets)))
        }
        "sinterstore" => {
            let (set_key, sets) = get_key_and_tail(array)?;
            Ok(Ops::Sets(SetOps::SInterStore(set_key, sets)))
        }
        "spop" => {
            verify_size_lower(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            let count = match tail.get(1) {
                Some(c) => Some(Count::try_from(*c)?),
                None => None,
            };
            Ok(Ops::Sets(SetOps::SPop(key, count)))
        }
        "sismember" => {
            let (key, member) = get_key_and_val(array)?;
            Ok(Ops::Sets(SetOps::SIsMember(key, member)))
        }
        "smove" => {
            verify_size(&tail, 3)?;
            let src = Key::try_from(tail[0])?;
            let dest = Key::try_from(tail[1])?;
            let member = Value::try_from(tail[2])?;
            Ok(Ops::Sets(SetOps::SMove(src, dest, member)))
        }
        "srandmember" => {
            verify_size_lower(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            let count = match tail.get(1) {
                Some(c) => Some(Count::try_from(*c)?),
                None => None,
            };
            Ok(Ops::Sets(SetOps::SRandMembers(key, count)))
        }
        "lpush" => {
            let (key, vals) = get_key_and_tail(array)?;
            Ok(Ops::Lists(ListOps::LPush(key, vals)))
        }
        "rpush" => {
            let (key, vals) = get_key_and_tail(array)?;
            Ok(Ops::Lists(ListOps::RPush(key, vals)))
        }
        "lpushx" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let val = Value::try_from(tail[1])?;
            Ok(Ops::Lists(ListOps::LPushX(key, val)))
        }
        "rpushx" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let val = Value::try_from(tail[1])?;
            Ok(Ops::Lists(ListOps::RPushX(key, val)))
        }
        "llen" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::Lists(ListOps::LLen(key)))
        }
        "lpop" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::Lists(ListOps::LPop(key)))
        }
        "blpop" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::Lists(ListOps::BLPop(key)))
        }
        "rpop" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::Lists(ListOps::RPop(key)))
        }
        "linsert" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::Lists(ListOps::LPop(key)))
        }
        "lindex" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let index = Index::try_from(tail[1])?;
            Ok(Ops::Lists(ListOps::LIndex(key, index)))
        }
        "lset" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let index = Index::try_from(tail[1])?;
            let value = Value::try_from(tail[2])?;
            Ok(Ops::Lists(ListOps::LSet(key, index, value)))
        }
        "lrange" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let start_index = Index::try_from(tail[1])?;
            let end_index = Index::try_from(tail[2])?;
            Ok(Ops::Lists(ListOps::LRange(key, start_index, end_index)))
        }
        "ltrim" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let start_index = Index::try_from(tail[1])?;
            let end_index = Index::try_from(tail[2])?;
            Ok(Ops::Lists(ListOps::LTrim(key, start_index, end_index)))
        }
        "rpoplpush" => {
            verify_size(&tail, 2)?;
            let source = Key::try_from(tail[0])?;
            let dest = Key::try_from(tail[1])?;
            Ok(Ops::Lists(ListOps::RPopLPush(source, dest)))
        }
        "hget" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let field = Key::try_from(tail[1])?;
            Ok(Ops::Hashes(HashOps::HGet(key, field)))
        }
        "hset" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let field = Key::try_from(tail[1])?;
            let value = Key::try_from(tail[2])?;
            Ok(Ops::Hashes(HashOps::HSet(key, field, value)))
        }
        "hexists" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let field = Key::try_from(tail[1])?;
            Ok(Ops::Hashes(HashOps::HExists(key, field)))
        }
        "hgetall" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            Ok(Ops::Hashes(HashOps::HGetAll(key)))
        }
        "hmget" => {
            verify_size_lower(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let fields = tails_as_strings(&tail[1..])?;
            println!("{:?}", fields);
            Ok(Ops::Hashes(HashOps::HMGet(key, fields)))
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
