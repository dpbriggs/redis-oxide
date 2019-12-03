use std::convert::TryFrom;
use std::fmt::Debug;

use crate::bloom::{bloom_interact, BloomOps};
use crate::hashes::{hash_interact, HashOps};
use crate::keys::{key_interact, KeyOps};
use crate::lists::{list_interact, ListOps};
use crate::misc::MiscOps;
use crate::sets::{set_interact, SetOps};
use crate::sorted_sets::{zset_interact, ZSetOps};
use crate::types::{ReturnValue, StateRef};

use crate::types::{
    Count, Index, Key, RedisValue, Score, UTimeout, Value, EMPTY_ARRAY, NULL_ARRAY,
    NULL_BULK_STRING,
};

#[derive(Debug, Clone)]
pub enum Ops {
    Keys(KeyOps),
    Sets(SetOps),
    Lists(ListOps),
    Misc(MiscOps),
    Hashes(HashOps),
    ZSets(ZSetOps),
    Blooms(BloomOps),
}

/// Top level interaction function. Used by the server to run
/// operations against state.
pub async fn op_interact(op: Ops, state: StateRef) -> ReturnValue {
    match op {
        Ops::Keys(op) => key_interact(op, state).await,
        Ops::Sets(op) => set_interact(op, state).await,
        Ops::Lists(op) => list_interact(op, state).await,
        Ops::Hashes(op) => hash_interact(op, state).await,
        Ops::ZSets(op) => zset_interact(op, state).await,
        Ops::Blooms(op) => bloom_interact(op, state).await,
        _ => unreachable!(),
    }
}

#[derive(Debug)]
pub enum OpsError {
    InvalidStart,
    Noop,
    UnknownOp,
    NotEnoughArgs(usize, usize), // req, given
    WrongNumberOfArgs(usize, usize),
    InvalidArgPattern(&'static str),
    InvalidType,
    SyntaxError,
    InvalidArgs(String),
}

impl From<OpsError> for RedisValue {
    fn from(op: OpsError) -> RedisValue {
        match op {
            OpsError::InvalidStart => RedisValue::Error(b"Invalid start!".to_vec()),
            OpsError::UnknownOp => RedisValue::Error(b"Unknown Operation!".to_vec()),
            OpsError::InvalidArgPattern(explain) => {
                let f = format!("Invalid Arg Pattern, {}", explain);
                RedisValue::Error(f.as_bytes().to_vec())
            }
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
            OpsError::InvalidArgs(s) => RedisValue::Error(s.as_bytes().to_vec()),
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
            RedisValue::BulkString(s) | RedisValue::SimpleString(s) => {
                match String::from_utf8(s.to_owned()) {
                    Ok(s) => s.parse().map_err(|_| OpsError::InvalidType),
                    Err(_) => Err(OpsError::InvalidType),
                }
            }
            _ => Err(OpsError::InvalidType),
        }
    }
}

// Translate single RedisValue inputs into an Ops
// Used for commands like PING
// TODO: Get rid of this
fn translate_string(start: &[u8]) -> Result<Ops, OpsError> {
    let start = &String::from_utf8_lossy(start);
    match start.to_lowercase().as_ref() {
        "ping" => Ok(Ops::Misc(MiscOps::Pong)),
        "keys" => Ok(Ops::Misc(MiscOps::Keys)),
        "flushall" => Ok(Ops::Misc(MiscOps::FlushAll)),
        "flushdb" => Ok(Ops::Misc(MiscOps::FlushDB)),
        _ => Err(OpsError::UnknownOp),
    }
}

/// Ensure the passed collection has an even number of arguments.
fn ensure_even<T>(v: &[T]) -> Result<(), OpsError> {
    if v.len() % 2 != 0 {
        return Err(OpsError::InvalidArgPattern(
            "even number of arguments required!",
        ));
    }
    Ok(())
}

fn values_from_tail<'a, ValueType>(tail: &[&'a RedisValue]) -> Result<Vec<ValueType>, OpsError>
where
    ValueType: TryFrom<&'a RedisValue, Error = OpsError>,
{
    let mut items: Vec<ValueType> = Vec::new();
    for item in tail.iter() {
        let value = ValueType::try_from(item)?;
        items.push(value);
    }
    Ok(items)
}

/// Verify that the collection v has _at least_ min_size values.
/// e.g. If you wanted to verify that there's two or more items, min_size would be 2.
fn verify_size_lower<T>(v: &[T], min_size: usize) -> Result<(), OpsError> {
    if v.len() < min_size {
        return Err(OpsError::NotEnoughArgs(min_size, v.len()));
    }
    Ok(())
}

/// Verify the exact size of a sequence.
/// Useful for some commands that require an exact number of arguments (like get and set)
fn verify_size<T>(v: &[T], size: usize) -> Result<(), OpsError> {
    if v.len() != size {
        return Err(OpsError::WrongNumberOfArgs(size, v.len()));
    }
    Ok(())
}

/// Get a tuple of (KeyType, ValueType)
/// Mainly used for the thousand 2-adic ops
fn get_key_and_value<'a, KeyType, ValueType>(
    array: &'a [RedisValue],
) -> Result<(KeyType, ValueType), OpsError>
where
    KeyType: TryFrom<&'a RedisValue, Error = OpsError>,
    ValueType: TryFrom<&'a RedisValue, Error = OpsError>,
{
    if array.len() < 3 {
        return Err(OpsError::WrongNumberOfArgs(2, array.len() - 1));
    }
    let key = KeyType::try_from(&array[1])?;
    let val = ValueType::try_from(&array[2])?;
    Ok((key, val))
}

/// Transform &[RedisValue] into (KeyType, Vec<TailType>)
/// Used for commands like DEL arg1 arg2...
fn get_key_and_tail<'a, KeyType, TailType>(
    array: &'a [RedisValue],
) -> Result<(KeyType, Vec<TailType>), OpsError>
where
    KeyType: TryFrom<&'a RedisValue, Error = OpsError>,
    TailType: TryFrom<&'a RedisValue, Error = OpsError>,
{
    if array.len() < 3 {
        return Err(OpsError::WrongNumberOfArgs(3, array.len()));
    }
    let set_key = KeyType::try_from(&array[1])?;
    let mut tail: Vec<TailType> = Vec::new();
    for tail_item in array.iter().skip(2) {
        let tmp = TailType::try_from(tail_item)?;
        tail.push(tmp)
    }
    Ok((set_key, tail))
}

/// Transform a sequence of [Key1, Val1, Key2, Val2, ...] -> Vec<(Key, Value)>
fn get_key_value_pairs<'a, KeyType, ValueType>(
    tail: &[&'a RedisValue],
) -> Result<Vec<(KeyType, ValueType)>, OpsError>
where
    KeyType: TryFrom<&'a RedisValue, Error = OpsError> + Debug,
    ValueType: TryFrom<&'a RedisValue, Error = OpsError> + Debug,
{
    ensure_even(tail)?;
    let keys = tail.iter().step_by(2);
    let vals = tail.iter().skip(1).step_by(2);
    let mut ret = Vec::new();
    for (&key, &val) in keys.zip(vals) {
        let key = KeyType::try_from(key)?;
        let val = ValueType::try_from(val)?;
        ret.push((key, val))
    }
    Ok(ret)
}

/// Convenience macro to automatically construct the right variant
/// of Ops.
macro_rules! ok {
    (KeyOps::$OpName:ident($($OpArg:expr),*)) => {
        Ok(Ops::Keys(KeyOps::$OpName($( $OpArg ),*)))
    };
    (MiscOps::$OpName:ident($($OpArg:expr),*)) => {
        Ok(Ops::Misc(MiscOps::$OpName($( $OpArg ),*)))
    };
    (MiscOps::$OpName:ident) => {
        Ok(Ops::Misc(MiscOps::$OpName))
    };
    (SetOps::$OpName:ident($($OpArg:expr),*)) => {
        Ok(Ops::Sets(SetOps::$OpName($( $OpArg ),*)))
    };
    (HashOps::$OpName:ident($($OpArg:expr),*)) => {
        Ok(Ops::Hashes(HashOps::$OpName($( $OpArg ),*)))
    };
    (ListOps::$OpName:ident($($OpArg:expr),*)) => {
        Ok(Ops::Lists(ListOps::$OpName($( $OpArg ),*)))
    };
    (ZSetOps::$OpName:ident($($OpArg:expr),*)) => {
        Ok(Ops::ZSets(ZSetOps::$OpName($( $OpArg ),*)))
    };
    (BloomOps::$OpName:ident($($OpArg:expr),*)) => {
        Ok(Ops::Blooms(BloomOps::$OpName($( $OpArg ),*)))
    };
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
            let (key, val) = get_key_and_value(array)?;
            ok!(KeyOps::Set(key, val))
        }
        "mset" => ok!(KeyOps::MSet(get_key_value_pairs(&tail)?)),
        "get" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(KeyOps::Get(key))
        }
        "mget" => {
            verify_size_lower(&tail, 1)?;
            let keys = values_from_tail(&tail)?;
            ok!(KeyOps::MGet(keys))
        }
        "test" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(KeyOps::Test(key))
        }
        "del" => {
            verify_size_lower(&tail, 1)?;
            let keys = values_from_tail(&tail)?;
            ok!(KeyOps::Del(keys))
        }
        "rename" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let new_key = Key::try_from(tail[1])?;
            ok!(KeyOps::Rename(key, new_key))
        }
        "renamenx" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let new_key = Key::try_from(tail[1])?;
            ok!(KeyOps::RenameNx(key, new_key))
        }
        "exists" => {
            verify_size_lower(&tail, 1)?;
            let keys = values_from_tail(&tail)?;
            ok!(MiscOps::Exists(keys))
        }
        "printcmds" => ok!(MiscOps::PrintCmds),
        // Sets
        "sadd" => {
            let (set_key, vals) = get_key_and_tail(array)?;
            ok!(SetOps::SAdd(set_key, vals))
        }
        "srem" => {
            let (set_key, vals) = get_key_and_tail(array)?;
            ok!(SetOps::SRem(set_key, vals))
        }
        "smembers" => {
            verify_size(&tail, 1)?;
            let set_key = Key::try_from(tail[0])?;
            ok!(SetOps::SMembers(set_key))
        }
        "scard" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(SetOps::SCard(key))
        }
        "sdiff" => {
            verify_size_lower(&tail, 2)?;
            let keys = values_from_tail(&tail)?;
            ok!(SetOps::SDiff(keys))
        }
        "sunion" => {
            verify_size_lower(&tail, 2)?;
            let keys = values_from_tail(&tail)?;
            ok!(SetOps::SUnion(keys))
        }
        "sinter" => {
            verify_size_lower(&tail, 2)?;
            let keys = values_from_tail(&tail)?;
            ok!(SetOps::SInter(keys))
        }
        "sdiffstore" => {
            let (set_key, sets) = get_key_and_tail(array)?;
            ok!(SetOps::SDiffStore(set_key, sets))
        }
        "sunionstore" => {
            let (set_key, sets) = get_key_and_tail(array)?;
            ok!(SetOps::SUnionStore(set_key, sets))
        }
        "sinterstore" => {
            let (set_key, sets) = get_key_and_tail(array)?;
            ok!(SetOps::SInterStore(set_key, sets))
        }
        "spop" => {
            verify_size_lower(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            let count = match tail.get(1) {
                Some(c) => Some(Count::try_from(*c)?),
                None => None,
            };
            ok!(SetOps::SPop(key, count))
        }
        "sismember" => {
            let (key, member) = get_key_and_value(array)?;
            ok!(SetOps::SIsMember(key, member))
        }
        "smove" => {
            verify_size(&tail, 3)?;
            let src = Key::try_from(tail[0])?;
            let dest = Key::try_from(tail[1])?;
            let member = Value::try_from(tail[2])?;
            ok!(SetOps::SMove(src, dest, member))
        }
        "srandmember" => {
            verify_size_lower(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            let count = match tail.get(1) {
                Some(c) => Some(Count::try_from(*c)?),
                None => None,
            };
            ok!(SetOps::SRandMembers(key, count))
        }
        "lpush" => {
            let (key, vals) = get_key_and_tail(array)?;
            ok!(ListOps::LPush(key, vals))
        }
        "rpush" => {
            let (key, vals) = get_key_and_tail(array)?;
            ok!(ListOps::RPush(key, vals))
        }
        "lpushx" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let val = Value::try_from(tail[1])?;
            ok!(ListOps::LPushX(key, val))
        }
        "rpushx" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let val = Value::try_from(tail[1])?;
            ok!(ListOps::RPushX(key, val))
        }
        "llen" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(ListOps::LLen(key))
        }
        "lpop" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(ListOps::LPop(key))
        }
        "blpop" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let timeout = UTimeout::try_from(tail[1])?;
            ok!(ListOps::BLPop(key, timeout))
        }
        "brpop" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let timeout = UTimeout::try_from(tail[1])?;
            ok!(ListOps::BRPop(key, timeout))
        }
        "rpop" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(ListOps::RPop(key))
        }
        "linsert" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(ListOps::LPop(key))
        }
        "lindex" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let index = Index::try_from(tail[1])?;
            ok!(ListOps::LIndex(key, index))
        }
        "lset" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let index = Index::try_from(tail[1])?;
            let value = Value::try_from(tail[2])?;
            ok!(ListOps::LSet(key, index, value))
        }
        "lrange" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let start_index = Index::try_from(tail[1])?;
            let end_index = Index::try_from(tail[2])?;
            ok!(ListOps::LRange(key, start_index, end_index))
        }
        "ltrim" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let start_index = Index::try_from(tail[1])?;
            let end_index = Index::try_from(tail[2])?;
            ok!(ListOps::LTrim(key, start_index, end_index))
        }
        "rpoplpush" => {
            verify_size(&tail, 2)?;
            let source = Key::try_from(tail[0])?;
            let dest = Key::try_from(tail[1])?;
            ok!(ListOps::RPopLPush(source, dest))
        }
        "hget" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let field = Key::try_from(tail[1])?;
            ok!(HashOps::HGet(key, field))
        }
        "hset" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let field = Key::try_from(tail[1])?;
            let value = Key::try_from(tail[2])?;
            ok!(HashOps::HSet(key, field, value))
        }
        "hsetnx" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let field = Key::try_from(tail[1])?;
            let value = Key::try_from(tail[2])?;
            ok!(HashOps::HSetNX(key, field, value))
        }
        "hmset" => {
            verify_size_lower(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            // let args = tails_as_strings(&tail[1..])?;
            // // TODO: Avoid cloning here
            // let mut key_value_tuples: Vec<(Key, Value)> = Vec::new();
            // for i in args.chunks(2) {
            //     let key_value = (i[0].clone(), i[1].clone());
            //     key_value_tuples.push(key_value);
            // }
            let key_value_tuples = get_key_value_pairs(&tail[1..])?;
            ok!(HashOps::HMSet(key, key_value_tuples))
        }
        "hexists" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let field = Key::try_from(tail[1])?;
            ok!(HashOps::HExists(key, field))
        }
        "hgetall" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(HashOps::HGetAll(key))
        }
        "hmget" => {
            verify_size_lower(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let fields = values_from_tail(&tail[1..])?;
            ok!(HashOps::HMGet(key, fields))
        }
        "hkeys" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(HashOps::HKeys(key))
        }
        "hlen" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(HashOps::HLen(key))
        }
        "hdel" => {
            verify_size_lower(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let fields = values_from_tail(&tail[1..])?;
            ok!(HashOps::HDel(key, fields))
        }
        "hvals" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(HashOps::HVals(key))
        }
        "hstrlen" => {
            // verify_size(&tail, 2)?;
            // let key = Key::try_from(tail[0])?;
            // let field = Key::try_from(tail[1])?;

            // ok!(HashOps::HStrLen(key, field))
            // get_key_and_value
            let (key, field) = get_key_and_value(array)?;
            ok!(HashOps::HStrLen(key, field))
        }
        "hincrby" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let field = Key::try_from(tail[1])?;
            let value = Count::try_from(tail[2])?;
            Ok(Ops::Hashes(HashOps::HIncrBy(key, field, value)))
        }
        // Sorted Sets
        "zadd" => {
            verify_size_lower(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let member_scores = get_key_value_pairs(&tail[1..])?;
            ok!(ZSetOps::ZAdd(key, member_scores))
        }
        "zrem" => {
            verify_size_lower(&tail, 2)?;
            let (key, keys_to_rem) = get_key_and_tail(&array[1..])?;
            ok!(ZSetOps::ZRem(key, keys_to_rem))
        }
        "zrange" => {
            verify_size(&tail, 3)?;
            let key = Key::try_from(tail[0])?;
            let lower = Score::try_from(tail[1])?;
            let upper = Score::try_from(tail[2])?;
            ok!(ZSetOps::ZRange(key, lower, upper))
        }
        "zcard" => {
            verify_size(&tail, 1)?;
            let key = Key::try_from(tail[0])?;
            ok!(ZSetOps::ZCard(key))
        }
        "zscore" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let score = Key::try_from(tail[1])?;
            ok!(ZSetOps::ZScore(key, score))
        }
        "zpopmax" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let count = Count::try_from(tail[1])?;
            ok!(ZSetOps::ZPopMax(key, count))
        }
        "zpopmin" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let count = Count::try_from(tail[1])?;
            ok!(ZSetOps::ZPopMin(key, count))
        }
        "zrank" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let member_key = Key::try_from(tail[1])?;
            ok!(ZSetOps::ZRank(key, member_key))
        }
        "binsert" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let value = Value::try_from(tail[1])?;
            ok!(BloomOps::BInsert(key, value))
        }
        "bcontains" => {
            verify_size(&tail, 2)?;
            let key = Key::try_from(tail[0])?;
            let value = Value::try_from(tail[1])?;
            ok!(BloomOps::BContains(key, value))
        }
        "select" => {
            verify_size(&tail, 1)?;
            let new_db = Index::try_from(tail[0])?;
            ok!(MiscOps::Select(new_db))
        }
        "echo" => {
            verify_size(&tail, 1)?;
            let val = Value::try_from(tail[0])?;
            ok!(MiscOps::Echo(val))
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
