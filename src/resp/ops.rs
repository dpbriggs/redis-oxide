use crate::resp::resp::RedisValue;

pub type Key = String;

#[derive(Debug)]
pub enum Ops {
    // Key Value
    Set(Key, String),
    Get(Key),
    // Sets
    SAdd(Key, Vec<String>),
    SRem(Key, Vec<String>),
    SMembers(Key),
    SCard(Key),
    SDiff(Vec<String>),
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
}

fn translate_string(start: &String) -> Result<Ops, OpsError> {
    match start.to_lowercase().as_ref() {
        "ping" => Ok(Ops::Pong),
        "keys" => Ok(Ops::Keys),
        _ => Err(OpsError::UnknownOp),
    }
}

fn is_string_type(r: &RedisValue) -> bool {
    match r {
        RedisValue::SimpleString(_) => true,
        RedisValue::BulkString(_) => true,
        _ => false,
    }
}

fn all_strings(v: &Vec<&RedisValue>) -> bool {
    v.iter().fold(true, |acc, x| match x {
        RedisValue::SimpleString(_) => true && acc,
        RedisValue::BulkString(_) => true && acc,
        _ => false && acc,
    })
}

fn tails_as_strings(tail: Vec<&RedisValue>) -> Result<Vec<String>, OpsError> {
    if !all_strings(&tail) {
        return Err(OpsError::InvalidType);
    }
    let keys: Vec<String> = tail.iter().map(|x| x.get_string_inner()).collect();
    Ok(keys)
}

fn verify_size_lower(v: &Vec<&RedisValue>, min_size: usize) -> Result<(), OpsError> {
    if v.len() < min_size {
        return Err(OpsError::NotEnoughArgs(min_size));
    }
    Ok(())
}

fn verify_size(v: &Vec<&RedisValue>, size: usize) -> Result<(), OpsError> {
    if v.len() != size {
        return Err(OpsError::WrongNumberOfArgs(size));
    }
    Ok(())
}

fn get_key_and_val(array: &Vec<RedisValue>) -> Result<(Key, String), OpsError> {
    if array.len() < 3 {
        return Err(OpsError::WrongNumberOfArgs(3));
    }
    let key = array.get(1).unwrap();
    let val = array.get(2).unwrap();
    Ok((key.get_string_inner(), val.get_string_inner())) // TODO: Verify types required
}

fn get_key_and_tail(array: &Vec<RedisValue>) -> Result<(Key, Vec<String>), OpsError> {
    if array.len() < 3 {
        return Err(OpsError::WrongNumberOfArgs(3));
    }
    let set_key = array.get(1).unwrap().get_string_inner();
    let vals = tails_as_strings(array.iter().skip(2).collect())?;
    Ok((set_key, vals))
}

fn translate_array(array: &Vec<RedisValue>) -> Result<Ops, OpsError> {
    if array.is_empty() {
        return Err(OpsError::Noop);
    }
    let head: &String = {
        match array.first().unwrap() {
            RedisValue::SimpleString(s) => Ok(s),
            RedisValue::BulkString(s) => Ok(s),
            _ => Err(OpsError::InvalidStart),
        }
    }?;
    if let Ok(op) = translate_string(head) {
        return Ok(op);
    }
    let tail: Vec<&RedisValue> = array.iter().skip(1).collect();
    match head.to_lowercase().as_ref() {
        // Key-Value
        "set" => {
            let (key, val) = get_key_and_val(array)?;
            Ok(Ops::Set(key, val))
        }
        "get" => {
            verify_size(&tail, 1)?;
            let key = tail.get(0).unwrap().get_string_inner();
            Ok(Ops::Get(key))
        }
        "exists" => {
            verify_size_lower(&tail, 1)?;
            let keys = tails_as_strings(tail)?;
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
            let set_key = tail.get(0).unwrap().get_string_inner();
            Ok(Ops::SMembers(set_key))
        }
        "scard" => {
            verify_size(&tail, 1)?;
            let key = tail.get(0).unwrap().get_string_inner();
            Ok(Ops::SCard(key))
        }
        "sdiff" => {
            verify_size_lower(&tail, 2)?;
            let keys = tails_as_strings(tail)?;
            Ok(Ops::SDiff(keys))
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
