// use futures::future::Future;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::From;
use std::sync::{Arc, RwLock};

/// These types are used by engine and ops to actually perform useful work.
pub type Value = Vec<u8>;
/// Key is the standard type to index our structures
pub type Key = Vec<u8>;
/// Count is used for commands that count.
pub type Count = i64;
pub type Index = i64;

#[allow(dead_code)]
#[derive(Debug, PartialEq, Clone)]
pub enum RedisValue {
    SimpleString(Value),
    Error(Value),
    BulkString(Value),
    Int(i64), // is it always i64?
    Array(Vec<RedisValue>),
    NullArray,
    NullBulkString,
}

pub const NULL_BULK_STRING: &str = "$-1\r\n";
pub const NULL_ARRAY: &str = "*-1\r\n";
pub const EMPTY_ARRAY: &str = "*0\r\n";

#[derive(Debug, PartialEq)]
pub enum InteractionRes {
    Ok,
    StringRes(Value),
    Error(&'static [u8]),
    MultiStringRes(Vec<Value>),
    UIntRes(usize),
    Nil,
    // TODO: Figure out how to get the futures working properly.
    // FutureRes(Box<InteractionRes>, Box<Future<Item = (), Error = ()> + Send>),
    // FutureResValue(Box<Future<Item = (), Error = ()> + Send>),
}

impl InteractionRes {
    pub fn is_error(&self) -> bool {
        if let InteractionRes::Error(_) = *self {
            return true;
        }
        false
    }
}

type KeyString = HashMap<Key, Value>;
type KeySet = HashMap<Key, HashSet<Value>>;
type KeyList = HashMap<Key, VecDeque<Value>>;

#[derive(Default, Debug, Clone)]
pub struct State {
    pub kv: Arc<RwLock<KeyString>>,
    pub sets: Arc<RwLock<KeySet>>,
    pub lists: Arc<RwLock<KeyList>>,
}

#[derive(Serialize, Deserialize)]
pub struct Database {
    pub kv: Vec<u8>,
    pub sets: Vec<u8>,
    pub lists: Vec<u8>,
}

impl From<InteractionRes> for RedisValue {
    fn from(engine_res: InteractionRes) -> Self {
        match engine_res {
            InteractionRes::Ok => RedisValue::SimpleString(vec![b'O', b'K']),
            InteractionRes::Nil => RedisValue::NullBulkString,
            InteractionRes::StringRes(s) => RedisValue::BulkString(s),
            InteractionRes::MultiStringRes(a) => RedisValue::Array(
                a.iter()
                    .map(|s| RedisValue::BulkString(s.to_vec()))
                    .collect(),
            ),
            InteractionRes::UIntRes(i) => RedisValue::Int(i as i64),
            InteractionRes::Error(e) => RedisValue::Error(e.to_vec()),
            // InteractionRes::FutureRes(s, _) => RedisValue::from(*s),
            // InteractionRes::FutureResValue(_) => unreachable!(),
        }
    }
}

pub trait StateInteration {
    fn interact(self, engine: State) -> InteractionRes;
}
