// use futures::future::Future;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::From;
use std::sync::Arc;
use tokio::prelude::*;

use parking_lot::Mutex;
use std::fs::File;

/// These types are used by state and ops to actually perform useful work.
pub type Value = Vec<u8>;
/// Key is the standard type to index our structures
pub type Key = Vec<u8>;
/// Count is used for commands that count.
pub type Count = i64;
pub type Index = i64;

pub type DumpFile = Arc<Mutex<File>>;

#[allow(dead_code)]
#[derive(Debug, PartialEq, Clone)]
pub enum RedisValue {
    SimpleString(Value),
    Error(Value),
    BulkString(Value),
    Int(i64),
    Array(Vec<RedisValue>),
    NullArray,
    NullBulkString,
}

pub const NULL_BULK_STRING: &str = "$-1\r\n";
pub const NULL_ARRAY: &str = "*-1\r\n";
pub const EMPTY_ARRAY: &str = "*0\r\n";

#[derive(Debug, PartialEq)]
pub enum ReturnValue {
    Ok,
    StringRes(Value),
    Error(&'static [u8]),
    MultiStringRes(Vec<Value>),
    Array(Vec<ReturnValue>),
    IntRes(i64),
    Nil,
    // TODO: Figure out how to get the futures working properly.
    // FutureRes(Box<ReturnValue>, Box<Future<Item = (), Error = ()> + Send>),
    // FutureResValue(Box<Future<Item = (), Error = ()> + Send>),
}

impl From<ReturnValue> for InteractionRes {
    fn from(int: ReturnValue) -> InteractionRes {
        InteractionRes::Immediate(int)
    }
}

pub enum InteractionRes {
    Immediate(ReturnValue),
    ImmediateWithWork(ReturnValue, Box<Future<Item = (), Error = ()> + Send>),
    Blocking(Box<Future<Item = ReturnValue, Error = ()> + Send>),
}

impl std::fmt::Debug for InteractionRes {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            InteractionRes::Immediate(v) => write!(f, "{:?}", v),
            InteractionRes::ImmediateWithWork(v, _) => {
                write!(f, "ImmediateWithWork({:?}, Box<Future>)", v)
            }
            InteractionRes::Blocking(_) => write!(f, "foobar"),
        }
    }
}

impl ReturnValue {
    pub fn is_error(&self) -> bool {
        if let ReturnValue::Error(_) = *self {
            return true;
        }
        false
    }
}

type KeyString = HashMap<Key, Value>;
type KeySet = HashMap<Key, HashSet<Value>>;
type KeyList = HashMap<Key, VecDeque<Value>>;
type KeyHash = HashMap<Key, HashMap<Key, Value>>;

#[derive(Default, Debug, Clone)]
pub struct State {
    pub kv: Arc<RwLock<KeyString>>,
    pub sets: Arc<RwLock<KeySet>>,
    pub lists: Arc<RwLock<KeyList>>,
    pub hashes: Arc<RwLock<KeyHash>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Database {
    pub kv: Vec<u8>,
    pub sets: Vec<u8>,
    pub lists: Vec<u8>,
    pub hashes: Vec<u8>,
}

impl From<ReturnValue> for RedisValue {
    fn from(state_res: ReturnValue) -> Self {
        match state_res {
            ReturnValue::Ok => RedisValue::SimpleString(vec![b'O', b'K']),
            ReturnValue::Nil => RedisValue::NullBulkString,
            ReturnValue::StringRes(s) => RedisValue::BulkString(s),
            ReturnValue::MultiStringRes(a) => RedisValue::Array(
                a.into_iter()
                    .map(RedisValue::BulkString)
                    .collect(),
            ),
            ReturnValue::IntRes(i) => RedisValue::Int(i as i64),
            ReturnValue::Error(e) => RedisValue::Error(e.to_vec()),
            ReturnValue::Array(a) => {
                RedisValue::Array(a.into_iter().map(RedisValue::from).collect())
            }
            // ReturnValue::FutureRes(s, _) => RedisValue::from(*s),
            // ReturnValue::FutureResValue(_) => unreachable!(),
        }
    }
}

pub trait StateInteration {
    fn interact(self, state: State) -> InteractionRes;
}
