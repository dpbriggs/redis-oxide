/// Common Types in the project.
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
/// Index is used to represent indices in structures.
pub type Index = i64;

/// DumpFile type alias.
pub type DumpFile = Arc<Mutex<File>>;

/// RedisValue is the canonical type for values flowing
/// through the system. Inputs are converted into RedisValues,
/// and outputs are converted into RedisValues.
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

/// Special constants in the RESP protocol.
pub const NULL_BULK_STRING: &str = "$-1\r\n";
pub const NULL_ARRAY: &str = "*-1\r\n";
pub const EMPTY_ARRAY: &str = "*0\r\n";

/// Convenience type for returns value. Maps directly to RedisValues.
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

/// Convenience trait to convert ReturnValues to InteractionRes.
impl From<ReturnValue> for InteractionRes {
    fn from(int: ReturnValue) -> InteractionRes {
        InteractionRes::Immediate(int)
    }
}

/// Highest level output type. This is the return value returned by operations.
pub enum InteractionRes {
    Immediate(ReturnValue),
    #[allow(dead_code)]
    ImmediateWithWork(ReturnValue, Box<Future<Item = (), Error = ()> + Send>),
    Blocking(Box<Future<Item = ReturnValue, Error = ()> + Send>),
}

/// Debug impl for InteractionRes; used by debug logs.
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

/// Convenience method to determine an error. Used in testing.
impl ReturnValue {
    pub fn is_error(&self) -> bool {
        if let ReturnValue::Error(_) = *self {
            return true;
        }
        false
    }
}

/// Canonical type for Key-Value storage.
type KeyString = HashMap<Key, Value>;
/// Canonical type for Key-Set storage.
type KeySet = HashMap<Key, HashSet<Value>>;
/// Canonical type for Key-List storage.
type KeyList = HashMap<Key, VecDeque<Value>>;
/// Canonical type for Key-Hash storage.
type KeyHash = HashMap<Key, HashMap<Key, Value>>;

/// The state stored by redis-oxide. These fields are the ones
/// used by the various datastructure files (keys.rs, etc)
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub kv: Arc<RwLock<KeyString>>,
    pub sets: Arc<RwLock<KeySet>>,
    pub lists: Arc<RwLock<KeyList>>,
    pub hashes: Arc<RwLock<KeyHash>>,
}

/// Mapping of a ReturnValue to a RedisValue.
impl From<ReturnValue> for RedisValue {
    fn from(state_res: ReturnValue) -> Self {
        match state_res {
            ReturnValue::Ok => RedisValue::SimpleString(vec![b'O', b'K']),
            ReturnValue::Nil => RedisValue::NullBulkString,
            ReturnValue::StringRes(s) => RedisValue::BulkString(s),
            ReturnValue::MultiStringRes(a) => {
                RedisValue::Array(a.into_iter().map(RedisValue::BulkString).collect())
            }
            ReturnValue::IntRes(i) => RedisValue::Int(i as i64),
            ReturnValue::Error(e) => RedisValue::Error(e.to_vec()),
            ReturnValue::Array(a) => {
                RedisValue::Array(a.into_iter().map(RedisValue::from).collect())
            }
        }
    }
}

/// StateInteration is how Operations interact with State.
pub trait StateInteration {
    fn interact(self, state: State) -> InteractionRes;
}
