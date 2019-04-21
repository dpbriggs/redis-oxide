// use futures::future::Future;
use std::collections::{HashMap, HashSet, VecDeque};
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
pub enum EngineRes {
    Ok,
    StringRes(Value),
    Error(&'static [u8]),
    MultiStringRes(Vec<Value>),
    UIntRes(usize),
    Nil,
    // FutureRes(Box<EngineRes>, Box<Future<Item = (), Error = ()> + Send>),
    // TODO: Figure out how to get EngineRes out of this.
    // FutureResValue(Box<Future<Item = (), Error = ()> + Send>),
}

impl EngineRes {
    pub fn is_error(&self) -> bool {
        if let EngineRes::Error(_) = *self {
            return true;
        }
        false
    }
}

// impl std::fmt::Debug for EngineRes {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         match self {
//             EngineRes::Ok => write!(f, "EngineRes::Ok"),
//             EngineRes::StringRes(s) => write!(f, "EngineRes::StringRes({:?})", s),
//             EngineRes::Error(e) => write!(f, "EngineRes::Error({:?})", e),
//             EngineRes::MultiStringRes(v) => write!(f, "EngineRes::MultiStringRes({:?})", v),
//             EngineRes::UIntRes(u) => write!(f, "EngineRes::UIntRes({:?})", u),
//             EngineRes::Nil => write!(f, "EngineRes::Nil"),
//             EngineRes::FutureRes(b, _) => write!(f, "{:?} PLUS FUTURE (UNKNOWN)", b),
//             EngineRes::FutureResValue(_) => write!(f, "UNKNOWN FUTURE RES"),
//         }
//     }
// }

type KeyString = HashMap<Key, Value>;
type KeySet = HashMap<Key, HashSet<Value>>;
type KeyList = HashMap<Key, VecDeque<Value>>;

#[derive(Default, Debug, Clone)]
pub struct Engine {
    pub kv: Arc<RwLock<KeyString>>,
    pub sets: Arc<RwLock<KeySet>>,
    pub lists: Arc<RwLock<KeyList>>,
}

// pub trait Exec: Clone + Debug {
//     fn exec(self, engine: Engine) -> EngineRes;
// }
