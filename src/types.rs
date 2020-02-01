use bytes::Bytes;
use dashmap::DashMap;
use growable_bloom_filter::GrowableBloom;
/// Common Types in the project.
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::From;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use parking_lot::Mutex;
use std::fs::File;

use crate::data_structures::receipt_map::RecieptMap;
use crate::data_structures::sorted_set::SortedSet;
use crate::data_structures::stack::Stack;

/// These types are used by state and ops to actually perform useful work.
pub type Value = Bytes;
/// Key is the standard type to index our structures
pub type Key = Bytes;
/// Count is used for commands that count.
pub type Count = i64;
/// Index is used to represent indices in structures.
pub type Index = i64;
/// Score is used in sorted sets
pub type Score = i64;
/// Timeout unit
pub type UTimeout = i64;
/// Bool type
pub type RedisBool = i64;

/// DumpTimeoutUnitpe alias.
pub type DumpFile = Arc<Mutex<File>>;

/// RedisValueRef is the canonical type for values flowing
/// through the system. Inputs are converted into RedisValues,
/// and outputs are converted into RedisValues.
#[derive(PartialEq, Clone)]
pub enum RedisValueRef {
    String(Bytes),
    Error(Bytes),
    ErrorMsg(Vec<u8>),
    Int(i64),
    Array(Vec<RedisValueRef>),
    NullArray,
    NullBulkString,
}

impl std::fmt::Debug for RedisValueRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisValueRef::String(s) => {
                write!(f, "RedisValueRef::String({:?})", String::from_utf8_lossy(s))
            }
            RedisValueRef::Error(s) => {
                write!(f, "RedisValueRef::Error({:?})", String::from_utf8_lossy(s))
            }
            RedisValueRef::ErrorMsg(s) => write!(f, "RedisValueRef::ErrorMsg({:?})", s),

            RedisValueRef::Int(i) => write!(f, "RedisValueRef::Int({:?})", i),
            RedisValueRef::NullBulkString => write!(f, "RedisValueRef::NullBulkString"),
            RedisValueRef::NullArray => write!(f, "RedisValueRef::NullArray"),
            RedisValueRef::Array(arr) => {
                write!(f, "RedisValueRef::Array(")?;
                for item in arr {
                    write!(f, "{:?}", item)?;
                    write!(f, ",")?;
                }
                write!(f, ")")?;
                Ok(())
            }
        }
    }
}

// // TODO: Get rid of this
// impl<'a> From<RedisValueRef> for RedisValueRef {
//     fn from(other: RedisValueRef) -> RedisValueRef {
//         match other {
//             RedisValueRef::String(v) => RedisValueRef::BulkString(v.to_vec()),
//             RedisValueRef::Error(e) => RedisValueRef::Error(e.to_vec()),
//             RedisValueRef::Int(i) => RedisValueRef::Int(i),
//             RedisValueRef::Array(a) => RedisValueRef::Array(a.into_iter().map(|i| i.into()).collect()),
//             RedisValueRef::NullBulkString => RedisValueRef::NullBulkString,
//             RedisValueRef::NullArray => RedisValueRef::NullArray,
//         }
//     }
// }

/// Special constants in the RESP protocol.
pub const NULL_BULK_STRING: &str = "$-1\r\n";
pub const NULL_ARRAY: &str = "*-1\r\n";
pub const EMPTY_ARRAY: &str = "*0\r\n";

/// Convenience type for returns value. Maps directly to RedisValues.
#[derive(Debug, PartialEq, Clone)]
pub enum ReturnValue {
    Ok,
    StringRes(Value),
    Error(&'static [u8]),
    MultiStringRes(Vec<Value>),
    Array(Vec<ReturnValue>),
    IntRes(i64),
    Nil,
}

/// Convenience trait to convert Count to ReturnValue.
impl From<Count> for ReturnValue {
    fn from(int: Count) -> ReturnValue {
        ReturnValue::IntRes(int)
    }
}

/// Convenience trait to convert ReturnValues to ReturnValue.
impl From<Vec<Value>> for ReturnValue {
    fn from(vals: Vec<Value>) -> ReturnValue {
        ReturnValue::Array(vals.into_iter().map(ReturnValue::StringRes).collect())
    }
}

/// Convenience trait to convert ReturnValues to ReturnValue.
impl From<Vec<String>> for ReturnValue {
    fn from(strings: Vec<String>) -> ReturnValue {
        let strings_to_bytes: Vec<Bytes> = strings
            .into_iter()
            .map(|s| s.as_bytes().to_vec().into())
            .collect();
        ReturnValue::MultiStringRes(strings_to_bytes)
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
type KeyString = DashMap<Key, Value>;
/// Canonical type for Key-Set storage.
type KeySet = HashMap<Key, HashSet<Value>>;
/// Canonical type for Key-List storage.
type KeyList = HashMap<Key, VecDeque<Value>>;
/// Canonical type for Key-Hash storage.
type KeyHash = HashMap<Key, HashMap<Key, Value>>;
/// Canonical type for Key-Hash storage.
type KeyZSet = HashMap<Key, SortedSet>;
/// Canonical type for Key-Bloom storage.
type KeyBloom = HashMap<Key, GrowableBloom>;
type KeyStack = HashMap<Key, Stack<Value>>;

/// Top level database struct.
/// Holds all StateRef dbs, and will hand them out on request.
#[derive(Default, Serialize, Deserialize)]
pub struct StateStore {
    pub states: Mutex<HashMap<Index, StateRef>>,
    #[serde(skip)]
    pub commands_ran_since_save: AtomicU64,
    #[serde(skip)]
    pub commands_threshold: u64,
    #[serde(skip)]
    pub memory_only: bool,
}

/// Reference type for `StateStore`
pub type StateStoreRef = Arc<StateStore>;

/// Reference type for `State`
pub type StateRef = Arc<State>;

/// The state stored by redis-oxide. These fields are the ones
/// used by the various datastructure files (keys.rs, etc)
#[derive(Default, Serialize, Deserialize)]
pub struct State {
    #[serde(default)]
    pub kv: KeyString,
    #[serde(default)]
    pub sets: RwLock<KeySet>,
    #[serde(default)]
    pub lists: RwLock<KeyList>,
    #[serde(default)]
    pub hashes: RwLock<KeyHash>,
    #[serde(default)]
    pub zsets: RwLock<KeyZSet>,
    #[serde(default)]
    pub blooms: RwLock<KeyBloom>,
    #[serde(default)]
    pub stacks: RwLock<KeyStack>,
    #[serde(skip)]
    pub reciept_map: Mutex<RecieptMap>,
}

/// Mapping of a ReturnValue to a RedisValueRef.
impl From<ReturnValue> for RedisValueRef {
    fn from(state_res: ReturnValue) -> Self {
        match state_res {
            ReturnValue::Ok => RedisValueRef::String(Bytes::from_static(b"OK")),
            ReturnValue::Nil => RedisValueRef::NullBulkString,
            ReturnValue::StringRes(s) => RedisValueRef::String(s),
            ReturnValue::MultiStringRes(a) => {
                RedisValueRef::Array(a.into_iter().map(RedisValueRef::String).collect())
            }
            ReturnValue::IntRes(i) => RedisValueRef::Int(i as i64),
            ReturnValue::Error(e) => RedisValueRef::Error(Bytes::from_static(e)),
            ReturnValue::Array(a) => {
                RedisValueRef::Array(a.into_iter().map(RedisValueRef::from).collect())
            }
        }
    }
}
