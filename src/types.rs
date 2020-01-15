use growable_bloom_filter::GrowableBloom;
/// Common Types in the project.
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::From;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use parking_lot::Mutex;
use std::fs::File;

use crate::data_structures::concurrent_hashmap::ConcurrentHashMap;
use crate::data_structures::receipt_map::RecieptMap;
use crate::data_structures::sorted_set::SortedSet;
use crate::data_structures::stack::Stack;

/// These types are used by state and ops to actually perform useful work.
pub type Value = Vec<u8>;
/// Key is the standard type to index our structures
pub type Key = Vec<u8>;
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
#[derive(Debug, PartialEq, Clone)]
pub enum ReturnValue {
    Ok,
    StringRes(Value),
    Error(&'static [u8]),
    MultiStringRes(Vec<Value>),
    Array(Vec<ReturnValue>),
    IntRes(i64),
    Nil,
    BadType,
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
        let strings_to_bytes: Vec<Vec<u8>> =
            strings.into_iter().map(|s| s.as_bytes().to_vec()).collect();
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
type KeyString = HashMap<Key, Value>;
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
#[derive(Default, Debug, Serialize, Deserialize)]
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
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct State {
    #[serde(default)]
    pub kv: RwLock<KeyString>,
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
    #[serde(default)]
    pub concurrent_kv: ConcurrentHashMap,
    #[serde(skip)]
    pub reciept_map: Mutex<RecieptMap>,
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
            ReturnValue::BadType => RedisValue::Error("ERR: Bad Type".as_bytes().to_vec()),
        }
    }
}
