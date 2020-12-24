use crate::server::process_command;
use num_traits::cast::ToPrimitive;
use parking_lot::Mutex;
use std::{error::Error, sync::Arc};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex as TokioMutex;

use crate::types::DumpFile;
use crate::types::RedisValueRef;
use crate::{logger::LOGGER, types::StateStoreRef};
use x7::ffi::{ForeignData, X7Interpreter};
use x7::symbols::Expr;

fn bytes_to_string(s: &[u8]) -> String {
    String::from_utf8_lossy(s.as_ref()).to_string()
}

#[derive(Debug)]
struct FFIError {
    reason: String,
}

impl FFIError {
    fn new(reason: String) -> Box<dyn Error + Send> {
        Box::new(Self { reason })
    }
}

impl std::fmt::Display for FFIError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl Error for FFIError {}

impl ForeignData for RedisValueRef {
    fn to_x7(&self) -> Result<Expr, Box<dyn std::error::Error + Send>> {
        let res = match self {
            RedisValueRef::BulkString(s) | RedisValueRef::SimpleString(s) => {
                Expr::String(bytes_to_string(s))
            }
            RedisValueRef::Error(e) => {
                return Err(FFIError::new(bytes_to_string(e)));
            }
            RedisValueRef::ErrorMsg(e) => {
                return Err(FFIError::new(bytes_to_string(e)));
            }
            RedisValueRef::Int(i) => Expr::Num((*i).into()),
            RedisValueRef::Array(a) => Expr::Tuple(
                a.into_iter()
                    .map(|ele| ele.to_x7())
                    .collect::<Result<_, _>>()?,
            ),
            RedisValueRef::NullArray | RedisValueRef::NullBulkString => Expr::Nil,
        };
        Ok(res)
    }

    fn from_x7(expr: &Expr) -> Result<Self, Box<dyn std::error::Error + Send>> {
        let res = match expr {
            Expr::Nil => RedisValueRef::NullArray,
            Expr::Num(n) => RedisValueRef::Int(
                n.to_i64()
                    .ok_or_else(|| FFIError::new(format!("Failed to convert {} into an i64", n)))?,
            ),
            Expr::Symbol(s) | Expr::String(s) => RedisValueRef::BulkString(s.clone().into()),
            Expr::List(l) | Expr::Tuple(l) | Expr::Quote(l) => RedisValueRef::Array(
                l.iter()
                    .map(|e| ForeignData::from_x7(e))
                    .collect::<Result<_, _>>()?,
            ),
            Expr::Bool(b) => RedisValueRef::BulkString(format!("{}", b).into()),
            bad_type @ _ => {
                return Err(FFIError::new(format!(
                    "redis-oxide cannot reason about this type: {:?}",
                    bad_type
                )))
            }
        };
        Ok(res)
    }
}

pub struct ScriptingBridge {
    script_lock: TokioMutex<()>,
    prog_send: Sender<String>,
    prog_recv: TokioMutex<Receiver<Result<RedisValueRef, Box<dyn Error + Send>>>>,
}

impl ScriptingBridge {
    pub fn new(
        prog_send: Sender<String>,
        prog_recv: Receiver<Result<RedisValueRef, Box<dyn Error + Send>>>,
    ) -> Arc<Self> {
        let sb = Self {
            script_lock: TokioMutex::new(()),
            prog_send,
            prog_recv: TokioMutex::new(prog_recv),
        };
        Arc::new(sb)
    }

    pub async fn handle_script_cmd(&self, cmd: String) -> RedisValueRef {
        self.script_lock.lock().await;
        if let Err(e) = self.prog_send.send(cmd).await {
            error!(LOGGER, "Failed to send program: {}", e);
        }
        match self.prog_recv.lock().await.recv().await {
            Some(res) => match res {
                Ok(r) => r,
                Err(e) => RedisValueRef::Error(format!("{}", e).into()),
            },
            None => RedisValueRef::Error("Failed to receive a response from x7".into()),
        }
    }
}

pub async fn handle_redis_cmd(
    mut cmd_recv: Receiver<Vec<RedisValueRef>>,
    cmd_res_send: Sender<RedisValueRef>,
    state_store: StateStoreRef,
    dump_file: DumpFile,
    scripting_engine: Arc<ScriptingBridge>,
) {
    // TODO: Support, or return an error when interacting with
    // change db commands
    let mut state = state_store.get_default();
    while let Some(cmd) = cmd_recv.recv().await {
        debug!(LOGGER, "Recieved redis command: {:?}", cmd);
        let res = process_command(
            &mut state,
            state_store.clone(),
            dump_file.clone(),
            scripting_engine.clone(),
            RedisValueRef::Array(cmd),
        )
        .await;
        if let Err(e) = cmd_res_send.send(res).await {
            error!(LOGGER, "Failed to write response! {}", e);
        }
    }
}

pub struct ScriptingEngine {
    interpreter: X7Interpreter,
    prog_revc: Receiver<String>,
    prog_send: Sender<Result<RedisValueRef, Box<dyn Error + Send>>>,
    cmd_recv: Arc<Mutex<Receiver<RedisValueRef>>>,
    cmd_send: Arc<Sender<Vec<RedisValueRef>>>,
}

impl ScriptingEngine {
    pub fn new(
        prog_revc: Receiver<String>,
        prog_send: Sender<Result<RedisValueRef, Box<dyn Error + Send>>>,
        cmd_recv: Receiver<RedisValueRef>,
        cmd_send: Sender<Vec<RedisValueRef>>,
    ) -> Self {
        let res = Self {
            interpreter: X7Interpreter::new(),
            prog_revc,
            prog_send,
            cmd_recv: Arc::new(Mutex::new(cmd_recv)),
            cmd_send: Arc::new(cmd_send),
        };
        res.setup_interpreter();
        res
    }

    pub fn main_loop(mut self) {
        loop {
            if let Some(program) = self.prog_revc.blocking_recv() {
                debug!(LOGGER, "Recieved this program: {}", program);
                let res = self.interpreter.run_program::<RedisValueRef>(&program);
                if let Err(e) = self.prog_send.blocking_send(res) {
                    eprintln!("Failed to return program result! {}", e);
                }
            }
        }
    }

    fn setup_interpreter(&self) {
        let send_clone = self.cmd_send.clone();
        let recv_clone = self.cmd_recv.clone();
        let send_fn = move |args: Vec<RedisValueRef>| {
            if let Err(e) = send_clone.blocking_send(args) {
                return Err(FFIError::new(format!(
                    "redis-oxide failed to send the command: {}",
                    e
                )));
            }
            recv_clone.lock().blocking_recv().ok_or_else(|| {
                FFIError::new("redis-oxide failed to recv the response for the command!".into())
            })
        };
        self.interpreter
            .add_function_test("redis", 1, Arc::new(send_fn));
    }
    // fn spawn(self) {}
}

// async fn start_interpreter(state_ref: StateRef) {
//     let interpreter = X7Interpreter::new();
//     let state_ref_clone = state_ref.clone();
//     let fn_call = move |args: Vec<RedisValueRef>| {
//         translate(RedisValueRef::Array(args)).map(|op| op_interact(op, state_ref_clone));
//         Ok(RedisValueRef::NullArray)
//     };
//     interpreter.add_function_test("redis", 1, Arc::new(fn_call));
// }
