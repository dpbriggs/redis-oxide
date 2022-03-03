use crate::server::process_command;
use num_traits::cast::ToPrimitive;
use std::{error::Error, sync::Arc};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::startup::Config;
use crate::types::DumpFile;
use crate::types::RedisValueRef;
use crate::{logger::LOGGER, types::StateStoreRef};
use x7::ffi::{ForeignData, IntoX7Function, Variadic, X7Interpreter};
use x7::symbols::Expr;

fn bytes_to_string(s: &[u8]) -> String {
    String::from_utf8_lossy(s).to_string()
}

struct FFIError {
    reason: String,
}

impl FFIError {
    fn boxed(reason: String) -> Box<dyn Error + Send> {
        Box::new(Self { reason })
    }
}

impl std::fmt::Debug for FFIError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.reason)
    }
}

impl std::fmt::Display for FFIError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.reason)
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
                return Err(FFIError::boxed(bytes_to_string(e)));
            }
            RedisValueRef::ErrorMsg(e) => {
                return Err(FFIError::boxed(bytes_to_string(e)));
            }
            RedisValueRef::Int(i) => Expr::Integer(*i),
            RedisValueRef::Array(a) => {
                Expr::Tuple(a.iter().map(|ele| ele.to_x7()).collect::<Result<_, _>>()?)
            }
            RedisValueRef::NullArray | RedisValueRef::NullBulkString => Expr::Nil,
        };
        Ok(res)
    }

    fn from_x7(expr: &Expr) -> Result<Self, Box<dyn std::error::Error + Send>> {
        let res =
            match expr {
                Expr::Nil => RedisValueRef::NullArray,
                Expr::Num(n) => RedisValueRef::Int(n.to_i64().ok_or_else(|| {
                    FFIError::boxed(format!("Failed to convert {} into an i64", n))
                })?),
                Expr::Integer(n) => RedisValueRef::Int(*n),
                Expr::String(s) => RedisValueRef::BulkString(s.clone().into()),
                Expr::Symbol(s) => RedisValueRef::BulkString(s.read().into()),
                Expr::List(l) | Expr::Tuple(l) | Expr::Quote(l) => RedisValueRef::Array(
                    l.iter()
                        .map(ForeignData::from_x7)
                        .collect::<Result<_, _>>()?,
                ),
                Expr::Bool(b) => RedisValueRef::BulkString(format!("{}", b).into()),
                bad_type => {
                    return Err(FFIError::boxed(format!(
                        "redis-oxide cannot reason about this type: {:?}",
                        bad_type
                    )))
                }
            };
        Ok(res)
    }
}

#[allow(clippy::type_complexity)]
pub struct ScriptingBridge {
    prog_send: Sender<(
        Program,
        OneShotSender<Result<RedisValueRef, Box<dyn Error + Send>>>,
    )>,
}

impl ScriptingBridge {
    #[allow(clippy::type_complexity)]
    pub fn new(
        prog_send: Sender<(
            Program,
            OneShotSender<Result<RedisValueRef, Box<dyn Error + Send>>>,
        )>,
    ) -> Arc<Self> {
        let sb = Self { prog_send };
        Arc::new(sb)
    }

    pub async fn handle_script_cmd(&self, cmd: Program) -> RedisValueRef {
        let (sx, rx) = oneshot_channel();
        if let Err(e) = self.prog_send.send((cmd, sx)).await {
            error!(LOGGER, "Failed to send program: {}", e);
        }
        match rx.await {
            Ok(x7_result) => match x7_result {
                Ok(r) => r,
                Err(e) => RedisValueRef::Error(format!("{}", e).into()),
            },
            Err(e) => {
                RedisValueRef::Error(format!("Failed to receive a response from x7 {}", e).into())
            }
        }
    }
}

use tokio::sync::oneshot::{channel as oneshot_channel, Sender as OneShotSender};

use tokio::sync::oneshot::error::TryRecvError;
pub async fn handle_redis_cmd(
    mut cmd_recv: Receiver<(Vec<RedisValueRef>, OneShotSender<RedisValueRef>)>,
    state_store: StateStoreRef,
    dump_file: DumpFile,
    scripting_engine: Arc<ScriptingBridge>,
) {
    // TODO: Support, or return an error when interacting with
    // change db commands
    let mut state = state_store.get_default();
    while let Some((cmd, return_channel)) = cmd_recv.recv().await {
        debug!(LOGGER, "Recieved redis command: {:?}", cmd);
        let res = process_command(
            &mut state,
            state_store.clone(),
            dump_file.clone(),
            scripting_engine.clone(),
            RedisValueRef::Array(cmd),
        )
        .await;
        if let Err(e) = return_channel.send(res) {
            error!(LOGGER, "Failed to write response! {:?}", e);
        }
    }
}

#[derive(Debug)]
pub enum Program {
    String(String),
    Function(String, Vec<RedisValueRef>),
}

pub struct ScriptingEngine {
    interpreter: X7Interpreter,
    #[allow(clippy::type_complexity)]
    prog_revc: Receiver<(
        Program,
        OneShotSender<Result<RedisValueRef, Box<dyn Error + Send>>>,
    )>,
    // prog_send: Sender<Result<RedisValueRef, Box<dyn Error + Send>>>,
    cmd_send: Arc<Sender<(Vec<RedisValueRef>, OneShotSender<RedisValueRef>)>>,
}

impl ScriptingEngine {
    #[allow(clippy::type_complexity)]
    pub fn new(
        prog_revc: Receiver<(
            Program,
            OneShotSender<Result<RedisValueRef, Box<dyn Error + Send>>>,
        )>,
        cmd_send: Sender<(Vec<RedisValueRef>, OneShotSender<RedisValueRef>)>,
        state_store: StateStoreRef,
        opts: &Config,
    ) -> Result<Self, Box<dyn Error>> {
        let res = Self {
            interpreter: X7Interpreter::new(),
            prog_revc,
            cmd_send: Arc::new(cmd_send),
        };
        res.setup_interpreter(state_store);
        res.load_scripts_dir(opts)?;
        Ok(res)
    }

    pub fn main_loop(mut self) {
        loop {
            if let Some((program, return_channel)) = self.prog_revc.blocking_recv() {
                debug!(LOGGER, "Recieved this program: {:?}", program);
                self.spawn_handling_thread(program, return_channel);
            }
        }
    }

    fn load_scripts_dir(&self, opts: &Config) -> Result<(), Box<dyn Error>> {
        if let Some(path) = &opts.scripts_dir {
            info!(LOGGER, "Loading scripts in {:?}", path);
            self.interpreter.load_lib_dir(path)
        } else {
            Ok(())
        }
    }

    fn add_redis_fn(&self) {
        let send_clone = self.cmd_send.clone();
        let send_fn = move |args: Variadic<RedisValueRef>| {
            let args = args.into_vec();
            let (sx, mut rx) = oneshot_channel();
            if let Err(e) = send_clone.blocking_send((args, sx)) {
                return Err(FFIError::boxed(format!(
                    "redis-oxide failed to send the command: {}",
                    e
                )));
            }
            loop {
                match rx.try_recv() {
                    Ok(ret_value) => return Ok(ret_value),
                    Err(TryRecvError::Empty) => continue,
                    Err(TryRecvError::Closed) => {
                        return Err(FFIError::boxed(
                            "redix-oxide failed to return a value!".into(),
                        ))
                    }
                }
            }
        };
        self.interpreter.add_function("redis", send_fn.to_x7_fn());
    }

    /// Add the "def-redis-fn" function to the interpreter
    ///
    /// e.g. script '(def-redis-fn my-sum (a b) (+ a b))'
    /// >>> my-sum "hello " world
    /// "hello world"
    fn embed_foreign_script(&self, state_store: StateStoreRef) {
        // (def-redis-fn my-sum (a b) (+ a b))
        let interpreter_clone = self.interpreter.clone();
        let f = move |args: Variadic<Expr>| {
            let args = args.into_vec();
            let fn_name = match args[0].get_symbol_string() {
                Ok(sym) => sym,
                Err(e) => return Err(e),
            };
            let f_args = args[1].clone(); // (arg1 arg2)
            let f_body = args[2].clone(); // (redis "set" arg1 arg2)
            let res = interpreter_clone.add_dynamic_function(&fn_name, f_args, f_body);
            if res.is_ok() {
                state_store.add_foreign_function(&fn_name.read());
            }
            res
        };
        self.interpreter
            .add_unevaled_function("def-redis-fn", f.to_x7_fn());
    }

    fn setup_interpreter(&self, state_store: StateStoreRef) {
        // "redis"
        self.add_redis_fn();
        // "def-redis-fn"
        self.embed_foreign_script(state_store);
    }

    fn spawn_handling_thread(
        &self,
        program: Program,
        return_channel: OneShotSender<Result<RedisValueRef, Box<dyn Error + Send>>>,
    ) {
        let interpreter = self.interpreter.clone();
        std::thread::spawn(move || {
            let res = match program {
                Program::String(s) => interpreter.run_program::<RedisValueRef>(&s),
                Program::Function(fn_name, fn_args) => interpreter.run_function(&fn_name, &fn_args),
            };
            if let Err(e) = return_channel.send(res) {
                error!(LOGGER, "Failed to send program result! {:?}", e)
            }
        });
    }
}
