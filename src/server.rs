/// Server launch file. Starts the services to make redis-oxide work.
use crate::asyncresp::RedisValueCodec;
use crate::database::{save_state, save_state_interval};
use crate::logger::LOGGER;
use crate::{
    ops::translate,
    startup::Config,
    types::{DumpFile, InteractionRes, RedisValue, State},
};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_codec::Decoder;

fn save_if_required(state: State, dump_file: DumpFile) {
    state.commands_ran.fetch_add(1, Ordering::SeqCst);
    let should_save = state.commands_ran.compare_exchange(
        state.commands_threshold,
        0,
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    if should_save.is_ok() {
        // Keeping these in an outer scope appeases the compiler,
        // as it cannot determine if lifetimes are long enough.
        // It's pretty cheap to clone anyway -- they're just arcs.
        let state_clone = state.clone();
        let dump_file_clone = dump_file.clone();
        tokio::spawn(async {
            save_state(state_clone, dump_file_clone);
        });
    }
}


async fn process(socket: TcpStream, state: State, dump_file: DumpFile) {
    tokio::spawn(async move {
        // let mut framed = RedisValueCodec::default().framed(socket);
        let (mut tx, mut rx) = RedisValueCodec::default().framed(socket).split();
        while let Some(redis_value) = rx.next().await {
            if let Err(e) = redis_value {
                error!(LOGGER, "Error recieving redis value {:?}", e);
                continue
            }
            let res = match translate(&redis_value.unwrap()) {
                Ok(op) => {
                    debug!(LOGGER, "running op {:?}", op.clone());
                    // Step 1: Execute the operation the operation (from translate above)
                    let res = match state.clone().exec_op(op) {
                        InteractionRes::Immediate(r) => RedisValue::from(r),
                        InteractionRes::ImmediateWithWork(r, w) => {
                            tokio::spawn(w);
                            RedisValue::from(r)
                        }
                        InteractionRes::Blocking(w) => {
                            // TODO: Use actual await when tokio works with latest nightly
                            // XXX: This kills the server.
                            // let r = w.wait().unwrap();
                            // Ok(RedisValue::from(r))
                            if let InteractionRes::Immediate(r) = w.await.into() {
                                RedisValue::from(r)
                            } else {
                                unreachable!();
                            }
                        }
                    };
                    // Step 2: Update commands_ran counter, and save if necessary
                    // Atomics for saving state. Add 1, and then compare with state.commands_threshold.
                    save_if_required(state.clone(), dump_file.clone());
                    // Step 3: Finally Return
                    res
                }
                Err(e) => RedisValue::from(e),
            };
            if let Err(e) = tx.send(res).await {
                error!(LOGGER, "Failed to send data to client! {:?}", e)
            };
        }
    });
}

/// Process a single socket; one of these tasks per tcp accept.
// fn process(socket: TcpStream, state: State, dump_file: DumpFile) {
//     // tx -- the sender; accepts RedisValues and send them back to the client
//     // rx -- the receiver; accepts bytes and creates RedisValues.
//     let (tx, rx) = RedisValueCodec::default().framed(socket).split();
//     // Map all requests into responses and send them back to the client.
//     tx.await;
//     tx.poll_read();
//     info!(LOGGER, "accepting new connection...");
//     let task = tx
//         .send_all(rx.and_then(move |r: RedisValue| match translate(&r) {
//             Ok(op) => {
//                 debug!(LOGGER, "running op {:?}", op.clone());
//                 // Step 1: Execute the operation the operation (from translate above)
//                 let res = match state.clone().exec_op(op) {
//                     InteractionRes::Immediate(r) => Ok(RedisValue::from(r)),
//                     InteractionRes::ImmediateWithWork(r, w) => {
//                         tokio::spawn(w);
//                         Ok(RedisValue::from(r))
//                     }
//                     InteractionRes::Blocking(_w) => {
//                         // TODO: Use actual await when tokio works with latest nightly
//                         // XXX: This kills the server.
//                         // let r = w.wait().unwrap();
//                         // Ok(RedisValue::from(r))
//                         Ok(RedisValue::NullBulkString)
//                     }
//                 };
//                 // Step 2: Update commands_ran counter, and save if necessary
//                 // Atomics for saving state. Add 1, and then compare with state.commands_threshold.
//                 state.commands_ran.fetch_add(1, Ordering::SeqCst);
//                 let should_save = state.commands_ran.compare_exchange(
//                     state.commands_threshold,
//                     0,
//                     Ordering::SeqCst,
//                     Ordering::SeqCst,
//                 );
//                 if should_save.is_ok() {
//                     // Keeping these in an outer scope appeases the compiler,
//                     // as it cannot determine if lifetimes are long enough.
//                     // It's pretty cheap to clone anyway -- they're just arcs.
//                     let state_clone = state.clone();
//                     let dump_file_clone = dump_file.clone();
//                     // tokio::spawn(async || {
//                     //     save_state(state_clone, dump_file_clone);
//                     // });
//                 }
//                 // Step 3: Finally Return
//                 res
//             }
//             Err(e) => Ok(RedisValue::from(e)),
//         }));
//     tokio::spawn(task);
// }

/// The listener for redis-oxide. Accepts connections and spawns handlers.
async fn socket_listener(
    state: State,
    dump_file: DumpFile,
    config: Config,
) {
    // First, get the address determined and parsed.
    let addr_str = format!("{}:{}", "127.0.0.1", config.port);
    let addr = addr_str
        .parse::<SocketAddr>()
        .expect("Cannot parse address!");

    // Second, bind/listen on that address
    let mut listener = TcpListener::bind(&addr).await.expect("Could not connect to socket!");
    info!(LOGGER, "Listening on: {}", addr);
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                info!(LOGGER, "Accepted connection!");
                process(socket, state.clone(), dump_file.clone()).await;
            },
            Err(e) => error!(LOGGER, "Failed to establish connectin: {:?}", e)
        };
    }
}

pub async fn server(state: State, dump_file: DumpFile, config: Config) {
    tokio::spawn(save_state_interval(state.clone(), dump_file.clone()));
    tokio::spawn(socket_listener(state.clone(), dump_file.clone(), config));
    loop {}
}

// /// Start the redis-oxide server.
// /// Spawns the socket listener and the state saving service.
// pub fn server(state: State, dump_file: DumpFile, config: Config) -> Result<(), R02Error> {
//     tokio::run(lazy(move || {
//         tokio::spawn(save_state_interval(state.clone(), dump_file.clone()));
//         tokio::spawn(socket_listener(state.clone(), dump_file.clone(), config));
//         Ok(())
//     }));
//     Ok(())
// }
