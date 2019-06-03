/// Server launch file. Starts the services to make redis-oxide work.
use std::env;

use crate::asyncresp::{R02Error, RedisValueCodec};
use crate::database::save_state;
use crate::logger::LOGGER;
use crate::{
    ops::translate,
    types::{DumpFile, InteractionRes, RedisValue, State},
};
use futures::future::lazy;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_codec::Decoder;

/// Process a single socket; one of these tasks per tcp accept.
fn process(socket: TcpStream, state: State) {
    let (tx, rx) = RedisValueCodec::default().framed(socket).split();
    // Map all requests into responses and send them back to the client.
    info!(LOGGER, "accepting new connection...");
    let task = tx
        .send_all(rx.and_then(move |r: RedisValue| match translate(&r) {
            Ok(op) => {
                debug!(LOGGER, "running op {:?}", op.clone());
                let res = state.clone().exec_op(op);
                match res {
                    InteractionRes::Immediate(r) => Ok(RedisValue::from(r)),
                    InteractionRes::ImmediateWithWork(r, w) => {
                        tokio::spawn(w);
                        Ok(RedisValue::from(r))
                    }
                    InteractionRes::Blocking(_w) => {
                        // TODO: Use actual await when tokio works with latest nightly
                        // XXX: This kills the server.
                        // let r = w.wait().unwrap();
                        // Ok(RedisValue::from(r))
                        Ok(RedisValue::NullBulkString)
                    }
                }
                // Ok(RedisValue::from(res))
            }
            Err(e) => Ok(RedisValue::from(e)),
        }))
        .then(|res| {
            if let Err(e) = res {
                warn!(LOGGER, "failed to process connection; error = {:?}", e);
            }

            Ok(())
        });
    tokio::spawn(task);
}

/// The listener for redis-oxide. Accepts connections and spawns handlers.
fn socket_listener(state: State) -> impl Future<Item = (), Error = ()> {
    // and set up our redis server.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6379".to_string());
    let addr = addr.parse::<SocketAddr>().expect("Cannot bind to port!");

    let listener = TcpListener::bind(&addr).expect("it to work");
    info!(LOGGER, "Listening on: {}", addr);
    listener
        .incoming()
        .map_err(|e| println!("failed to accept socket; error = {:?}", e))
        .for_each(move |socket| {
            process(socket, state.clone());
            Ok(())
        })
        .map_err(|e| panic!("Failed to start server! error = {:?}", e))
}

/// Start the redis-oxide server.
/// Spawns the socket listener and the state saving service.
pub fn server(state: State, dump_file: DumpFile) -> Result<(), R02Error> {
    tokio::run(lazy(move || {
        tokio::spawn(save_state(state.clone(), dump_file));
        tokio::spawn(socket_listener(state.clone()));
        Ok(())
    }));
    Ok(())
}
