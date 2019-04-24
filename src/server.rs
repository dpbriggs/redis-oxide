use std::env;

use crate::asyncresp::{MyError, RedisValueCodec};
use crate::database::save_state;
use crate::logger::LOGGER;
use crate::{
    ops::translate,
    types::{DumpFile, RedisValue, State},
};
use futures::future::lazy;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_codec::Decoder;

fn process(socket: TcpStream, state: State) {
    let (tx, rx) = RedisValueCodec::default().framed(socket).split();
    // Map all requests into responses and send them back to the client.
    info!(LOGGER, "accepting new connection...");
    let task = tx
        .send_all(rx.and_then(move |r: RedisValue| match translate(&r) {
            Ok(op) => {
                debug!(LOGGER, "running op {:?}", op.clone());
                let res = state.clone().interact(op);
                Ok(RedisValue::from(res))
            }
            Err(e) => Ok(RedisValue::from(e)),
        }))
        .then(|res| {
            if let Err(e) = res {
                println!("failed to process connection; error = {:?}", e);
            }

            Ok(())
        });
    tokio::spawn(task);
}

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

pub fn server(state: State, dump_file: DumpFile) -> Result<(), MyError> {
    // Parse the address we're going to run this server on
    // tokio::spawn(save_state(state.clone()));
    tokio::run(lazy(move || {
        tokio::spawn(save_state(state.clone(), dump_file));
        tokio::spawn(socket_listener(state.clone()));
        Ok(())
    }));
    Ok(())
}
