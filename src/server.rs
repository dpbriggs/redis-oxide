use std::env;

use crate::asyncresp::{MyError, RedisValueCodec};
use crate::logger::LOGGER;
use crate::{
    ops::translate,
    types::{RedisValue, State},
};
use futures::future::lazy;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::timer::Interval;
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

fn save_state(state: State) -> impl Future<Item = (), Error = ()> {
    Interval::new(Instant::now(), Duration::from_millis(60 * 1000))
        .skip(1)
        .for_each(move |_| {
            info!(LOGGER, "Saving state...");
            debug!(LOGGER, "state: {:?}", state.save_state());
            Ok(())
        })
        .map_err(|e| error!(LOGGER, "save state failed; err={:?}", e))
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

pub fn server(state: State) -> Result<(), MyError> {
    // Parse the address we're going to run this server on
    // tokio::spawn(save_state(state.clone()));
    tokio::run(lazy(move || {
        tokio::spawn(save_state(state.clone()));
        tokio::spawn(socket_listener(state.clone()));
        Ok(())
    }));
    Ok(())
}
