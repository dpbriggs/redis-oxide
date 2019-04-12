use std::env;

use std::net::SocketAddr;

use crate::asyncresp::{MyError, RedisValueCodec};
use crate::{
    engine::Engine,
    ops::translate,
    types::{EngineRes, RedisValue},
};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_codec::Decoder;

fn process(socket: TcpStream, engine: Engine) {
    let (tx, rx) = RedisValueCodec::default().framed(socket).split();
    // Map all requests into responses and send them back to the client.
    let task = tx
        .send_all(rx.and_then(move |r: RedisValue| match translate(&r) {
            Ok(op) => {
                let res = engine.clone().exec(op);
                let ret: EngineRes = if let EngineRes::FutureRes(v, f) = res {
                    tokio::spawn(f);
                    *v
                } else if let EngineRes::FutureResValue(f) = res {
                    tokio::spawn(f); // TODO: Figure out how to get EngineRes
                    EngineRes::Ok
                } else {
                    res
                };
                Ok(RedisValue::from(ret))
            }
            Err(e) => Ok(RedisValue::from(e)),
        }))
        .then(|res| {
            if let Err(e) = res {
                println!("failed to process connection; error = {:?}", e);
            }

            Ok(())
        });

    // Spawn the task that handles the connection.
    tokio::spawn(task);
}

pub fn server(engine: Engine) -> Result<(), MyError> {
    // Parse the address we're going to run this server on
    // and set up our redis server.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6379".to_string());
    let addr = addr.parse::<SocketAddr>().expect("Cannot bind to port!");

    let listener = TcpListener::bind(&addr).expect("it to work");
    println!("Listening on: {}", addr);

    tokio::run({
        listener
            .incoming()
            .map_err(|e| println!("failed to accept socket; error = {:?}", e))
            .for_each(move |socket| {
                process(socket, engine.clone());
                Ok(())
            })
    });
    Ok(())
}
