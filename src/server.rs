/// Server launch file. Starts the services to make redis-oxide work.
use crate::asyncresp::RedisValueCodec;
use crate::database::save_state;
use crate::logger::LOGGER;
use crate::ops::op_interact;
use crate::{
    ops::translate,
    startup::Config,
    types::{DumpFile, RedisValue, ReturnValue, StateRef},
};
use futures::StreamExt;
use futures_util::sink::SinkExt;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;

fn incr_and_save_if_required(state: StateRef, dump_file: DumpFile) {
    state.commands_ran_since_save.fetch_add(1, Ordering::SeqCst);
    let should_save = state.commands_ran_since_save.compare_exchange(
        state.commands_threshold,
        0,
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    if should_save.is_ok() {
        let state_clone = state;
        let dump_file_clone = dump_file;
        tokio::spawn(async {
            save_state(state_clone, dump_file_clone);
        });
    }
}

/// Spawn a RESP handler for the given socket.
///
/// This will synchronously process requests / responses for this
/// connection only. Other connections will be spread across the
/// thread pool.
async fn process(socket: TcpStream, state: StateRef, dump_file: DumpFile) {
    tokio::spawn(async move {
        // let transport = Framed::new(socket, RedisValueCodec::default());
        let mut transport = RedisValueCodec::default().framed(socket);
        // for redis_value in transport.await {
        //     println!("{:?}", redis_value);
        // }
        while let Some(redis_value) = transport.next().await {
            if let Err(e) = redis_value {
                error!(LOGGER, "Error recieving redis value {:?}", e);
                continue;
            }
            let res = match translate(&redis_value.unwrap()) {
                Ok(op) => {
                    debug!(LOGGER, "running op {:?}", op.clone());
                    // Step 1: Execute the operation the operation (from translate above)
                    let res: ReturnValue = op_interact(op, state.clone()).await;
                    // Step 2: Update commands_ran_since_save counter, and save if necessary
                    incr_and_save_if_required(state.clone(), dump_file.clone());
                    // Step 3: Finally Return
                    res.into()
                }
                Err(e) => RedisValue::from(e),
            };
            if let Err(e) = transport.send(res).await {
                error!(LOGGER, "Failed to send data to client! {:?}", e)
            };
        }
    });
}

/// The listener for redis-oxide. Accepts connections and spawns handlers.
pub async fn socket_listener(state: StateRef, dump_file: DumpFile, config: Config) {
    // First, get the address determined and parsed.
    let addr_str = format!("{}:{}", "127.0.0.1", config.port);
    let addr = match addr_str.parse::<SocketAddr>() {
        Ok(s) => s,
        Err(e) => {
            error!(
                LOGGER,
                "Could not start server! Could not parse {} as listening address, given error: {}",
                addr_str,
                e
            );
            return;
        }
    };

    // Second, bind/listen on that address
    let mut listener = match TcpListener::bind(&addr).await {
        Ok(s) => s,
        Err(e) => {
            error!(
                LOGGER,
                "Could not start server! Could not bind to {}, given error: {}", addr_str, e
            );
            if config.port <= 1024 {
                info!(LOGGER, "Hint: You're attempting to bind to a privileged port. Try using -p 6379 or -p 8888");
            }
            return;
        }
    };
    // Finally, loop over each TCP accept and spawn a handler.
    info!(LOGGER, "Listening on: {}", addr);
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                debug!(LOGGER, "Accepted connection!");
                process(socket, state.clone(), dump_file.clone()).await;
            }
            Err(e) => error!(LOGGER, "Failed to establish connectin: {:?}", e),
        };
    }
}
