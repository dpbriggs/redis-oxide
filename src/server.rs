use std::env;

use std::net::SocketAddr;
use std::sync::Arc;

use crate::{engine::Engine, ops::translate, types::RedisValue};
use std::str::FromStr;
use tokio::io::{read_to_end, write_all};
use tokio::net::TcpListener;
use tokio::prelude::*;

pub fn server() -> Result<(), Box<std::error::Error>> {
    // Parse the address we're going to run this server on
    // and set up our TCP listener to accept connections.
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".to_string());
    let addr = addr.parse::<SocketAddr>()?;
    let listener = TcpListener::bind(&addr).map_err(|_| "failed to bind")?;
    println!("Listening on: {}", addr);

    let engine = Arc::new(Engine::default());

    let done = listener
        .incoming()
        .map_err(|e| println!("error accepting socket; error = {:?}", e))
        .for_each(move |socket| {
            // As with many other small examples, the first thing we'll do is
            // *split* this TCP stream into two separately owned halves. This'll
            // allow us to work with the read and write halves independently.
            let (reader, writer) = socket.split();

            // Since our protocol is line-based we use `tokio_io`'s `lines` utility
            // to convert our stream of bytes, `reader`, into a `Stream` of lines.
            let buf: Vec<u8> = Vec::new();
            // let lines = lines(BufReader::new(reader));
            // reader.read_to_end(&mut buf);
            let lines = read_to_end(reader, buf);

            // Here's where the meat of the processing in this server happens. First
            // we see a clone of the database being created, which is creating a
            // new reference for this connected client to use. Also note the `move`
            // keyword on the closure here which moves ownership of the reference
            // into the closure, which we'll need for spawning the client below.
            //
            // The `map` function here means that we'll run some code for all
            // requests (lines) we receive from the client. The actual handling here
            // is pretty simple, first we parse the request and if it's valid we
            // generate a response based on the values in the database.
            let engine = engine.clone();
            let writes = lines
                .map(move |(_stream, line)| {
                    println!("{:?}", line);
                    println!("---");
                    let line = String::from_utf8(line).unwrap();
                    let res = match RedisValue::from_str(&line) {
                        Ok(r) => match translate(&r) {
                            Ok(ops) => ops,
                            Err(e) => {
                                return RedisValue::Error(format!("{:?}", e).as_bytes().to_vec())
                                    .to_string()
                            }
                        },
                        Err(e) => {
                            return RedisValue::Error(format!("{:?}", e).as_bytes().to_vec())
                                .to_string()
                        }
                    };
                    let res = (*engine).clone().exec(res);
                    res.to_string()
                })
                .map(|res| write_all(writer, res.into_bytes()).map(|(w, _)| w));

            // At this point `responses` is a stream of `Response` types which we
            // now want to write back out to the client. To do that we use
            // `Stream::fold` to perform a loop here, serializing each response and
            // then writing it out to the client.
            // let writes = responses.fold(writer, |writer, response| {
            //     write_all(writer, response.into_bytes()).map(|(w, _)| w)
            // });

            // Like with other small servers, we'll `spawn` this client to ensure it
            // runs concurrently with all other clients, for now ignoring any errors
            // that we see.
            let msg = writes.then(move |_| Ok(()));

            tokio::spawn(msg)
        });

    tokio::run(done);
    Ok(())
}
