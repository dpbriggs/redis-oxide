#![feature(const_fn)]
#![feature(const_str_len)]
#[macro_use]
extern crate nom;
#[cfg(test)]
extern crate pretty_assertions;
extern crate promptly;
extern crate shlex;

use promptly::prompt;

mod engine;
mod resp;

use self::engine::engine::Engine;
use self::engine::server::server;
use self::resp::{ops::translate, resp::RedisValue};
use std::str::FromStr;

fn main() {
    // let test_str = "set \"fo  o\" \"awdaw   ddw\"";
    // let test_str_two = "get \"fo  o\"";
    let engine = Engine::default();
    server();
    loop {
        let line: String = prompt("> ");
        println!("{:?}", line);
        // let set = "*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        // let ping = "*1\r\n$4\r\nPING\r\n";
        let res = RedisValue::from_str(&line);
        match res {
            Ok(r) => match translate(&r) {
                Ok(op) => println!("{:?}", engine.clone().exec(op)),
                Err(e) => println!("translate: {:?}", e),
            },
            Err(e) => println!("line: {:?}", e),
        }
    }
}
