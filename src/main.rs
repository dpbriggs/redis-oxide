#![feature(const_fn)]
#![feature(const_str_len)]
#[macro_use]
extern crate nom;
#[cfg(test)]
extern crate pretty_assertions;
extern crate promptly;
extern crate shlex;

use promptly::prompt;
use shlex::split;
use std::collections::HashMap;
use std::fmt;

mod resp;

use crate::resp::RedisValue;

#[derive(Debug)]
enum Ops {
    Unknown(String),
    Set(String, String),
    Get(String),
}

#[derive(Debug)]
enum OpsError {
    ParsingError,
    NoInput,
}

type KeyValue = HashMap<String, String>;

#[derive(Default, Debug)]
struct Engine {
    kv: KeyValue,
}

#[derive(Debug)]
enum EngineRes {
    Ok,
    StringRes(String),
    Nil,
    UnknownOp(String),
}

impl fmt::Display for EngineRes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EngineRes::Ok => write!(f, "OK"),
            EngineRes::StringRes(s) => write!(f, "{}", s),
            EngineRes::UnknownOp(s) => write!(f, "Unknown operation '{}'", s),
            EngineRes::Nil => write!(f, "(nil)"),
        }
    }
}

impl Engine {
    fn exec(&mut self, action: Ops) -> EngineRes {
        match action {
            Ops::Get(key) => self
                .kv
                .get(&key)
                .map_or(EngineRes::Nil, |v| EngineRes::StringRes(v.to_string())),
            Ops::Set(key, value) => {
                self.kv.insert(key, value);
                EngineRes::Ok
            }
            Ops::Unknown(s) => EngineRes::UnknownOp(s),
        }
    }
}

fn parse(s: &str) -> Result<Ops, OpsError> {
    let split = split(s).ok_or(OpsError::ParsingError)?;
    let first = split.first().ok_or(OpsError::NoInput)?;
    if first.to_lowercase() == "set" {
        let key = split.get(1).ok_or(OpsError::NoInput)?;
        let val = split.get(2).ok_or(OpsError::NoInput)?;
        return Ok(Ops::Set(key.to_string(), val.to_string()));
    }
    if first.to_lowercase() == "get" {
        let key = split.get(1).ok_or(OpsError::NoInput)?;
        return Ok(Ops::Get(key.to_string()));
    }
    Ok(Ops::Unknown(s.to_string()))
}

fn main() {
    // let test_str = "set \"fo  o\" \"awdaw   ddw\"";
    // let test_str_two = "get \"fo  o\"";
    let mut engine = Engine::default();
    loop {
        let line: String = prompt("> ");
        let res = parse(&line).map(|x| engine.exec(x));
        match res {
            Ok(s) => println!("{}", s),
            Err(e) => println!("{:?}", e),
        }
    }
}
