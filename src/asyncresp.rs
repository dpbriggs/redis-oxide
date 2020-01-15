use bytes::Bytes;
use std::convert::From;
use std::io;
use std::str;

use crate::types::{RedisValue, RedisValueRef, NULL_ARRAY, NULL_BULK_STRING};

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug)]
pub enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte,
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
}

impl From<std::io::Error> for RESPError {
    fn from(e: std::io::Error) -> RESPError {
        RESPError::IOError(e)
    }
}

#[derive(Default)]
pub struct RespParser;

type RedisResult = Result<Option<(usize, RedisBufSplit)>, RESPError>;

enum RedisBufSplit {
    String(BufSplit),
    Error(BufSplit),
    Array(Vec<RedisBufSplit>),
    NullBulkString,
    NullArray,
    Int(i64),
}

impl RedisBufSplit {
    fn to_redis_value(self, buf: &Bytes) -> RedisValueRef {
        match self {
            RedisBufSplit::String(bfs) => RedisValueRef::String(bfs.view_bytes(&buf)),
            RedisBufSplit::Error(bfs) => RedisValueRef::Error(bfs.view_bytes(&buf)),
            RedisBufSplit::Array(arr) => {
                RedisValueRef::Array(arr.into_iter().map(|bfs| bfs.to_redis_value(buf)).collect())
            }
            RedisBufSplit::NullArray => RedisValueRef::NullArray,
            RedisBufSplit::NullBulkString => RedisValueRef::NullBulkString,
            RedisBufSplit::Int(i) => RedisValueRef::Int(i),
        }
    }
}

struct BufSplit(usize, usize);

impl BufSplit {
    #[inline]
    fn copy_bytes<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    #[inline]
    fn view_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

#[inline]
fn word(buf: &mut BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
    if buf.len() <= pos {
        return None;
    }
    match buf[pos..].iter().position(|b| *b == b'\r') {
        Some(end) => {
            if end + 1 < buf.len() {
                Some((pos + end + 2, BufSplit(pos, pos + end)))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn int(mut buf: &mut BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
    if buf.len() <= pos {
        return Ok(None);
    }
    match word(buf, pos) {
        Some((pos, word)) => {
            let s = str::from_utf8(word.copy_bytes(&mut buf))
                .map_err(|_| RESPError::IntParseFailure)?;
            let i = s.parse().map_err(|_| RESPError::IntParseFailure)?;
            Ok(Some((pos, i)))
        }
        None => Ok(None),
    }
}

fn bulk_string(buf: &mut BytesMut, pos: usize) -> RedisResult {
    if buf.len() <= pos {
        return Ok(None);
    }
    match int(buf, pos)? {
        Some((pos, -1)) => Ok(Some((pos, RedisBufSplit::NullBulkString))),
        Some((pos, size)) if size >= 0 => {
            let total_size = pos + size as usize;
            if buf.len() < total_size + 2 {
                Ok(None)
            } else {
                let bb = RedisBufSplit::String(BufSplit(pos, total_size));
                Ok(Some((total_size + 2, bb)))
            }
        }
        Some((_pos, bad_size)) => Err(RESPError::BadBulkStringSize(bad_size)),
        None => Ok(None),
    }
}

fn simple_string(buf: &mut BytesMut, pos: usize) -> RedisResult {
    if buf.len() <= pos {
        return Ok(None);
    }
    match word(buf, pos) {
        Some((pos, word)) => Ok(Some((pos, RedisBufSplit::String(word)))),
        None => Ok(None),
    }
}

fn error(buf: &mut BytesMut, pos: usize) -> RedisResult {
    if buf.len() <= pos {
        return Ok(None);
    }
    match word(buf, pos) {
        Some((pos, word)) => Ok(Some((pos, RedisBufSplit::Error(word)))),
        None => Ok(None),
    }
}

fn resp_int(buf: &mut BytesMut, pos: usize) -> RedisResult {
    if buf.len() <= pos {
        return Ok(None);
    }
    match int(buf, pos)? {
        Some((pos, int)) => Ok(Some((pos, RedisBufSplit::Int(int)))),
        None => Ok(None),
    }
}

fn array(buf: &mut BytesMut, pos: usize) -> RedisResult {
    match int(buf, pos)? {
        None => Ok(None),
        Some((pos, -1)) => Ok(Some((pos, RedisBufSplit::NullArray))),
        Some((pos, num_elements)) if num_elements >= 0 => {
            let mut values = Vec::with_capacity(num_elements as usize);
            let mut curr_pos = pos;
            for _ in 0..num_elements {
                match parse(buf, curr_pos)? {
                    Some((new_pos, value)) => {
                        curr_pos = new_pos;
                        values.push(value);
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((curr_pos, RedisBufSplit::Array(values))))
        }
        _ => Err(RESPError::UnexpectedEnd), // TODO: Make proper error here,
    }
}

fn parse(buf: &mut BytesMut, pos: usize) -> RedisResult {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[pos] {
        b'+' => simple_string(buf, pos + 1),
        b'-' => error(buf, pos + 1),
        b'$' => bulk_string(buf, pos + 1),
        b':' => resp_int(buf, pos + 1),
        b'*' => array(buf, pos + 1),
        _ => Err(RESPError::UnknownStartingByte),
    }
}

impl Decoder for RespParser {
    type Item = RedisValueRef;
    type Error = RESPError;
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            return Ok(None);
        }

        match parse(buf, 0)? {
            Some((pos, value)) => {
                let our_data = buf.split_to(pos);
                Ok(Some(value.to_redis_value(&our_data.freeze())))
            }
            None => Ok(None),
        }
    }
}

impl Encoder for RespParser {
    type Item = RedisValue;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> io::Result<()> {
        write_redis_value(item, dst);
        Ok(())
    }
}

fn write_redis_value(item: RedisValue, dst: &mut BytesMut) {
    match item {
        RedisValue::Error(e) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&e);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValue::SimpleString(s) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValue::BulkString(s) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(s.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValue::Array(array) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(array.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for redis_value in array {
                write_redis_value(redis_value, dst);
            }
        }
        RedisValue::Int(i) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(i.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RedisValue::NullArray => dst.extend_from_slice(NULL_ARRAY.as_bytes()),
        RedisValue::NullBulkString => dst.extend_from_slice(NULL_BULK_STRING.as_bytes()),
    }
}

#[cfg(test)]
mod resp_parser_tests {
    use crate::asyncresp::RespParser;
    use crate::types::{RedisValue, RedisValueRef, Value};
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    fn generic_test(input: &'static str, output: RedisValue) {
        let mut decoder = RespParser::default();
        let result_read = decoder.decode(&mut BytesMut::from(input));

        let mut encoder = RespParser::default();
        let mut buf = BytesMut::new();
        let result_write = encoder.encode(output.clone(), &mut buf);

        assert!(
            result_write.as_ref().is_ok(),
            "{:?}",
            result_write.unwrap_err()
        );

        assert_eq!(input.as_bytes(), buf.as_ref());

        assert!(
            result_read.as_ref().is_ok(),
            "{:?}",
            result_read.unwrap_err()
        );
        // let values = result_read.unwrap().unwrap();

        // let generic_arr_test_case = vec![output.clone(), output.clone()];
        // let doubled = input.to_owned() + &input.to_owned();

        // assert_eq!(output, values);
        // generic_test_arr(&doubled, generic_arr_test_case)
    }

    fn generic_test_arr(input: &str, output: Vec<RedisValue>) {
        // TODO: Try to make this occur randomly
        let first: usize = input.len() / 2;
        let second = input.len() - first;
        let mut first = BytesMut::from(&input[0..=first]);
        let mut second = Some(BytesMut::from(&input[second..]));

        let mut decoder = RespParser::default();
        let mut res: Vec<RedisValue> = Vec::new();
        loop {
            match decoder.decode(&mut first) {
                Ok(Some(value)) => {
                    res.push(value.into());
                    break;
                }
                Ok(None) => {
                    if let None = second {
                        panic!("Test expected more bytes than expected!");
                    }
                    first.extend(second.unwrap());
                    second = None;
                }
                Err(e) => panic!("Should not error, {:?}", e),
                _ => break,
            }
        }
        if let Some(second) = second {
            first.extend(second);
        }
        loop {
            match decoder.decode(&mut first) {
                Ok(Some(value)) => {
                    res.push(value.into());
                    break;
                }
                Err(e) => panic!("Should not error, {:?}", e),
                _ => break,
            }
        }
        assert_eq!(output, res);
    }

    fn ezs() -> Value {
        b"hello".to_vec()
    }

    // Simple String has been removed.
    // #[test]
    // fn test_simple_string() {
    //     let t = RedisValue::BulkString(ezs());
    //     let s = "+hello\r\n";
    //     generic_test(s, t);

    //     let t0 = RedisValue::BulkString(ezs());
    //     let t1 = RedisValue::BulkString("abcdefghijklmnopqrstuvwxyz".as_bytes().to_vec());
    //     let s = "+hello\r\n+abcdefghijklmnopqrstuvwxyz\r\n";
    //     generic_test_arr(s, vec![t0, t1]);
    // }

    #[test]
    fn test_error() {
        let t = RedisValue::Error(ezs());
        let s = "-hello\r\n";
        generic_test(s, t);

        let t0 = RedisValue::Error("abcdefghijklmnopqrstuvwxyz".as_bytes().to_vec());
        let t1 = RedisValue::Error(ezs());
        let s = "-abcdefghijklmnopqrstuvwxyz\r\n-hello\r\n";
        generic_test_arr(s, vec![t0, t1]);
    }

    #[test]
    fn test_bulk_string() {
        let t = RedisValue::BulkString(ezs());
        let s = "$5\r\nhello\r\n";
        generic_test(s, t);

        let t = RedisValue::BulkString(b"".to_vec());
        let s = "$0\r\n\r\n";
        generic_test(s, t);
    }

    #[test]
    fn test_int() {
        let t = RedisValue::Int(0);
        let s = ":0\r\n";
        generic_test(s, t);

        let t = RedisValue::Int(123);
        let s = ":123\r\n";
        generic_test(s, t);

        let t = RedisValue::Int(-123);
        let s = ":-123\r\n";
        generic_test(s, t);
    }

    #[test]
    fn test_array() {
        let t = RedisValue::Array(vec![]);
        let s = "*0\r\n";
        generic_test(s, t);

        let inner = vec![
            RedisValue::BulkString(b"foo".to_vec()),
            RedisValue::BulkString(b"bar".to_vec()),
        ];
        let t = RedisValue::Array(inner);
        let s = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        generic_test(s, t);

        let inner = vec![RedisValue::Int(1), RedisValue::Int(2), RedisValue::Int(3)];
        let t = RedisValue::Array(inner);
        let s = "*3\r\n:1\r\n:2\r\n:3\r\n";
        generic_test(s, t);

        let inner = vec![
            RedisValue::Int(1),
            RedisValue::Int(2),
            RedisValue::Int(3),
            RedisValue::Int(4),
            RedisValue::BulkString(b"foobar".to_vec()),
        ];
        let t = RedisValue::Array(inner);
        let s = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n";
        generic_test(s, t);

        let inner = vec![
            RedisValue::Array(vec![
                RedisValue::Int(1),
                RedisValue::Int(2),
                RedisValue::Int(3),
            ]),
            RedisValue::Array(vec![
                RedisValue::SimpleString(b"Foo".to_vec()),
                RedisValue::Error(b"Bar".to_vec()),
            ]),
        ];
        let t = RedisValue::Array(inner);
        let s = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n";
        generic_test(s, t);

        let inner = vec![
            RedisValue::BulkString(b"foo".to_vec()),
            RedisValue::NullBulkString,
            RedisValue::BulkString(b"bar".to_vec()),
        ];
        let t = RedisValue::Array(inner);
        let s = "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n";
        generic_test(s, t);

        let t = RedisValue::NullArray;
        let s = "*-1\r\n";
        generic_test(s, t);
    }
}
