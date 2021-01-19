use bytes::Bytes;
use memchr::memchr;
use std::convert::From;
use std::io;
use std::str;

use crate::types::{RedisValueRef, NULL_ARRAY, NULL_BULK_STRING};

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug)]
pub enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte,
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
    BadArraySize(i64),
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
    fn redis_value(self, buf: &Bytes) -> RedisValueRef {
        match self {
            RedisBufSplit::String(bfs) => RedisValueRef::BulkString(bfs.as_bytes(buf)),
            RedisBufSplit::Error(bfs) => RedisValueRef::Error(bfs.as_bytes(buf)),
            RedisBufSplit::Array(arr) => {
                RedisValueRef::Array(arr.into_iter().map(|bfs| bfs.redis_value(buf)).collect())
            }
            RedisBufSplit::NullArray => RedisValueRef::NullArray,
            RedisBufSplit::NullBulkString => RedisValueRef::NullBulkString,
            RedisBufSplit::Int(i) => RedisValueRef::Int(i),
        }
    }
}

/// Fundamental struct for viewing byte slices
///
/// Used for zero-copy redis values.
struct BufSplit(usize, usize);

impl BufSplit {
    /// Get a lifetime appropriate slice of the underlying buffer.
    ///
    /// Constant time.
    #[inline]
    fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    /// Get a Bytes object representing the appropriate slice
    /// of bytes.
    ///
    /// Constant time.
    #[inline]
    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

#[inline]
fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
    if buf.len() <= pos {
        return None;
    }
    memchr(b'\r', &buf[pos..]).and_then(|end| {
        if end + 1 < buf.len() {
            Some((pos + end + 2, BufSplit(pos, pos + end)))
        } else {
            None
        }
    })
}

fn int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
    match word(buf, pos) {
        Some((pos, word)) => {
            let s = str::from_utf8(word.as_slice(buf)).map_err(|_| RESPError::IntParseFailure)?;
            let i = s.parse().map_err(|_| RESPError::IntParseFailure)?;
            Ok(Some((pos, i)))
        }
        None => Ok(None),
    }
}

fn bulk_string(buf: &BytesMut, pos: usize) -> RedisResult {
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

#[allow(clippy::unnecessary_wraps)]
fn simple_string(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, RedisBufSplit::String(word))))
}

#[allow(clippy::unnecessary_wraps)]
fn error(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, RedisBufSplit::Error(word))))
}

fn resp_int(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(int(buf, pos)?.map(|(pos, int)| (pos, RedisBufSplit::Int(int))))
}

fn array(buf: &BytesMut, pos: usize) -> RedisResult {
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
        Some((_pos, bad_num_elements)) => Err(RESPError::BadArraySize(bad_num_elements)),
    }
}

fn parse(buf: &BytesMut, pos: usize) -> RedisResult {
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
                Ok(Some(value.redis_value(&our_data.freeze())))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<RedisValueRef> for RespParser {
    type Error = io::Error;

    fn encode(&mut self, item: RedisValueRef, dst: &mut BytesMut) -> io::Result<()> {
        write_redis_value(item, dst);
        Ok(())
    }
}

fn write_redis_value(item: RedisValueRef, dst: &mut BytesMut) {
    match item {
        RedisValueRef::Error(e) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&e);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::ErrorMsg(e) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&e);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::SimpleString(s) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::BulkString(s) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(s.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::Array(array) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(array.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for redis_value in array {
                write_redis_value(redis_value, dst);
            }
        }
        RedisValueRef::Int(i) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(i.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::NullArray => dst.extend_from_slice(NULL_ARRAY.as_bytes()),
        RedisValueRef::NullBulkString => dst.extend_from_slice(NULL_BULK_STRING.as_bytes()),
    }
}

#[cfg(test)]
mod resp_parser_tests {
    use crate::asyncresp::RespParser;
    use crate::types::{RedisValueRef, Value};
    use bytes::{Bytes, BytesMut};
    use tokio_util::codec::{Decoder, Encoder};

    fn generic_test(input: &'static str, output: RedisValueRef) {
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

    fn generic_test_arr(input: &str, output: Vec<RedisValueRef>) {
        // TODO: Try to make this occur randomly
        let first: usize = input.len() / 2;
        let second = input.len() - first;
        let mut first = BytesMut::from(&input[0..=first]);
        let mut second = Some(BytesMut::from(&input[second..]));

        let mut decoder = RespParser::default();
        let mut res: Vec<RedisValueRef> = Vec::new();
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
        Bytes::from_static(b"hello")
    }

    // XXX: Simple String has been removed.
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
        let t = RedisValueRef::Error(ezs());
        let s = "-hello\r\n";
        generic_test(s, t);

        let t0 = RedisValueRef::Error(Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz"));
        let t1 = RedisValueRef::Error(ezs());
        let s = "-abcdefghijklmnopqrstuvwxyz\r\n-hello\r\n";
        generic_test_arr(s, vec![t0, t1]);
    }

    #[test]
    fn test_bulk_string() {
        let t = RedisValueRef::BulkString(ezs());
        let s = "$5\r\nhello\r\n";
        generic_test(s, t);

        let t = RedisValueRef::BulkString(Bytes::from_static(b""));
        let s = "$0\r\n\r\n";
        generic_test(s, t);
    }

    #[test]
    fn test_int() {
        let t = RedisValueRef::Int(0);
        let s = ":0\r\n";
        generic_test(s, t);

        let t = RedisValueRef::Int(123);
        let s = ":123\r\n";
        generic_test(s, t);

        let t = RedisValueRef::Int(-123);
        let s = ":-123\r\n";
        generic_test(s, t);
    }

    #[test]
    fn test_array() {
        let t = RedisValueRef::Array(vec![]);
        let s = "*0\r\n";
        generic_test(s, t);

        let inner = vec![
            RedisValueRef::BulkString(Bytes::from_static(b"foo")),
            RedisValueRef::BulkString(Bytes::from_static(b"bar")),
        ];
        let t = RedisValueRef::Array(inner);
        let s = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        generic_test(s, t);

        let inner = vec![
            RedisValueRef::Int(1),
            RedisValueRef::Int(2),
            RedisValueRef::Int(3),
        ];
        let t = RedisValueRef::Array(inner);
        let s = "*3\r\n:1\r\n:2\r\n:3\r\n";
        generic_test(s, t);

        let inner = vec![
            RedisValueRef::Int(1),
            RedisValueRef::Int(2),
            RedisValueRef::Int(3),
            RedisValueRef::Int(4),
            RedisValueRef::BulkString(Bytes::from_static(b"foobar")),
        ];
        let t = RedisValueRef::Array(inner);
        let s = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n";
        generic_test(s, t);

        let inner = vec![
            RedisValueRef::Array(vec![
                RedisValueRef::Int(1),
                RedisValueRef::Int(2),
                RedisValueRef::Int(3),
            ]),
            RedisValueRef::Array(vec![
                RedisValueRef::BulkString(Bytes::from_static(b"Foo")),
                RedisValueRef::Error(Bytes::from_static(b"Bar")),
            ]),
        ];
        let t = RedisValueRef::Array(inner);
        let s = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n$3\r\nFoo\r\n-Bar\r\n";
        generic_test(s, t);

        let inner = vec![
            RedisValueRef::BulkString(Bytes::from_static(b"foo")),
            RedisValueRef::NullBulkString,
            RedisValueRef::BulkString(Bytes::from_static(b"bar")),
        ];
        let t = RedisValueRef::Array(inner);
        let s = "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n";
        generic_test(s, t);

        let t = RedisValueRef::NullArray;
        let s = "*-1\r\n";
        generic_test(s, t);
    }
}
