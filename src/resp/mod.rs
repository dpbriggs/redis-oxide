use std::num::ParseIntError;
use std::str::FromStr;
use std::string::ToString;

#[derive(Debug)]
pub enum RedisValue {
    SimpleString(String),
    Error(String),
    BulkString(String),
    Int(i64), // is it always i64?
    Array(Vec<RedisValue>),
    NullArray,
    NullBulkString,
}

enum RedisValueParsingError {
    BadInt(ParseIntError),
}

const NULL_BULK_STRING: &'static str = "$-1\r\n";
const NULL_ARRAY: &'static str = "*-1\r\n";
const EMPTY_ARRAY: &'static str = "*0\r\n";

impl ToString for RedisValue {
    fn to_string(&self) -> String {
        let v = match self {
            RedisValue::SimpleString(s) => format!("+{}\r\n", s),
            RedisValue::Error(e) => format!("-{}\r\n", e),
            RedisValue::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            RedisValue::Int(i) => format!(":{}\r\n", i.to_string()),
            RedisValue::Array(a) => {
                if a.is_empty() {
                    return EMPTY_ARRAY.to_string();
                }
                let contents: String = a
                    .iter()
                    .map(|inner| inner.to_string())
                    .collect::<Vec<String>>()
                    .join("");
                if contents.ends_with("\r\n") {
                    return format!("*{}\r\n{}", a.len(), contents);
                }
                format!("*{}\r\n{}\r\n", a.len(), contents)
            }
            RedisValue::NullBulkString => NULL_BULK_STRING.to_string(),
            RedisValue::NullArray => NULL_ARRAY.to_string(),
        };
        v
    }
}

#[cfg(test)]
mod tests {
    use crate::resp::RedisValue;
    #[cfg(test)]
    use pretty_assertions::{assert_eq, assert_ne};
    fn ezs() -> String {
        "hello".to_string()
    }
    #[test]
    fn simple_strings() {
        let t = RedisValue::SimpleString(ezs()).to_string();
        assert_eq!(t, "+hello\r\n".to_string());
    }
    #[test]
    fn error() {
        let t = RedisValue::Error(ezs()).to_string();
        assert_eq!(t, "-hello\r\n".to_string());
    }
    #[test]
    fn bulk_string() {
        let t = RedisValue::BulkString(ezs()).to_string();
        assert_eq!(t, "$5\r\nhello\r\n".to_string());
        let t = RedisValue::BulkString("".to_string()).to_string();
        assert_eq!(t, "$0\r\n\r\n".to_string());
    }
    #[test]
    fn array() {
        let t = RedisValue::Array(vec![]).to_string();
        assert_eq!(t, "*0\r\n".to_string());

        let inner = vec![
            RedisValue::BulkString("foo".to_string()),
            RedisValue::BulkString("bar".to_string()),
        ];
        let t = RedisValue::Array(inner).to_string();
        assert_eq!(t, "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".to_string());

        let inner = vec![RedisValue::Int(1), RedisValue::Int(2), RedisValue::Int(3)];
        let t = RedisValue::Array(inner).to_string();
        assert_eq!(t, "*3\r\n:1\r\n:2\r\n:3\r\n".to_string());

        let inner = vec![
            RedisValue::Int(1),
            RedisValue::Int(2),
            RedisValue::Int(3),
            RedisValue::Int(4),
            RedisValue::BulkString("foobar".to_string()),
        ];
        let t = RedisValue::Array(inner).to_string();
        assert_eq!(
            t,
            "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n".to_string()
        );

        let inner = vec![
            RedisValue::Array(vec![
                RedisValue::Int(1),
                RedisValue::Int(2),
                RedisValue::Int(3),
            ]),
            RedisValue::Array(vec![
                RedisValue::SimpleString("Foo".to_string()),
                RedisValue::Error("Bar".to_string()),
            ]),
        ];
        let t = RedisValue::Array(inner).to_string();
        assert_eq!(
            t,
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n".to_string()
        );

        let inner = vec![
            RedisValue::BulkString("foo".to_string()),
            RedisValue::NullBulkString,
            RedisValue::BulkString("bar".to_string()),
        ];
        let t = RedisValue::Array(inner).to_string();
        assert_eq!(t, "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n".to_string());
    }

}
