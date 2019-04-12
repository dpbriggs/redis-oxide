use crate::types::EngineRes;
use shlex::split;
use std::convert::From;
use std::str::FromStr;
use std::string::ToString;

use crate::types::{RedisValue, EMPTY_ARRAY, NULL_ARRAY, NULL_BULK_STRING};

impl From<EngineRes> for RedisValue {
    fn from(engine_res: EngineRes) -> Self {
        match engine_res {
            EngineRes::Ok => RedisValue::SimpleString(vec![b'O', b'K']),
            EngineRes::Nil => RedisValue::NullBulkString,
            EngineRes::StringRes(s) => RedisValue::BulkString(s),
            EngineRes::MultiStringRes(a) => RedisValue::Array(
                a.iter()
                    .map(|s| RedisValue::BulkString(s.to_vec()))
                    .collect(),
            ),
            EngineRes::UIntRes(i) => RedisValue::Int(i as i64),
            EngineRes::Error(e) => RedisValue::Error(e.to_vec()),
            EngineRes::FutureRes(s, _) => RedisValue::from(*s),
            EngineRes::FutureResValue(_) => unreachable!(),
        }
    }
}

impl ToString for RedisValue {
    fn to_string(&self) -> String {
        match self {
            RedisValue::SimpleString(s) => format!("+{}\r\n", String::from_utf8_lossy(s)),
            RedisValue::Error(e) => format!("-{}\r\n", String::from_utf8_lossy(e)),
            RedisValue::BulkString(s) => {
                format!("${}\r\n{}\r\n", s.len(), String::from_utf8_lossy(s))
            }
            RedisValue::Int(i) => format!(":{}\r\n", i.to_string()),
            RedisValue::Array(a) => {
                if a.is_empty() {
                    return EMPTY_ARRAY.to_string();
                }
                let contents: String = a
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<String>>()
                    .join("");
                if contents.ends_with("\r\n") {
                    return format!("*{:?}\r\n{}", a.len(), contents);
                }
                format!("*{:?}\r\n{:?}\r\n", a.len(), contents)
            }
            RedisValue::NullBulkString => NULL_BULK_STRING.to_string(),
            RedisValue::NullArray => NULL_ARRAY.to_string(),
        }
    }
}

// Here begins the "FromStr" bits

named!(get_string<&[u8], &[u8]>, take_until_and_consume!("\r\n"));

named!(
    get_i64<&[u8], i64>,
    map_res!(get_string, |s: &[u8]| String::from_utf8_lossy(&s)
        .parse::<i64>())
);

named!(
    get_error<&[u8], RedisValue>,
    map!(get_string, |x: &[u8]| RedisValue::Error(x.to_vec()))
);

named!(
    get_simple_string<&[u8], RedisValue>,
    map!(get_string, |x: &[u8]| RedisValue::SimpleString(x.to_vec()))
);

// this was a fucking nightmare to write.
named!(
    get_bulk_string<&[u8], RedisValue>,
    alt!(
        do_parse!(
            length: map_res!(take_until_and_consume!("\r\n"), |s: &[u8]| String::from_utf8_lossy(s).parse::<u64>())
                >> strs: take!(length)
                >> tag!("\r\n")
                >> (RedisValue::BulkString(strs.to_vec()))
        ) | do_parse!(tag!("-1\r\n") >> (RedisValue::NullBulkString))
    )
);

named!(get_int<&[u8], RedisValue>, map!(get_i64, RedisValue::Int));

named!(
    get_array<&[u8], RedisValue>,
    alt!(
        do_parse!(
            length: map_res!(take_until_and_consume!("\r\n"), |s: &[u8]| String::from_utf8_lossy(s).parse::<u64>())
                >> kids: count!(redis_value_from, length as usize)
                >> (RedisValue::Array(kids))
        ) | do_parse!(tag!("-1\r\n") >> (RedisValue::NullArray))
    )
);

named!(
    redis_value_from<&[u8], RedisValue>,
    switch!(take!(1),
            b"+" => call!(get_simple_string) |
            b"-" => call!(get_error) |
            b"$" => call!(get_bulk_string) |
            b":" => call!(get_int) |
            b"*" => call!(get_array)
    )
);

fn parse_inline(s: &str) -> Result<RedisValue, String> {
    match split(s) {
        Some(inner) => Ok(RedisValue::Array(
            inner
                .iter()
                .map(|x| RedisValue::SimpleString((*x).as_bytes().to_vec()))
                .collect(),
        )),
        None => Err("No Input!".to_string()),
    }
}

impl FromStr for RedisValue {
    type Err = String; // TODO: Use a better type

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err("Empty String!".to_owned());
        }
        let first_byte = s.chars().nth(0).unwrap();
        match first_byte {
            '+' | '-' | '$' | ':' | '*' => match redis_value_from(s.as_bytes()) {
                Ok(r) => Ok(r.1),
                Err(e) => Err(e.to_string()),
            },
            _ => match parse_inline(s) {
                Ok(s) => Ok(s),
                Err(e) => Err(e),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::resp::FromStr;
    use crate::types::{RedisValue, Value};
    #[cfg(test)]
    use pretty_assertions::assert_eq;
    fn ezs() -> Value {
        "hello".as_bytes().to_vec()
    }
    #[test]
    fn simple_strings() {
        let t = RedisValue::SimpleString(ezs());
        let s = "+hello\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));
    }
    #[test]
    fn error() {
        let t = RedisValue::Error(ezs());
        let s = "-hello\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));
    }
    #[test]
    fn bulk_string() {
        let t = RedisValue::BulkString(ezs());
        let s = "$5\r\nhello\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));

        let t = RedisValue::BulkString("".as_bytes().to_vec());
        let s = "$0\r\n\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));
    }
    #[test]
    fn array() {
        let t = RedisValue::Array(vec![]);
        let s = "*0\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));

        let inner = vec![
            RedisValue::BulkString("foo".as_bytes().to_vec()),
            RedisValue::BulkString("bar".as_bytes().to_vec()),
        ];
        let t = RedisValue::Array(inner);
        let s = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));

        let inner = vec![RedisValue::Int(1), RedisValue::Int(2), RedisValue::Int(3)];
        let t = RedisValue::Array(inner);
        let s = "*3\r\n:1\r\n:2\r\n:3\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));

        let inner = vec![
            RedisValue::Int(1),
            RedisValue::Int(2),
            RedisValue::Int(3),
            RedisValue::Int(4),
            RedisValue::BulkString("foobar".as_bytes().to_vec()),
        ];
        let t = RedisValue::Array(inner);
        let s = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));

        let inner = vec![
            RedisValue::Array(vec![
                RedisValue::Int(1),
                RedisValue::Int(2),
                RedisValue::Int(3),
            ]),
            RedisValue::Array(vec![
                RedisValue::SimpleString("Foo".as_bytes().to_vec()),
                RedisValue::Error("Bar".as_bytes().to_vec()),
            ]),
        ];
        let t = RedisValue::Array(inner);
        let s = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));

        let inner = vec![
            RedisValue::BulkString("foo".as_bytes().to_vec()),
            RedisValue::NullBulkString,
            RedisValue::BulkString("bar".as_bytes().to_vec()),
        ];
        let t = RedisValue::Array(inner);
        let s = "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));

        let t = RedisValue::NullArray;
        let s = "*-1\r\n";
        assert_eq!(t.to_string(), s.to_string());
        assert_eq!(Ok(t), RedisValue::from_str(s));
    }
}
