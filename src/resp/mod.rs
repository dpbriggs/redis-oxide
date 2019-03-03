use std::num::ParseIntError;
// use std::str::FromStr;
use std::string::ToString;

use nom::be_u64;

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
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

named!(
    get_string<&str, String>,
    map!(take_until_and_consume!("\r\n"), |s| s.to_string())
);

named!(
    get_int<&str, i64>,
    map_res!(get_string, |s: String| s.parse::<i64>())
);

named!(
    get_error<&str, RedisValue>,
    map!(get_string, |s| RedisValue::Error(s))
);

named!(
    get_simple_string<&str, RedisValue>,
    map!(get_string, |s| RedisValue::SimpleString(s))
);

// this was a fucking nightmare to write.
named!(get_bulk_string<&str, RedisValue>,
       alt!(
           do_parse!(
               length: map_res!(take_until_and_consume!("\r\n"), |s: &str| s.parse::<u64>()) >>
                   strs: take!(length) >>
                   tag!("\r\n") >>
                   (RedisValue::BulkString(strs.to_string()))
           ) |
           do_parse!(
                   tag!("-1\r\n") >>
                   (RedisValue::NullBulkString)
           )
       )
);

named!(
    get_redis_int<&str, RedisValue>,
    map!(get_int, |s| RedisValue::Int(s))
);

named!(
    get_array<&str, RedisValue>,
    alt!(
        do_parse!(
            length: map_res!(take_until_and_consume!("\r\n"), |s: &str| s.parse::<u64>()) >>
                kids: count!(redis_value_from, length as usize) >>
                (RedisValue::Array(kids))
        ) |
        do_parse!(
            tag!("-1\r\n") >>
                (RedisValue::NullArray)
        )
    )
);

named!(
    redis_value_from<&str, RedisValue>,
    switch!(take!(1),
            "+" => call!(get_simple_string) |
            "-" => call!(get_error) |
            "$" => call!(get_bulk_string) |
            ":" => call!(get_redis_int) |
            "*" => call!(get_array)
    )
);
// alt!(
//     do_parse!(
//         tag("+") >>
//             get_simple_string
//     )
//         | value!(("", RedisValue::NullBulkString)))

#[test]
fn nom_pls() {
    let test_str = "+OK\r\n";
    let res = redis_value_from(test_str);
    println!("{:?}", res);
    let test_bulk = "$5\r\nhello\r\n";
    let res = redis_value_from(test_bulk);
    println!("{:?}", res);
    let test_bulk = ":1\r\n";
    let res = redis_value_from(test_bulk);
    println!("{:?}", res);
    let test_bulk = "$-1\r\n";
    let res = redis_value_from(test_bulk);
    println!("{:?}", res);
    let test_bulk = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n";
    let res = redis_value_from(test_bulk);
    println!("{:?}", res);
}

#[cfg(test)]
mod tests {
    use crate::resp::RedisValue;
    #[cfg(test)]
    use pretty_assertions::assert_eq;
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
        let t = RedisValue::NullArray.to_string();
        assert_eq!(t, "*-1\r\n".to_string());
    }

}
