use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};

const CRLF: &[u8; 2] = b"\r\n";

#[derive(PartialEq, Debug)]
pub enum RedisValue {
    SimpleString(String),
    BulkString(String),
    Array(Vec<RedisValue>),
}

impl RedisValue {
    pub fn get_bulk_string(&self) -> Result<String> {
        match self {
            RedisValue::BulkString(s) => Ok(s.to_string()),
            _ => Err(anyhow!("value is not bulkstring")),
        }
    }

    pub fn serialize(&self) -> String {
        match self {
            RedisValue::SimpleString(s) => format!("+{}\r\n", s),
            RedisValue::BulkString(s) => {
                if s.is_empty() {
                    return format!("$-1\r\n");
                }
                format!("${}\r\n{}\r\n", s.len(), s)
            }
            _ => panic!("serializer not implemented for this type"),
        }
    }
}

pub fn parse_redis_value(buffer: &mut BytesMut) -> Result<RedisValue> {
    match buffer[0] as char {
        '*' => parse_array(buffer),
        '$' => parse_bulk_string(buffer),
        '+' => parse_simple_string(buffer),
        _ => Err(anyhow!("improper RESP format")),
    }
}

fn parse_simple_string(buffer: &mut BytesMut) -> Result<RedisValue> {
    assert_eq!(buffer[0] as char, '+');
    buffer.advance(1);
    let val = split_by_next_crlf(buffer).unwrap();
    Ok(RedisValue::SimpleString(
        String::from_utf8(val.to_vec()).unwrap(),
    ))
}

fn parse_array(buffer: &mut BytesMut) -> Result<RedisValue> {
    assert_eq!(buffer[0] as char, '*');
    buffer.advance(1);
    let len = parse_int(split_by_next_crlf(buffer).unwrap())? as usize;
    let vals = (0..len)
        .map(|_| parse_redis_value(buffer))
        .collect::<Result<_>>()?;
    Ok(RedisValue::Array(vals))
}

fn parse_bulk_string(buffer: &mut BytesMut) -> Result<RedisValue> {
    assert_eq!(buffer[0] as char, '$');
    buffer.advance(1);
    let len = parse_int(split_by_next_crlf(buffer).unwrap())? as usize;
    let message = String::from_utf8(buffer.split_to(len).to_vec())?;
    buffer.advance(2);
    Ok(RedisValue::BulkString(message))
}

fn parse_int(buffer: BytesMut) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}

fn split_by_next_crlf(buffer: &mut BytesMut) -> Option<BytesMut> {
    if let Some(pos) = buffer.windows(2).position(|bytes| bytes == CRLF) {
        let line = buffer.split_to(pos);
        buffer.advance(2);
        Some(line)
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use crate::{parse_redis_value, parser::RedisValue};
    use bytes::BytesMut;

    #[test]
    fn should_parse_simple_string() {
        assert_eq!(
            parse_redis_value(&mut BytesMut::from("+OK\r\n")).unwrap(),
            RedisValue::SimpleString("OK".to_string())
        );
    }

    #[test]
    fn should_parse_bulk_string() {
        assert_eq!(
            parse_redis_value(&mut BytesMut::from("$5\r\nhello\r\n")).unwrap(),
            RedisValue::BulkString("hello".to_string())
        );
    }

    #[test]
    fn should_parse_empty_bulk_string() {
        assert_eq!(
            parse_redis_value(&mut BytesMut::from("$0\r\n\r\n")).unwrap(),
            RedisValue::BulkString("".to_string())
        );
    }

    #[test]
    fn should_parse_array() {
        let array = RedisValue::Array(vec![
            RedisValue::BulkString("hello".to_string()),
            RedisValue::BulkString("world".to_string()),
            RedisValue::SimpleString("OK".to_string()),
        ]);
        assert_eq!(
            parse_redis_value(&mut BytesMut::from(
                "*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n+OK\r\n"
            ))
            .unwrap(),
            array
        );
    }
}
