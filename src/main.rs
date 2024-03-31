use anyhow::{anyhow, Ok, Result};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("start");
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_clinet(socket).await;
        });
    }
}

#[derive(PartialEq, Debug)]
enum RedisValue {
    SimpleString(String),
    BulkString(String),
    Array(Vec<RedisValue>),
}

enum Request {
    PING,
    ECHO(String),
}

async fn handle_clinet(mut stream: TcpStream) {
    let mut buf = BytesMut::with_capacity(512);
    loop {
        let read_size = stream.read_buf(&mut buf).await.unwrap();
        if read_size == 0 {
            return;
        }
        println!(
            "received data: {:?}",
            std::str::from_utf8(&buf[..read_size])
        );
        let request = get_request(parse_redis_value(&mut buf).unwrap()).unwrap();
        let response = handle_request(request);
        stream
            .write_all(response.serialize().as_bytes())
            .await
            .unwrap();
    }
}

fn handle_request(req: Request) -> RedisValue {
    match req {
        Request::PING => RedisValue::BulkString("PONG".to_string()),
        Request::ECHO(s) => RedisValue::BulkString(s),
    }
}

fn get_request(value: RedisValue) -> Result<Request> {
    return match value {
        RedisValue::Array(vals) => {
            // let command = vals.first().unwrap().get_bulk_string()?;
            let (command, args) = vals.split_first().ok_or(anyhow!("command is empty"))?;
            match command.get_bulk_string()?.to_lowercase().as_str() {
                "ping" => Ok(Request::PING),
                "echo" => {
                    let message = args
                        .first()
                        .ok_or(anyhow!("echo needs at least 1 argument"))?
                        .get_bulk_string()?;
                    Ok(Request::ECHO(message))
                }
                _ => Err(anyhow!("unsupported command")),
            }
        }
        _ => Err(anyhow!("expects command to be an array")),
    };
}

impl RedisValue {
    fn get_bulk_string(&self) -> Result<String> {
        match self {
            RedisValue::BulkString(s) => Ok(s.to_string()),
            _ => Err(anyhow!("value is not bulkstring")),
        }
    }

    fn serialize(&self) -> String {
        match self {
            RedisValue::SimpleString(s) => format!("+{}\r\n", s),
            RedisValue::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            _ => panic!("serializer not implemented for this type"),
        }
    }
}

fn parse_redis_value(buffer: &mut BytesMut) -> Result<RedisValue> {
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
    let val = read_until_crlf(buffer).unwrap();
    Ok(RedisValue::SimpleString(
        String::from_utf8(val.to_vec()).unwrap(),
    ))
}

fn parse_array(buffer: &mut BytesMut) -> Result<RedisValue> {
    assert_eq!(buffer[0] as char, '*');
    buffer.advance(1);
    let len = parse_int(read_until_crlf(buffer).unwrap())? as usize;
    let mut vals = vec![];
    for _i in 0..len {
        let val = parse_redis_value(buffer)?;
        vals.push(val);
    }
    Ok(RedisValue::Array(vals))
}

fn parse_bulk_string(buffer: &mut BytesMut) -> Result<RedisValue> {
    assert_eq!(buffer[0] as char, '$');
    buffer.advance(1);
    let len = parse_int(read_until_crlf(buffer).unwrap())? as usize;
    let message = String::from_utf8(buffer.split_to(len).to_vec())?;
    buffer.advance(2);
    Ok(RedisValue::BulkString(message))
}

fn parse_int(buffer: BytesMut) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}

fn read_until_crlf(buffer: &mut BytesMut) -> Option<BytesMut> {
    if let Some(pos) = buffer.windows(2).position(|bytes| bytes == b"\r\n") {
        let line = buffer.split_to(pos);
        buffer.advance(2);
        Some(line)
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use crate::{parse_redis_value, RedisValue};
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
