use crate::store::Store;
use std::collections::VecDeque;

use anyhow::{anyhow, Ok, Result};

use crate::parser::RedisValue;

pub enum Request {
    PING,
    ECHO(String),
    SET(String, String),
    GET(String),
}
pub struct RequestHandler {
    store: Store,
}
impl RequestHandler {
    pub fn new() -> Self {
        RequestHandler {
            store: Store::new(),
        }
    }

    pub fn handle_request(&mut self, req: Request) -> RedisValue {
        match req {
            Request::PING => RedisValue::BulkString("PONG".to_string()),
            Request::ECHO(s) => RedisValue::BulkString(s),
            Request::SET(key, value) => {
                self.store.set(key, value);
                RedisValue::SimpleString("OK".to_string())
            }
            Request::GET(key) => RedisValue::BulkString(
                self.store
                    .get(key)
                    .ok_or(anyhow!("value not set"))
                    .unwrap()
                    .to_string(),
            ),
        }
    }
}

pub fn get_request(value: RedisValue) -> Result<Request> {
    let (command, mut args) = get_command(value)?;
    match command.as_str() {
        "ping" => Ok(Request::PING),
        "echo" => {
            let message = args
                .pop_front()
                .ok_or(anyhow!("echo needs at least 1 argument"))?;
            Ok(Request::ECHO(message))
        }
        "set" => {
            let key = args
                .pop_front()
                .ok_or(anyhow!("set needs at least 2 argument"))?;
            let value = args
                .pop_front()
                .ok_or(anyhow!("set needs at least 2 argument"))?;
            Ok(Request::SET(key, value))
        }
        "get" => {
            let key = args
                .pop_front()
                .ok_or(anyhow!("get needs at least 1 argument"))?;
            Ok(Request::GET(key))
        }
        _ => Err(anyhow!("unsupported command")),
    }
}

fn get_command(value: RedisValue) -> Result<(String, VecDeque<String>)> {
    match value {
        RedisValue::Array(vals) => {
            let mut args: VecDeque<String> = vals
                .iter()
                .map(|val| val.get_bulk_string())
                .collect::<Result<_>>()?;
            let command = args
                .pop_front()
                .ok_or(anyhow!("command is empty"))?
                .to_lowercase();
            Ok((command, args))
        }
        _ => Err(anyhow!("expects command to be an array")),
    }
}
