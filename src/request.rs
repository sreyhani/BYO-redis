use crate::store::StoreArc;
use std::{collections::VecDeque, time::Duration};

use anyhow::{anyhow, Ok, Result};

use crate::parser::RedisValue;

pub enum Request {
    PING,
    ECHO(String),
    SET(String, String, Option<Duration>),
    GET(String),
}

pub struct RequestHandler {
    store: StoreArc,
}
impl RequestHandler {
    pub fn new(store: StoreArc) -> Self {
        RequestHandler { store }
    }

    pub async fn handle_request(&mut self, req: Request) -> RedisValue {
        match req {
            Request::PING => RedisValue::BulkString("PONG".to_string()),
            Request::ECHO(s) => RedisValue::BulkString(s),
            Request::SET(key, value, None) => {
                self.store.set(key, value).await;
                RedisValue::SimpleString("OK".to_string())
            }

            Request::SET(key, value, Some(expire)) => {
                self.store.set_with_expire(key, value, expire).await;
                RedisValue::SimpleString("OK".to_string())
            }

            Request::GET(key) => {
                let val = self.store.get(key).await.unwrap_or(String::new());
                RedisValue::BulkString(val)
            }
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
        "set" => make_set_request(&mut args),
        "get" => {
            let key = args
                .pop_front()
                .ok_or(anyhow!("get needs at least 1 argument"))?;
            Ok(Request::GET(key))
        }
        _ => Err(anyhow!("unsupported command")),
    }
}

fn make_set_request(
    args: &mut VecDeque<String>,
) -> std::prelude::v1::Result<Request, anyhow::Error> {
    let key = args
        .pop_front()
        .ok_or(anyhow!("set needs at least 2 argument"))?;
    let value = args
        .pop_front()
        .ok_or(anyhow!("set needs at least 2 argument"))?;
    if !args.is_empty() {
        let arg = args.pop_front().unwrap().to_lowercase();
        if arg == "px" {
            let delay = args
                .pop_front()
                .ok_or(anyhow!("px needs argument"))?
                .parse::<u64>()?;
            return Ok(Request::SET(key, value, Some(Duration::from_millis(delay))));
        }
    }
    Ok(Request::SET(key, value, None))
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
