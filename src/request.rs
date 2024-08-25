use crate::{config::SystemConfigArc, store::StoreArc};
use std::{collections::VecDeque, time::Duration};

use anyhow::{anyhow, Ok, Result};

use crate::parser::RedisValue;

pub enum Request {
    Ping,
    Echo(String),
    Set(String, String, Option<Duration>),
    Get(String),
    ConfigGet(String),
    KEYS(String),
    INFO,
    REPLCONF,
}

pub struct RequestHandler {
    store: StoreArc,
    config: SystemConfigArc,
}
impl RequestHandler {
    pub fn new(store: StoreArc, config: SystemConfigArc) -> Self {
        RequestHandler { store, config }
    }

    pub async fn handle_request(&mut self, req: Request) -> RedisValue {
        match req {
            Request::Ping => RedisValue::SimpleString("PONG".to_string()),
            Request::Echo(s) => RedisValue::BulkString(s),
            Request::Set(key, value, None) => {
                self.store.set(key, value).await;
                RedisValue::SimpleString("OK".to_string())
            }

            Request::Set(key, value, Some(expire)) => {
                self.store.set_with_expire(key, value, expire).await;
                RedisValue::SimpleString("OK".to_string())
            }

            Request::Get(key) => {
                let val = self.store.get(key).await.unwrap_or(String::new());
                RedisValue::BulkString(val)
            }
            Request::ConfigGet(key) => {
                let val = self.config.get_config(&key).unwrap_or(String::new());
                RedisValue::Array(vec![
                    RedisValue::BulkString(key),
                    RedisValue::BulkString(val),
                ])
            }
            Request::KEYS(pattern) => {
                assert!(pattern == "*");
                let key = self.store.get_matching_keys(pattern).await;
                let resp = key.into_iter().map(|k| RedisValue::BulkString(k)).collect();
                RedisValue::Array(resp)
            }
            Request::INFO => {
                let replicatioin_config = self.config.get_replication_config().to_string();
                RedisValue::BulkString(replicatioin_config)
            }
            Request::REPLCONF => RedisValue::SimpleString("OK".to_owned()),
        }
    }
}

pub fn get_request(value: RedisValue) -> Result<Request> {
    let (command, mut args) = get_command_and_args(value)?;
    match command.as_str() {
        "ping" => Ok(Request::Ping),
        "echo" => {
            let message = args
                .pop_front()
                .ok_or(anyhow!("echo needs at least 1 argument"))?;
            Ok(Request::Echo(message))
        }
        "set" => make_set_request(&mut args),
        "get" => {
            let key = args
                .pop_front()
                .ok_or(anyhow!("get needs at least 1 argument"))?;
            Ok(Request::Get(key))
        }
        "config" => make_config_request(&mut args),
        "keys" => {
            let pattern = args
                .pop_front()
                .ok_or(anyhow!("keys needs at least 1 argument"))?;
            Ok(Request::KEYS(pattern))
        }
        "info" => Ok(Request::INFO),
        "replconf" => Ok(Request::REPLCONF),
        x => Err(anyhow!("unsupported command: {x}")),
    }
}

fn make_config_request(args: &mut VecDeque<String>) -> Result<Request> {
    let sub_command = args.pop_front().ok_or(anyhow!("config needs subcommand"))?;
    match sub_command.to_lowercase().as_str() {
        "get" => {
            let key = args
                .pop_front()
                .ok_or(anyhow!("config get needs at least 1 argument"))?;
            Ok(Request::ConfigGet(key))
        }
        _ => Err(anyhow!("config {} is not supported", sub_command)),
    }
}

fn make_set_request(args: &mut VecDeque<String>) -> Result<Request> {
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
            return Ok(Request::Set(key, value, Some(Duration::from_millis(delay))));
        }
    }
    Ok(Request::Set(key, value, None))
}

fn get_command_and_args(value: RedisValue) -> Result<(String, VecDeque<String>)> {
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
