use anyhow::{anyhow, Ok, Result};

use crate::parser::RedisValue;

pub enum Request {
    PING,
    ECHO(String),
}

pub fn handle_request(req: Request) -> RedisValue {
    match req {
        Request::PING => RedisValue::BulkString("PONG".to_string()),
        Request::ECHO(s) => RedisValue::BulkString(s),
    }
}

pub fn get_request(value: RedisValue) -> Result<Request> {
    let (command, args) = get_command(value)?;
    match command.as_str() {
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

fn get_command(value: RedisValue) -> Result<(String, Vec<RedisValue>)> {
    match value {
        RedisValue::Array(mut vals) => {
            if vals.is_empty() {
                return Err(anyhow!("command is empty"));
            }
            let command_str = vals.remove(0).get_bulk_string()?.to_lowercase();
            Ok((command_str, vals))
        }
        _ => Err(anyhow!("expects command to be an array")),
    }
}
