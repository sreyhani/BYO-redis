use crate::{
    config::SystemConfigArc,
    parser::{parse_redis_value, RedisValue},
};
use anyhow::anyhow;
use anyhow::Result;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub async fn start_slave_replica(config: SystemConfigArc) {
    let (ip, port) = config.get_replication_config().get_ip_port();
    let tcp_stream = TcpStream::connect(format!("{ip}:{port}")).await.unwrap();
    handshake_with_master(tcp_stream).await.unwrap();
}

async fn handshake_with_master(mut tcp_stream: TcpStream) -> Result<()> {
    let handshake1 = make_command(vec!["PING"]);
    send_command(&mut tcp_stream, handshake1).await;
    check_response(&mut tcp_stream, RedisValue::BulkString("PONG".to_owned())).await?;
    Ok(())
}

fn make_command(commands: Vec<&str>) -> RedisValue {
    let redis_commands = commands
        .into_iter()
        .map(|command| -> RedisValue { RedisValue::BulkString(command.to_owned()) })
        .collect();
    RedisValue::Array(redis_commands)
}

async fn check_response(stream: &mut TcpStream, expected_response: RedisValue) -> Result<()> {
    let mut buf = BytesMut::with_capacity(512);
    let read_size = stream.read_buf(&mut buf).await.unwrap();
    if read_size == 0 {
        return Err(anyhow!("No response from master"));
    }
    let response = parse_redis_value(&mut buf).unwrap();
    if response != expected_response {
        return Err(anyhow!("Invalid reponse from master"));
    }
    println!("received response: {:?}", response);
    Ok(())
}

async fn send_command(tcp_stream: &mut TcpStream, command: RedisValue) {
    tcp_stream
        .write_all(command.serialize().as_bytes())
        .await
        .unwrap();
}
