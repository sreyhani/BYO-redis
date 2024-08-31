use crate::{
    config::SystemConfigArc,
    parser::{parse_redis_value, RedisValue},
    request::{get_request, RequestHandler},
    store::StoreArc,
};
use anyhow::anyhow;
use anyhow::Result;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub async fn start_slave_replica(store: StoreArc, config: SystemConfigArc) {
    let (ip, port) = config.get_replication_config().get_ip_port();
    let mut tcp_stream = TcpStream::connect(format!("{ip}:{port}")).await.unwrap();
    handshake_with_master(config.clone(), &mut tcp_stream)
        .await
        .unwrap();
    handle_updates_from_master(&mut tcp_stream, store, config).await;
}

async fn handshake_with_master(config: SystemConfigArc, stream: &mut TcpStream) -> Result<()> {
    let handshake1 = make_command(vec!["PING"]);
    send_command(stream, handshake1).await;
    check_response(stream, RedisValue::SimpleString("PONG".to_owned())).await?;

    let handshake2 = make_command(vec!["REPLCONF", "listening-port", &config.get_port()]);
    send_command(stream, handshake2).await;
    check_response(stream, RedisValue::SimpleString("OK".to_owned())).await?;

    let handshake3 = make_command(vec!["REPLCONF", "capa", "psync2"]);
    send_command(stream, handshake3).await;
    check_response(stream, RedisValue::SimpleString("OK".to_owned())).await?;

    let handshake4 = make_command(vec!["PSYNC", "?", "-1"]);
    send_command(stream, handshake4).await;
    check_response(stream, RedisValue::SimpleString("FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0".to_owned())).await?;
    // read_rdb_file(stream).await?;
    println!("handshake done");
    Ok(())
}

async fn read_rdb_file(stream: &mut TcpStream) -> Result<()> {
    let mut buf = BytesMut::with_capacity(512);
    let read_size = stream.read_buf(&mut buf).await.unwrap();
    if read_size == 0 {
        return Err(anyhow!("No response from master"));
    }
    println!("rdb buf: {:?}", buf);
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
        return Err(anyhow!(
            "Invalid reponse from master: {:?} expected: {:?}",
            response,
            expected_response
        ));
    }
    Ok(())
}

async fn send_command(tcp_stream: &mut TcpStream, command: RedisValue) {
    tcp_stream
        .write_all(command.serialize().as_bytes())
        .await
        .unwrap();
}

async fn handle_updates_from_master(
    stream: &mut TcpStream,
    store: StoreArc,
    config: SystemConfigArc,
) {
    let mut buf = BytesMut::with_capacity(512);
    let mut req_handler = RequestHandler::new(store, config.clone());
    loop {
        let read_size = stream.read_buf(&mut buf).await.unwrap();
        if read_size == 0 {
            return;
        }
        println!("rdb buf: {:?}", buf);
        let request = get_request(parse_redis_value(&mut buf).unwrap()).unwrap();
        println!("receiving update from master {:?}", request);
        let _response = req_handler.handle_request(request.clone()).await;
    }
}
