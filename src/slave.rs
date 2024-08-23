use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{config::SystemConfigArc, parser::RedisValue};

pub async fn start_slave_replica(config: SystemConfigArc) {
    let (ip, port) = config.get_replication_config().get_ip_port();
    let mut tcp_stream = TcpStream::connect(format!("{ip}:{port}")).await.unwrap();
    let ping = RedisValue::BulkString("PING".to_owned());
    let handshake = RedisValue::Array(vec![ping]);
    tcp_stream
        .write_all(handshake.serialize().as_bytes())
        .await
        .unwrap();
}
