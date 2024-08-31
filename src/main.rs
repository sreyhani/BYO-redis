use std::fs::File;
use std::sync::Arc;

use bytes::BytesMut;
use redis_starter_rust::config::{parse_args, SystemConfigArc};
use redis_starter_rust::parser::parse_redis_value;
use redis_starter_rust::rdb::read_rdb_file;
use redis_starter_rust::request::{get_request, Request, RequestHandler};
use redis_starter_rust::slave::start_slave_replica;
use redis_starter_rust::store::{Store, StoreArc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let args = std::env::args();
    let config = Arc::new(parse_args(args).unwrap());
    let listener = TcpListener::bind("127.0.0.1:".to_owned() + &config.get_port())
        .await
        .unwrap();
    println!("start listening on {}", config.get_port());
    let store = Arc::new(Store::new());
    load_rdb_file(config.clone(), store.clone()).await;

    if config.get_replication_config().is_slave() {
        tokio::spawn(start_slave_replica(config.clone()));
    }

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let store_c = store.clone();
        let config_c = config.clone();
        tokio::spawn(async move {
            handle_clinet(socket, store_c, config_c).await;
        });
    }
}

async fn load_rdb_file(config: SystemConfigArc, store: StoreArc) {
    let rdb_file_path = config.get_rdb_path();
    if rdb_file_path.is_none() {
        return;
    }
    if File::open(rdb_file_path.clone().unwrap()).is_err() {
        return;
    }
    let rdb_file = read_rdb_file(rdb_file_path.unwrap()).unwrap();
    store.add_multiple_keys(rdb_file.key_vals).await;
    store.set_multiple_expires(rdb_file.key_expires);
}

async fn handle_clinet(mut stream: TcpStream, store: StoreArc, config: SystemConfigArc) {
    let mut buf = BytesMut::with_capacity(512);
    let mut req_handler = RequestHandler::new(store, config);
    loop {
        let read_size = stream.read_buf(&mut buf).await.unwrap();
        if read_size == 0 {
            return;
        }

        let request = get_request(parse_redis_value(&mut buf).unwrap()).unwrap();
        let response = req_handler.handle_request(request.clone()).await;
        stream
            .write_all(response.serialize().as_bytes())
            .await
            .unwrap();
        send_rdb_as_master(request, &mut stream).await;
    }
}

async fn send_rdb_as_master(request: Request, stream: &mut TcpStream) {
    match request {
        Request::PSYNC => {
            let empty_rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").expect("could not decode");
            stream.write(format!("${}\r\n", empty_rdb.len()).as_bytes()).await.unwrap();
            stream.write(empty_rdb.as_slice()).await.unwrap();
        }
        _ => (),
    }
}
