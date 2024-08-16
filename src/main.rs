use std::fs::File;
use std::sync::Arc;

use bytes::BytesMut;
use redis_starter_rust::config::{parse_args, SystemConfigArc};
use redis_starter_rust::parser::parse_redis_value;
use redis_starter_rust::rdb::read_rdb_file;
use redis_starter_rust::request::{get_request, RequestHandler};
use redis_starter_rust::store::{Store, StoreArc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let args = std::env::args();
    let config = Arc::new(parse_args(args).unwrap());
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("start");
    let store = Arc::new(Store::new());
    load_rdb_file(config.clone(), store.clone()).await;
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
        let response = req_handler.handle_request(request).await;
        stream
            .write_all(response.serialize().as_bytes())
            .await
            .unwrap();
    }
}
