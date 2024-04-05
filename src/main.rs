mod parser;
mod request;
mod store;

use std::sync::Arc;

use crate::request::{get_request, RequestHandler};
use crate::store::Store;
use bytes::BytesMut;
use parser::parse_redis_value;
use store::StoreArc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("start");
    let store = Arc::new(Store::new());
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let store_c = store.clone();
        tokio::spawn(async move {
            handle_clinet(socket, store_c).await;
        });
    }
}

async fn handle_clinet(mut stream: TcpStream, store: StoreArc) {
    let mut buf = BytesMut::with_capacity(512);
    let mut req_handler = RequestHandler::new(store);
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
