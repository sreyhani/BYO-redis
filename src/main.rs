mod parser;
mod request;
mod store;

use crate::request::{get_request, RequestHandler};
use bytes::BytesMut;
use parser::parse_redis_value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("start");
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_clinet(socket).await;
        });
    }
}

async fn handle_clinet(mut stream: TcpStream) {
    let mut buf = BytesMut::with_capacity(512);
    let mut req_handler = RequestHandler::new();
    loop {
        let read_size = stream.read_buf(&mut buf).await.unwrap();
        if read_size == 0 {
            return;
        }

        let request = get_request(parse_redis_value(&mut buf).unwrap()).unwrap();
        let response = req_handler.handle_request(request);
        stream
            .write_all(response.serialize().as_bytes())
            .await
            .unwrap();
    }
}
