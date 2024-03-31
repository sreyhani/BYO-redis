mod parser;
mod request;

use crate::request::{get_request, handle_request};
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
    loop {
        let read_size = stream.read_buf(&mut buf).await.unwrap();
        if read_size == 0 {
            return;
        }
        println!(
            "received data: {:?}",
            std::str::from_utf8(&buf[..read_size])
        );
        let request = get_request(parse_redis_value(&mut buf).unwrap()).unwrap();
        let response = handle_request(request);
        stream
            .write_all(response.serialize().as_bytes())
            .await
            .unwrap();
    }
}
