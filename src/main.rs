use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_clinet(socket).await;
        });
    }
}

async fn handle_clinet(mut stream: TcpStream) {
    let mut buf = [0; 256];
    loop {
        let _read_size = stream.read(&mut buf).await;
        stream.write_all(b"+PONG\r\n").await.unwrap();
    }
}
