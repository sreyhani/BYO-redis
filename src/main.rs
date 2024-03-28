use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_clinet(stream),
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_clinet(mut stream: TcpStream) {
    let mut buf = [0; 256];
    loop {
        let _read_size = stream.read(&mut buf);
        stream.write_all(b"+PONG\r\n").unwrap();
    }
}
