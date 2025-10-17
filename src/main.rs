#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    handle_stream(stream.try_clone().unwrap());
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    fn handle_stream(mut stream: TcpStream) {
        loop {
            let mut buffer = [0; 512];
            stream.read(&mut buffer).unwrap();
            if buffer.is_empty() {
                stream.shutdown(std::net::Shutdown::Both).unwrap();
                break;
            }
            stream.write_all(b"+PONG\r\n").unwrap();
        }
    }
}
