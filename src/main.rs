use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use codecrafters_redis::{decode_resp_array, encode_string, Handler};

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
            let decoded_command = decode_resp_array(&buffer).unwrap();
            println!("{:?}", decoded_command);
            let response =Handler::from_command(decoded_command).process_command();
            stream.write_all(&response).unwrap();
        }
    }
}
