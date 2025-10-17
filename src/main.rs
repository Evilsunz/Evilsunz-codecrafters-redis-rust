#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use codecrafters_redis::{decode_resp_array, encode_string};

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
            println!("Strat ");
            // stream.write_all(&encode_string("PONG")).unwrap();
            match decode_resp_array(&buffer) {
                Some(command) => {
                    println!("{}", command.get(0).unwrap());
                    if command.get(0) == Some(&"ECHO".to_string()) {
                        println!("1");
                        stream.write_all(&encode_string(command.get(1).unwrap())).unwrap();
                    } else if command.get(0) == Some(&"PING".to_string()) {
                        println!("2");
                        stream.write_all(&encode_string("PONG")).unwrap();
                    }

                }
                None => {
                    stream.write_all(&encode_string("Command not recognized")).unwrap();
                }
            }
        }
    }
}
