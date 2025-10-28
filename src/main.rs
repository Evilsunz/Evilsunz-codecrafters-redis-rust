use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::thread;
use clap::Parser;
use codecrafters_redis::{decode_resp_array, encode_str, Handler, TXContext};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
}

fn main() {
    let args = Args::parse();
    let mut listener = TcpListener::bind(SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), args.port)).unwrap();

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
        let mut tx_context = TXContext::default();
        loop {
            let mut buffer = [0; 512];
            stream.read(&mut buffer).unwrap();
            let decoded_command = decode_resp_array(&buffer).unwrap_or_else(|| {
                panic!("Failed to decode command {}", String::from_utf8_lossy(&buffer));
            });
            println!("Decoded +++++ {:?}", decoded_command);
            let response =Handler::from_command(decoded_command,&mut tx_context).process_command();
            stream.write_all(&response).unwrap();
        }
    }
}
