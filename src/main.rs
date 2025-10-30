use clap::Parser;
use codecrafters_redis::{
    decode_resp_array, encode_string, generate_master_repl_id, get_rdb_file, Handler,
    ReplicaInstance, TXContext,
};
use indexmap::IndexMap;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::format;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::{LazyLock, Mutex};
use std::{mem, thread};
use tokio::sync::watch;

pub static REPLICA_STORE: LazyLock<ReplicaStore> = LazyLock::new(|| ReplicaStore::new());

pub struct ReplicaStore {
    notifiers: Mutex<HashMap<String, watch::Sender<Option<Vec<u8>>>>>,
}

impl ReplicaStore {
    fn new() -> Self {
        Self {
            notifiers: Mutex::new(HashMap::new()),
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(short, long)]
    replicaof: Option<String>,
}

fn main() {
    println!("{:?}", Handler::Queued.to_string());
    let args = Args::parse();
    println!("{:?}", args);
    let mut listener = TcpListener::bind(SocketAddr::new(
        IpAddr::from_str("127.0.0.1").unwrap(),
        args.port,
    ))
    .unwrap();
    let ri = args
        .replicaof
        .map(|s| ReplicaInstance::create_replica(s, args.port))
        .unwrap_or_else(|| ReplicaInstance::default());
    let mut ri2 = ri.clone();
    if ri.is_replica {
        thread::spawn(move || {
            println!("for repl thread");
            ri2.connect_to_master();
        });
    }
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let ri_clone = ri.clone();
                thread::spawn(move || {
                    println!("New connection");
                    handle_stream(stream.try_clone().unwrap(), ri_clone);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    fn handle_stream(mut stream: TcpStream, mut ri: ReplicaInstance) {
        let mut tx_context = TXContext::default();
        loop {
            let mut buffer = [0; 512];
            let size = stream.read(&mut buffer).unwrap();
            let decoded_command = decode_resp_array(&buffer).unwrap_or_else(|| {
                panic!(
                    "Failed to decode command {}",
                    String::from_utf8_lossy(&buffer)
                );
            });
            let command = decoded_command.get(0).unwrap().clone();
            println!("Decoded +++++ {:?}", decoded_command);
            let handler = Handler::from_command(decoded_command, &mut tx_context, &mut ri);
            let handler_name = handler.to_string();
            let response = handler.process_command();
            stream.write_all(&response).unwrap();
            if (!ri.is_replica && command.eq("SET")){
                REPLICA_STORE
                    .notifiers
                    .lock()
                    .unwrap()
                    .iter()
                    .for_each(|(k, v)| {
                        println!("Sending to {}", k);
                        println!("Sending payload {}", String::from_utf8_lossy(&buffer[..size]));
                        v.send(Some(buffer[..size].to_vec())).unwrap();
                    });
            }

            if handler_name.starts_with("PSync") {
                println!(" +++++++++++ Received PSync command");
                let rdb = get_rdb_file();
                stream
                    .write_all(format!("${}\r\n", rdb.len()).as_bytes())
                    .unwrap();
                stream.write_all(&rdb).unwrap();

                let mut rx = REPLICA_STORE
                    .notifiers
                    .lock()
                    .unwrap()
                    .entry(generate_master_repl_id())
                    .or_insert_with(|| watch::channel(None).0)
                    .subscribe();
                loop {
                    if rx.has_changed().unwrap(){
                        if let Some(command) = rx.borrow_and_update().clone() {
                            //println!(" +++++++ Received {}" , String::from_utf8_lossy(&command));
                            stream.write_all(&command).unwrap();
                        }
                    }
                }
            }
        }
    }
}
