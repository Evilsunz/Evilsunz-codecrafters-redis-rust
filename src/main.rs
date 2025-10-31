use clap::Parser;
use codecrafters_redis::{decode_resp_array, encode_string, generate_master_repl_id, get_rdb_file, parse_rdb_by_config, set_send_to_replica, Handler, RdbSettings, ReplicaInstance, ReplicaStream, TXContext, REPLICA_STORE, REPLICA_STREAMS};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex};
use std::{mem, thread};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tokio::sync::watch::error::SendError;
use log::error;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(short, long)]
    replicaof: Option<String>,

    #[arg(short, long)]
    dir: Option<String>,

    #[arg(short, long)]
    dbfilename: Option<String>,
}

fn main() {
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
    
    let rdb_settings = match args.dbfilename {
        Some(dbfilename) => {
            Some(
                RdbSettings {
                    dir: args.dir.unwrap(),
                    filename: dbfilename.clone(),
                }
            )
        },
        None => None
    };
    
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
                let rdb_clone = rdb_settings.clone();
                thread::spawn(move || {
                    handle_stream(stream.try_clone().unwrap(), ri_clone, rdb_clone);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    fn handle_stream(mut stream: TcpStream, mut ri: ReplicaInstance, rdb_settings : Option<RdbSettings>) {
        let mut tx_context = TXContext::default();
        let rdb_settings_clone = rdb_settings.clone();
        if rdb_settings.is_some() {
            if let rdb_file = parse_rdb_by_config(&rdb_settings.unwrap().clone()) {
                if rdb_file.is_ok() {
                    for (db_num, database) in rdb_file.unwrap().databases {
                        println!("Database {}: {} keys", db_num, database.entries.len());
                        for entry in database.entries {
                            let mut vec_command = vec!("SET".to_string());
                            if entry.0.contains(":expire:"){
                                let expire = entry.0.split(":expire:").collect::<Vec<&str>>();

                                let start = SystemTime::now();
                                let since_the_epoch = start
                                    .duration_since(UNIX_EPOCH)
                                    .expect("time should go forward");
                                println!(" +++ Now {:?}", since_the_epoch);
                                println!(" +++ Expire: {:?}", expire[1].to_string());

                                let expire_time = expire[1].parse::<u64>().expect("Failed to parse expiration time");
                                if std::time::Duration::from_millis(expire_time) > since_the_epoch {
                                    vec_command.push(expire[0].to_string());
                                    vec_command.push(entry.1.as_string().unwrap());
                                    vec_command.push("PX".to_string());
                                    vec_command.push(expire[1].to_string());   
                                }

                                println!("{}", vec_command.join(" "));
                            }else {
                                vec_command.push(entry.0);
                                vec_command.push(entry.1.as_string().unwrap());
                            }

                            Handler::from_command(vec_command, &mut tx_context, &mut ri, rdb_settings_clone.clone())
                                .process_command();
                            // println!("{} : {:?}", entry.0 , entry.1 );
                        }
                    }
                }
            }
        }
        loop {
            let mut buffer = [0; 512];
            let size = stream.read(&mut buffer).unwrap();
            let decoded_command = decode_resp_array(&buffer).unwrap_or_else(|| {
                panic!("Failed to decode command");
            });
            let command = decoded_command.get(0).unwrap().clone();
            println!("Decoded +++++ {:?}", decoded_command);
            let handler = Handler::from_command(decoded_command, &mut tx_context, &mut ri, rdb_settings_clone.clone());
            println!("Handling {:?}", handler);
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
                        let _ = match v.send(Some(buffer[..size].to_vec())) {
                            Ok(_) => {
                                set_send_to_replica(true);
                            }
                            Err(err) => {
                                println!(" +++++++ error: {}", err);
                            }
                        };
                    });
            }

            if handler_name.starts_with("PSync") {
                println!(" +++++++++++ Received PSync command");
                let rdb = get_rdb_file();
                stream
                    .write_all(format!("${}\r\n", rdb.len()).as_bytes())
                    .unwrap();
                stream.write_all(&rdb).unwrap();
                REPLICA_STREAMS.lock().unwrap().push(ReplicaStream{stream: Arc::new(Mutex::new(stream.try_clone().unwrap()))});
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
