use clap::Parser;
use codecrafters_redis::{decode_resp_array, encode_int, generate_master_repl_id, get_rdb_file, is_readonly_command, parse_rdb_by_config, set_send_to_replica, AOFSettings, Handler, RdbSettings, ReplicaInstance, ReplicaStream, TXContext, REPLICA_STORE, REPLICA_STREAMS};
use codecrafters_redis::acl::Auth;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::{fs, thread};
use std::path::Path;
use tokio::sync::watch;
use codecrafters_redis::channels::{PUBSUB};
use codecrafters_redis::versions::{VERSIONS};
use tokio::runtime::Runtime;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(short, long)]
    replicaof: Option<String>,

    #[arg(short, long,default_value ="/app")]
    dir: Option<String>,

    #[arg(short, long)]
    dbfilename: Option<String>,

    #[arg(short, long, default_value = "no")]
    appendonly: Option<String>,

    #[arg(short, long, default_value = "appendonlydir")]
    appenddirname: Option<String>,

    #[arg(short, long, default_value = "appendonly.aof")]
    appendfilename: Option<String>,

    #[arg(short, long, default_value = "everysec")]
    appendfsync: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("{:?}", args);
    let listener = TcpListener::bind(SocketAddr::new(
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
                    dir: args.dir.clone().unwrap(),
                    filename: dbfilename.clone(),
                }
            )
        },
        None => None
    };


    let aof_settings = match args.appendonly {
        Some(appendonly) => {
            if appendonly == "yes" {
                let path = format!("{}/{}", args.dir.clone().unwrap(),args.appenddirname.clone().unwrap());
                let _ = fs::create_dir_all(Path::new(&path));
                let file_path = format!("{}/{}.1.incr.aof", path,args.appendfilename.clone().unwrap());
                let aof_file = fs::File::create(file_path).unwrap();
                let manifest_path = format!("{}/{}.manifest", path,args.appendfilename.clone().unwrap());
                let mut manifest = fs::File::create(manifest_path).unwrap();
                manifest.write_all(format!("file {}.1.incr.aof seq 1 type i", args.appendfilename.clone().unwrap()).as_bytes()).unwrap();
                Some(
                    AOFSettings {
                        dir: args.dir.unwrap(),
                        appenddirname : args.appenddirname.unwrap(),
                        appendonly,
                        appendfilename: args.appendfilename.unwrap(),
                        appendfsync: args.appendfsync.unwrap(),
                        aof_file: Some(Arc::new(Mutex::new(aof_file)))
                    }
                )
            } else {
                Some(AOFSettings {
                    dir: args.dir.unwrap(),
                    appenddirname : args.appenddirname.unwrap(),
                    appendonly,
                    appendfilename: args.appendfilename.unwrap(),
                    appendfsync: args.appendfsync.unwrap(),
                    aof_file: None
                })
            }
        },
        None => None
    };

    let mut ri2 = ri.clone();
    if ri.is_replica {
        thread::spawn(move || {
            println!("for repl thread");
            let _ = ri2.connect_to_master();
        });
    }
    VERSIONS.lock().unwrap().start_listening();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let ri_clone = ri.clone();
                let rdb_clone = rdb_settings.clone();
                let aof_clone = aof_settings.clone();
                thread::spawn(move || {
                    handle_stream(stream.try_clone().unwrap(), ri_clone, rdb_clone,aof_clone);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }


    fn load_rdb_data(
        rdb_settings: Option<RdbSettings>,
        tx_context: &mut TXContext,
        ri: &mut ReplicaInstance,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(settings) = rdb_settings {
            let rdb_file = parse_rdb_by_config(&settings)?;

            for (_db_num, database) in rdb_file.databases {
                for (key, value) in database.entries {
                    let mut vec_command = vec!["SET".to_string()];

                    if key.contains(":expire:") {
                        let parts: Vec<&str> = key.split(":expire:").collect();
                        let expire_time = parts[1].parse::<u64>()?;

                        let since_the_epoch = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("time should go forward");

                        if std::time::Duration::from_millis(expire_time) > since_the_epoch {
                            vec_command.push(parts[0].to_string());
                            vec_command.push(value.as_string().unwrap());
                            vec_command.push("PX".to_string());
                            vec_command.push(parts[1].to_string());
                        } else {
                            continue; // Skip expired keys
                        }
                    } else {
                        vec_command.push(key);
                        vec_command.push(value.as_string().unwrap());
                    }

                    Handler::from_command(vec_command, tx_context, ri,&mut Auth::default(), Some(settings.clone()), None)
                        .process_command();
                }
            }
        }
        Ok(())
    }

    fn handle_stream(mut stream: TcpStream, mut ri: ReplicaInstance, rdb_settings: Option<RdbSettings>, aof_settings: Option<AOFSettings>) {
        let mut tx_context = TXContext::default();
        let mut auth = Auth::default();
        let rdb_settings_clone = rdb_settings.clone();

        if let Err(e) = load_rdb_data(rdb_settings, &mut tx_context, &mut ri) {
            eprintln!("Failed to load RDB data: {}", e);
        }

        loop {
            let mut buffer = [0; 512];
            let size = match stream.read(&mut buffer) {
                Ok(size) => size,
                Err(e) => {
                    eprintln!("Failed to read from stream: {}", e);
                    break;
                }
            };

            let decoded_command = decode_resp_array(&buffer).unwrap_or_else(|| {
                panic!("Failed to decode command");
            });
            //Stupid
            let decoded_commandz = decoded_command.clone();
            let command = decoded_command.get(0).unwrap().clone();
            println!("Decoded +++++ {:?}", decoded_command);
            let handler = Handler::from_command(decoded_command, &mut tx_context, &mut ri, &mut auth, rdb_settings_clone.clone(), aof_settings.clone());
            if aof_settings.is_some() && !is_readonly_command(&handler){
                aof_settings.as_ref().unwrap().append_to_file(decoded_commandz.clone());
            }
            println!("Handling {:?}", handler);
            let handler_name = handler.to_string();
            let response = handler.process_command();

            if let Err(e) = stream.write_all(&response) {
                eprintln!("Failed to write response: {}", e);
                break;
            }

            if !ri.is_replica && command.eq("SET") {
                notify_replicas_on_set(&buffer, size);
            }

            if handler_name.starts_with("PSync") {
                handle_psync_command(&mut stream);
                break;
            }

            if handler_name.starts_with("Subscribe") {
                use codecrafters_redis::channels::SubscriptionModeHandler;
                use std::thread;
                let second_command = decoded_commandz.get(1).unwrap().clone();
                let client_id = format!("client_{:?}", thread::current().id());
                let mut sub_handler = SubscriptionModeHandler::new(client_id);

                if !second_command.is_empty() {
                    let initial_response = sub_handler.subscribe_to_channel(second_command.clone());
                    if let Err(e) = stream.write_all(&initial_response) {
                        eprintln!("Failed to send initial subscription response: {}", e);
                        break;
                    }
                }

                let rt = Runtime::new().unwrap();
                rt.block_on(async {
                    match tokio::net::TcpStream::from_std(stream) {
                        Ok(mut tokio_stream) => {
                            sub_handler.run_loop_async(&mut tokio_stream).await;
                        },
                        Err(e) => {
                            eprintln!("Failed to convert to tokio stream: {}", e);
                        }
                    }
                });
                break;
            }

            if handler_name.starts_with("Publish") {
                let second_command = decoded_commandz.get(1).unwrap().clone();
                let third_command = decoded_commandz.get(2).unwrap().clone();

                let subscriber_count = PUBSUB.publish(second_command.clone(),third_command.clone());
                let  _ =stream.write_all(&encode_int(&subscriber_count));
            }

        }
    }

    fn notify_replicas_on_set(buffer: &[u8], size: usize) {
        REPLICA_STORE
            .notifiers
            .lock()
            .unwrap()
            .iter()
            .for_each(|(_, sender)| {
                match sender.send(Some(buffer[..size].to_vec())) {
                    Ok(_) => {
                        set_send_to_replica(true);
                    }
                    Err(err) => {
                        eprintln!("Failed to send to replica: {}", err);
                    }
                }
            });
    }

    fn handle_psync_command(stream: &mut TcpStream) {
        println!("+++++++++++ Received PSync command");

        let rdb = get_rdb_file();
        if let Err(e) = stream.write_all(format!("${}\r\n", rdb.len()).as_bytes()) {
            eprintln!("Failed to write RDB length: {}", e);
            return;
        }

        if let Err(e) = stream.write_all(&rdb) {
            eprintln!("Failed to write RDB data: {}", e);
            return;
        }

        // Register this stream as a replica
        if let Ok(cloned_stream) = stream.try_clone() {
            REPLICA_STREAMS.lock().unwrap().push(ReplicaStream {
                stream: Arc::new(Mutex::new(cloned_stream))
            });
        }

        start_replica_sync_loop(stream);
    }

    // fn handle_subscribe_command(stream: &mut TcpStream, channel_name: &str) {
    //     use codecrafters_redis::channels::PUBSUB;
    //     use codecrafters_redis::encode_vec_of_value;
    //     use std::thread;
    //
    //     println!("+++++++++++ Entering subscription mode for channel: {}", channel_name);
    //     let client_id = format!("client_{:?}", thread::current().id());
    //     let (mut rx, _count) = PUBSUB.subscribe(client_id, channel_name.to_string());
    //     loop {
    //         match rx.blocking_recv() {
    //             Ok(message) => {
    //                 let response = vec![
    //                     resp::Value::String("message".to_string()),
    //                     resp::Value::String(message.channel),
    //                     resp::Value::String(message.content),
    //                 ];
    //                 let encoded = encode_vec_of_value(response);
    //                 if let Err(e) = stream.write_all(&encoded) {
    //                     eprintln!("Failed to write message to subscriber: {}", e);
    //                     break;
    //                 }
    //             }
    //             Err(e) => {
    //                 eprintln!("Subscription channel error: {}", e);
    //                 break;
    //             }
    //         }
    //     }
    // }

    fn start_replica_sync_loop(stream: &mut TcpStream) {
        let mut rx = REPLICA_STORE
            .notifiers
            .lock()
            .unwrap()
            .entry(generate_master_repl_id())
            .or_insert_with(|| watch::channel(None).0)
            .subscribe();

        loop {
            match rx.has_changed() {
                Ok(true) => {
                    if let Some(command) = rx.borrow_and_update().clone() {
                        if let Err(e) = stream.write_all(&command) {
                            eprintln!("Failed to write to replica: {}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Replica sync channel error: {}", e);
                    break;
                }
                _ => {}
            }
        }
    }
}
