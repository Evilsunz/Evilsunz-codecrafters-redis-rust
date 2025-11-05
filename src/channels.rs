use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, LazyLock, Mutex};
use indexmap::IndexMap;
use resp::Value;
use tokio::sync::{broadcast, watch};
use crate::{decode_resp_array, encode_error, encode_int, encode_str, encode_vec, encode_vec_of_value};

pub static PUBSUB: LazyLock<PubSub> = LazyLock::new(|| PubSub::new());

#[derive(Clone, Debug)]
pub struct Message {
    pub channel: String,
    pub content: String,
}

pub struct PubSub {
    channels: Arc<Mutex<HashMap<String, broadcast::Sender<Message>>>>,
    client_subscriptions: Arc<Mutex<HashMap<String, Vec<String>>>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            client_subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn subscribe(&self, client_id: String, channel_name: String) -> (broadcast::Receiver<Message>, usize) {
        let mut channels = self.channels.lock().unwrap();
        let mut subscriptions = self.client_subscriptions.lock().unwrap();

        let client_channels = subscriptions.entry(client_id.clone()).or_insert_with(Vec::new);
        if !client_channels.contains(&channel_name) {
            client_channels.push(channel_name.clone());
        }

        let sender = channels
            .entry(channel_name.clone())
            .or_insert_with(|| {
                println!("Created new channel: {}", channel_name);
                broadcast::channel(100).0
            });

        let receiver = sender.subscribe();
        let subscription_count = client_channels.len();

        (receiver, subscription_count)
    }

    pub fn publish(&self, channel_name: String, content: String) -> usize {
        let channels = self.channels.lock().unwrap();

        if let Some(sender) = channels.get(&channel_name) {
            let message = Message {
                channel: channel_name.clone(),
                content,
            };

            match sender.send(message) {
                Ok(subscriber_count) => {
                    println!("Message sent to {} subscribers on channel '{}'",
                             subscriber_count, channel_name);
                    subscriber_count
                }
                Err(_) => {
                    println!("No active subscribers on channel '{}'", channel_name);
                    0
                }
            }
        } else {
            println!("Channel '{}' does not exist", channel_name);
            0
        }
    }

    pub fn get_client_subscription_count(&self, client_id: &str) -> usize {
        let subscriptions = self.client_subscriptions.lock().unwrap();
        subscriptions.get(client_id).map(|v| v.len()).unwrap_or(0)
    }

    pub fn unsubscribe(&self, client_id: &str, channel_name: &str) -> usize {
        let mut subscriptions = self.client_subscriptions.lock().unwrap();

        if let Some(client_channels) = subscriptions.get_mut(client_id) {
            client_channels.retain(|ch| ch != channel_name);
            client_channels.len()
        } else {
            0
        }
    }

    pub fn subscriber_count(&self, channel_name: &str) -> usize {
        let channels = self.channels.lock().unwrap();

        if let Some(sender) = channels.get(channel_name) {
            sender.receiver_count()
        } else {
            0
        }
    }

    pub fn list_channels(&self) -> Vec<String> {
        let channels = self.channels.lock().unwrap();
        channels.keys().cloned().collect()
    }
}

// Subscription mode handler struct
pub struct SubscriptionModeHandler {
    client_id: String,
    subscribed_channels: HashMap<String, broadcast::Receiver<Message>>,
}

impl SubscriptionModeHandler {
    pub fn new(client_id: String) -> Self {
        Self {
            client_id,
            subscribed_channels: HashMap::new(),
        }
    }

    pub fn subscribe_to_channel(&mut self, channel: String) -> Vec<u8> {
        let (rx, count) = PUBSUB.subscribe(self.client_id.clone(), channel.clone());
        self.subscribed_channels.insert(channel.clone(), rx);

        let response = vec![
            Value::String("subscribe".to_string()),
            Value::String(channel),
            Value::Integer(count as i64),
        ];
        encode_vec_of_value(response)
    }

    pub fn unsubscribe_from_channel(&mut self, channel: String) -> Vec<u8> {
        self.subscribed_channels.remove(&channel);
        let count = PUBSUB.unsubscribe(&self.client_id, &channel);

        let response = vec![
            Value::String("unsubscribe".to_string()),
            Value::String(channel),
            Value::Integer(count as i64),
        ];
        encode_vec_of_value(response)
    }

    pub fn unsubscribe_from_all(&mut self) -> Vec<Vec<u8>> {
        let channels: Vec<String> = self.subscribed_channels.keys().cloned().collect();
        channels.into_iter()
            .map(|ch| self.unsubscribe_from_channel(ch))
            .collect()
    }

    pub fn handle_ping(&self, message: Option<String>) -> Vec<u8> {
        let pong_msg = message.unwrap_or_else(|| "PONG".to_string());
        let response = vec![
            Value::String("pong".to_string()),
            Value::String(pong_msg),
        ];
        encode_vec_of_value(response)
    }

    pub fn handle_command(&mut self, command: Vec<String>) -> Result<Vec<u8>, String> {
        match command.first().map(|s| s.to_uppercase()).as_deref() {
            Some("SUBSCRIBE") => {
                if let Some(channel) = command.get(1) {
                    Ok(self.subscribe_to_channel(channel.clone()))
                } else {
                    Err("ERR wrong number of arguments for 'subscribe' command".to_string())
                }
            }
            Some("UNSUBSCRIBE") => {
                if command.len() > 1 {
                    // Unsubscribe from specific channels
                    let responses: Vec<Vec<u8>> = command.iter()
                        .skip(1)
                        .map(|ch| self.unsubscribe_from_channel(ch.clone()))
                        .collect();
                    Ok(responses.into_iter().flatten().collect())
                } else {
                    // Unsubscribe from all
                    Ok(self.unsubscribe_from_all().into_iter().flatten().collect())
                }
            }
            Some("PING") => {
                Ok(self.handle_ping(command.get(1).cloned()))
            }
            Some("QUIT") => {
                Err("QUIT".to_string()) // Signal to exit
            }
            Some("PSUBSCRIBE") | Some("PUNSUBSCRIBE") => {
                Ok(encode_error("ERR Pattern subscriptions not implemented"))
            }
            Some(unknown_cmd) => {
                Ok(encode_error(&format!("ERR Can't execute '{}' only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context", unknown_cmd)))
            }
            _ => Err("ERR wrong number of arguments for 'psubscribe' command".to_string()),
        }
    }

    pub fn check_messages(&mut self) -> Vec<Vec<u8>> {
        let mut messages = Vec::new();

        for (channel_name, rx) in self.subscribed_channels.iter_mut() {
            match rx.try_recv() {
                Ok(message) => {
                    let response = vec![
                        Value::String("message".to_string()),
                        Value::String(message.channel),
                        Value::String(message.content),
                    ];
                    messages.push(encode_vec_of_value(response));
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    // No message available
                }
                Err(e) => {
                    eprintln!("Subscription channel error for {}: {}", channel_name, e);
                }
            }
        }

        messages
    }

    pub fn is_subscribed(&self) -> bool {
        !self.subscribed_channels.is_empty()
    }

    pub fn run_loop(&mut self, stream: &mut TcpStream) {
        println!("+++++++++++ Entering subscription mode");

        loop {
            // Check for published messages
            for message_data in self.check_messages() {
                if let Err(e) = stream.write_all(&message_data) {
                    eprintln!("Failed to write message to subscriber: {}", e);
                    return;
                }
            }

            // Read command from client
            let mut buffer = [0; 512];
            match stream.read(&mut buffer) {
                Ok(size) if size > 0 => {
                    if let Some(decoded_command) = decode_resp_array(&buffer) {
                        match self.handle_command(decoded_command) {
                            Ok(response) => {
                                if let Err(e) = stream.write_all(&response) {
                                    eprintln!("Failed to send response: {}", e);
                                    break;
                                }

                                // Exit if no channels left
                                if !self.is_subscribed() {
                                    println!("No channels left, exiting subscription mode");
                                    break;
                                }
                            }
                            Err(msg) if msg == "QUIT" => {
                                println!("Client requested QUIT");
                                let _ = stream.write_all(&encode_str("OK"));
                                break;
                            }
                            Err(e) => {
                                eprintln!("Error handling command: {}", e);
                                break;
                            }
                        }
                    }
                }
                Ok(_) => continue,
                Err(e) => {
                    eprintln!("Failed to read from stream: {}", e);
                    break;
                }
            }
        }
    }
}

pub fn subscribe(client_id: String, channel: String) -> Vec<u8> {
    let (_, count) = PUBSUB.subscribe(client_id, channel.clone());
    let mut vector: Vec<Value> = vec![
        Value::String("subscribe".to_string()),
        Value::String(channel),
        Value::Integer(count as i64)
    ];
    encode_vec_of_value(vector)
}