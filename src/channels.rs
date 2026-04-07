
use crate::{decode_resp_array, encode_error, encode_str, encode_vec_as_bulk, encode_vec_of_value};
use resp::Value;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast};

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

    pub fn subscribe(
        &self,
        client_id: String,
        channel_name: String,
    ) -> (broadcast::Receiver<Message>, usize) {
        let mut channels = self.channels.lock().unwrap();
        let mut subscriptions = self.client_subscriptions.lock().unwrap();

        let client_channels = subscriptions
            .entry(client_id.clone())
            .or_insert_with(Vec::new);
        if !client_channels.contains(&channel_name) {
            client_channels.push(channel_name.clone());
        }

        let sender = channels.entry(channel_name.clone()).or_insert_with(|| {
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
                    println!(
                        "Message sent to {} subscribers on channel '{}'",
                        subscriber_count, channel_name
                    );
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
            Value::Bulk("subscribe".to_string()),
            Value::Bulk(channel),
            Value::Integer(count as i64),
        ];
        encode_vec_of_value(response)
    }

    pub fn unsubscribe_from_channel(&mut self, channel: String) -> Vec<u8> {
        self.subscribed_channels.remove(&channel);
        let count = PUBSUB.unsubscribe(&self.client_id, &channel);

        let response = vec![
            Value::Bulk("unsubscribe".to_string()),
            Value::Bulk(channel),
            Value::Integer(count as i64),
        ];
        encode_vec_of_value(response)
    }

    pub fn unsubscribe_from_all(&mut self) -> Vec<Vec<u8>> {
        let channels: Vec<String> = self.subscribed_channels.keys().cloned().collect();
        channels
            .into_iter()
            .map(|ch| self.unsubscribe_from_channel(ch))
            .collect()
    }

    pub fn handle_ping(&self, _: Option<String>) -> Vec<u8> {
        // let pong_msg = message.unwrap_or_else(|| "PONG".to_string());
        let response = vec!["pong".to_string(), "".to_string()];
        encode_vec_as_bulk(response)
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
                    let responses: Vec<Vec<u8>> = command
                        .iter()
                        .skip(1)
                        .map(|ch| self.unsubscribe_from_channel(ch.clone()))
                        .collect();
                    Ok(responses.into_iter().flatten().collect())
                } else {
                    Ok(self.unsubscribe_from_all().into_iter().flatten().collect())
                }
            }
            Some("PING") => Ok(self.handle_ping(command.get(1).cloned())),
            Some("QUIT") => Err("QUIT".to_string()),
            Some("PSUBSCRIBE") | Some("PUNSUBSCRIBE") => {
                Ok(encode_error("ERR Pattern subscriptions not implemented"))
            }
            Some(unknown_cmd) => {
                Ok(encode_error(&format!("ERR Can't execute '{}' only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context", unknown_cmd)))
            }
            _ => Err("ERR wrong number of arguments for 'psubscribe' command".to_string()),
        }
    }

    pub fn is_subscribed(&self) -> bool {
        !self.subscribed_channels.is_empty()
    }

    pub async fn run_loop_async(&mut self, stream: &mut tokio::net::TcpStream) {
        println!("+++++++++++ Entering subscription mode (async)");

        // Merge all channel receivers into one stream using tokio's select macro
        loop {
            let mut buffer = [0u8; 512];

            // Wait for either a message from any channel OR client input
            tokio::select! {
                result = Self::recv_from_any_channel(&mut self.subscribed_channels) => {
                    match result {
                        Some((channel, message)) => {
                            let response = vec![
                                "message".to_string(),
                                message.channel,
                                message.content,
                            ];
                            println!("+++++++++++ Received {:?} bytes from {}", response, channel);
                            if let Err(e) = stream.write_all(&encode_vec_as_bulk(response)).await {
                                eprintln!("Failed to write message to subscriber: {}", e);
                                return;
                            }
                        }
                        None => {
                            eprintln!("All channels closed");
                            return;
                        }
                    }
                }

                read_result = stream.read(&mut buffer) => {
                    match read_result {
                        Ok(size) if size > 0 => {
                            if let Some(decoded_command) = decode_resp_array(&buffer) {
                                match self.handle_command(decoded_command) {
                                    Ok(response) => {
                                        if let Err(e) = stream.write_all(&response).await {
                                            eprintln!("Failed to send response: {}", e);
                                            break;
                                        }

                                        if !self.is_subscribed() {
                                            println!("No channels left, exiting subscription mode");
                                            break;
                                        }
                                    }
                                    Err(msg) if msg == "QUIT" => {
                                        println!("Client requested QUIT");
                                        let _ = stream.write_all(&encode_str("OK")).await;
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
    }

    async fn recv_from_any_channel(
        channels: &mut HashMap<String, broadcast::Receiver<Message>>,
    ) -> Option<(String, Message)> {
        use futures::stream::{FuturesUnordered, StreamExt};

        if channels.is_empty() {
            std::future::pending::<()>().await;
            unreachable!();
        }

        let mut futures = FuturesUnordered::new();
        for (name, rx) in channels.iter_mut() {
            let name = name.clone();
            futures.push(async move { (name, rx.recv().await) });
        }

        while let Some((name, result)) = futures.next().await {
            if let Ok(msg) = result {
                return Some((name, msg));
            }
        }

        None
    }
}

pub fn subscribe(client_id: String, channel: String) -> Vec<u8> {
    let (_, count) = PUBSUB.subscribe(client_id, channel.clone());
    let vector: Vec<Value> = vec![
        Value::String("subscribe".to_string()),
        Value::String(channel),
        Value::Integer(count as i64),
    ];
    encode_vec_of_value(vector)
}
