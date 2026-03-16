use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use log::{debug, info, warn, error};
use redis::Commands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Condvar, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

// Constants
const SOCKET_BUFFER: usize = 32768;
const OPEN_TIMEOUT_SECS: u64 = 15;

// Utility functions
fn b64e(data: &[u8]) -> String {
    B64.encode(data)
}

fn b64d(data: &str) -> io::Result<Vec<u8>> {
    B64.decode(data.as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("base64 decode error: {e}")))
}

fn prompt_input(label: &str) -> io::Result<String> {
    print!("{label}");
    io::stdout().flush()?;
    let mut s = String::new();
    io::stdin().read_line(&mut s)?;
    Ok(s.trim().to_string())
}

fn ask_redis_config() -> io::Result<(String, u16)> {
    let host = loop {
        let input = prompt_input("Redis host: ")?;
        if !input.is_empty() {
            break input;
        }
        eprintln!("host cannot be empty");
    };

    let port = loop {
        let input = prompt_input("Redis port: ")?;
        match input.parse::<u16>() {
            Ok(p) => break p,
            Err(_) => eprintln!("please enter a valid port number"),
        }
    };

    Ok((host, port))
}

fn ask_instance_id() -> io::Result<String> {
    let input = prompt_input("Server instance ID: ")?;
    if input.is_empty() {
        Err(io::Error::new(io::ErrorKind::InvalidInput, "instance ID cannot be empty"))
    } else {
        Ok(input)
    }
}

// Shared data structures
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ControlPayload {
    #[serde(rename = "type")]
    msg_type: String,
    conn_id: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    ok: Option<bool>,
    error: Option<String>,
    bind_host: Option<String>,
    bind_port: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ControlResponse {
    conn_id: String,
    ok: bool,
    bind_host: Option<String>,
    bind_port: Option<u16>,
    error: Option<String>,
    traceback: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DataPayload {
    #[serde(rename = "type")]
    msg_type: String,
    data_b64: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct OpenRequest {
    #[serde(rename = "type")]
    msg_type: String,
    conn_id: String,
    host: String,
    port: u16,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CloseRequest {
    #[serde(rename = "type")]
    msg_type: String,
    conn_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct DataMessage {
    #[serde(rename = "type")]
    msg_type: String,
    data_b64: Option<String>,
}

// CLIENT CODE
struct ResponseWaiter {
    responses: Arc<(Mutex<HashMap<String, ControlPayload>>, Condvar)>,
    instance_id: String,
}

impl ResponseWaiter {
    fn new(redis_url: String, instance_id: String) -> Self {
        let responses = Arc::new((Mutex::new(HashMap::new()), Condvar::new()));
        let responses_clone = Arc::clone(&responses);
        let instance_id_clone = instance_id.clone();

        thread::spawn(move || {
            let client = match redis::Client::open(redis_url.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("response waiter redis client error: {e}");
                    return;
                }
            };

            let mut conn = match client.get_connection() {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("response waiter redis connection error: {e}");
                    return;
                }
            };

            let control_response_stream = format!("socks5:control:responses:{}", instance_id_clone);
            let mut last_id = "0".to_string();

            loop {
                let reply: redis::RedisResult<redis::streams::StreamReadReply> = redis::cmd("XREAD")
                    .arg("BLOCK")
                    .arg(5000)
                    .arg("COUNT")
                    .arg(50)
                    .arg("STREAMS")
                    .arg(&control_response_stream)
                    .arg(&last_id)
                    .query(&mut conn);

                let reply = match reply {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("XREAD error: {e}");
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                };

                for key in reply.keys {
                    for id in key.ids {
                        last_id = id.id.clone();

                        let data_value = match id.map.get("data") {
                            Some(v) => v,
                            None => continue,
                        };

                        let data_str: String = match redis::from_redis_value(data_value) {
                            Ok(s) => s,
                            Err(_) => continue,
                        };

                        // Try parsing as ControlResponse first (from server)
                        let payload = if let Ok(resp) = serde_json::from_str::<ControlResponse>(&data_str) {
                            // Convert ControlResponse to ControlPayload format
                            ControlPayload {
                                msg_type: "response".to_string(),
                                conn_id: Some(resp.conn_id.clone()),
                                host: None,
                                port: None,
                                ok: Some(resp.ok),
                                error: resp.error,
                                bind_host: resp.bind_host,
                                bind_port: resp.bind_port,
                            }
                        } else if let Ok(p) = serde_json::from_str::<ControlPayload>(&data_str) {
                            p
                        } else {
                            println!("Failed to parse control message: {}", data_str);
                            continue;
                        };

                        let conn_id = match payload.conn_id.clone() {
                            Some(c) => c,
                            None => continue,
                        };

                        let (lock, cv) = &*responses_clone;
                        let mut guard = match lock.lock() {
                            Ok(g) => g,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        guard.insert(conn_id, payload);
                        cv.notify_all();
                    }
                }
            }
        });

        Self { responses, instance_id }
    }

    fn wait_for(&self, conn_id: &str, timeout: Duration) -> Option<ControlPayload> {
        let deadline = Instant::now() + timeout;
        let (lock, cv) = &*self.responses;
        let mut guard = match lock.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };

        loop {
            if let Some(payload) = guard.remove(conn_id) {
                return Some(payload);
            }

            let now = Instant::now();
            if now >= deadline {
                return None;
            }

            let wait_for = deadline.saturating_duration_since(now);
            let result = cv.wait_timeout(guard, wait_for);
            match result {
                Ok((g, _)) => guard = g,
                Err(poisoned) => {
                    let (g, _) = poisoned.into_inner();
                    guard = g;
                }
            }
        }
    }
}

struct TunnelSession {
    client_sock: TcpStream,
    client_addr: SocketAddr,
    conn_id: String,
    redis_url: String,
    waiter: Arc<ResponseWaiter>,
    closed: Arc<AtomicBool>,
    c2s_channel: String,
    s2c_channel: String,
    ctrl_channel: String,
    instance_id: String,
}

impl TunnelSession {
    fn new(client_sock: TcpStream, client_addr: SocketAddr, redis_url: String, waiter: Arc<ResponseWaiter>) -> Self {
        let conn_id = Uuid::new_v4().to_string();
        let instance_id = waiter.instance_id.clone();
        let c2s_channel = format!("socks5:data:c2s:{}:{}", instance_id, conn_id);
        let s2c_channel = format!("socks5:data:s2c:{}:{}", instance_id, conn_id);
        let ctrl_channel = format!("socks5:ctrl:{}:{}", instance_id, conn_id);

        Self {
            client_sock,
            client_addr,
            conn_id,
            redis_url,
            waiter,
            closed: Arc::new(AtomicBool::new(false)),
            c2s_channel,
            s2c_channel,
            ctrl_channel,
            instance_id,
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }

        if let Ok(client) = redis::Client::open(self.redis_url.as_str()) {
            if let Ok(mut conn) = client.get_connection() {
                let _ = conn.publish::<_, _, ()>(
                    &self.ctrl_channel,
                    serde_json::json!({ "type": "close" }).to_string(),
                );

                let control_request_stream = format!("socks5:control:requests:{}", self.instance_id);
                let _ : redis::RedisResult<String> = redis::cmd("XADD")
                    .arg(control_request_stream)
                    .arg("*")
                    .arg("data")
                    .arg(
                        serde_json::json!({
                            "type": "close",
                            "conn_id": self.conn_id
                        })
                        .to_string(),
                    )
                    .query(&mut conn);
            }
        }

        let _ = self.client_sock.shutdown(Shutdown::Both);
    }

    fn recv_exact(&mut self, n: usize) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; n];
        self.client_sock.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn send_socks_reply(&mut self, rep: u8, bind_host: &str, bind_port: u16) -> io::Result<()> {
        let mut packet = vec![0x05, rep, 0x00];

        if let Ok(ip) = IpAddr::from_str(bind_host) {
            match ip {
                IpAddr::V4(v4) => {
                    packet.push(0x01);
                    packet.extend_from_slice(&v4.octets());
                }
                IpAddr::V6(v6) => {
                    packet.push(0x04);
                    packet.extend_from_slice(&v6.octets());
                }
            }
        } else {
            let host_bytes = bind_host.as_bytes();
            if host_bytes.len() > 255 {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "domain name too long"));
            }
            packet.push(0x03);
            packet.push(host_bytes.len() as u8);
            packet.extend_from_slice(host_bytes);
        }

        packet.extend_from_slice(&bind_port.to_be_bytes());
        self.client_sock.write_all(&packet)?;
        Ok(())
    }

    fn handshake(&mut self) -> io::Result<()> {
        let head = self.recv_exact(2)?;
        let ver = head[0];
        let nmethods = head[1] as usize;

        if ver != 5 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not SOCKS5"));
        }

        let methods = self.recv_exact(nmethods)?;
        if !methods.contains(&0x00) {
            self.client_sock.write_all(&[0x05, 0xff])?;
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "client does not offer no-auth",
            ));
        }

        self.client_sock.write_all(&[0x05, 0x00])?;
        Ok(())
    }

    fn parse_request(&mut self) -> io::Result<(String, u16)> {
        let req = self.recv_exact(4)?;
        let ver = req[0];
        let cmd = req[1];
        let atyp = req[3];

        if ver != 5 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad version"));
        }
        if cmd != 0x01 {
            let _ = self.send_socks_reply(0x07, "0.0.0.0", 0);
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "only CONNECT supported",
            ));
        }

        let addr = match atyp {
            0x01 => {
                let raw = self.recv_exact(4)?;
                IpAddr::V4(Ipv4Addr::new(raw[0], raw[1], raw[2], raw[3])).to_string()
            }
            0x03 => {
                let len = self.recv_exact(1)?[0] as usize;
                let raw = self.recv_exact(len)?;
                String::from_utf8_lossy(&raw).to_string()
            }
            0x04 => {
                let raw = self.recv_exact(16)?;
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&raw);
                IpAddr::V6(Ipv6Addr::from(octets)).to_string()
            }
            _ => {
                let _ = self.send_socks_reply(0x08, "0.0.0.0", 0);
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad ATYP"));
            }
        };

        let port_bytes = self.recv_exact(2)?;
        let port = u16::from_be_bytes([port_bytes[0], port_bytes[1]]);
        Ok((addr, port))
    }

    fn open_remote(&mut self, host: &str, port: u16) -> io::Result<()> {
        eprintln!("[DEBUG] Attempting to open remote connection to {}:{} (conn_id: {})", host, port, self.conn_id);
        
        let client = redis::Client::open(self.redis_url.as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redis client error: {e}")))?;
        let mut conn = client
            .get_connection()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redis connection error: {e}")))?;

        let payload = serde_json::json!({
            "type": "open",
            "conn_id": self.conn_id,
            "host": host,
            "port": port,
        });

        let control_request_stream = format!("socks5:control:requests:{}", self.instance_id);
        eprintln!("[DEBUG] Sending control request: {}", payload.to_string());
        let _id: String = redis::cmd("XADD")
            .arg(control_request_stream)
            .arg("*")
            .arg("data")
            .arg(payload.to_string())
            .query(&mut conn)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("XADD error: {e}")))?;

        eprintln!("[DEBUG] Control request sent, waiting for response (conn_id: {})", self.conn_id);
        let resp = self
            .waiter
            .wait_for(&self.conn_id, Duration::from_secs(OPEN_TIMEOUT_SECS));

        let resp = match resp {
            Some(r) => {
                eprintln!("[DEBUG] Received response: {:?} (conn_id: {})", r, self.conn_id);
                r
            }
            None => {
                eprintln!("[ERROR] Remote open timeout for conn_id: {}", self.conn_id);
                let _ = self.send_socks_reply(0x01, "0.0.0.0", 0);
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "remote open timeout",
                ));
            }
        };

        if !resp.ok.unwrap_or(false) {
            let error_msg = resp.error.unwrap_or_else(|| "remote connect failed".to_string());
            eprintln!("[ERROR] Remote connection failed: {} (conn_id: {})", error_msg, self.conn_id);
            let _ = self.send_socks_reply(0x05, "0.0.0.0", 0);
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                error_msg,
            ));
        }

        eprintln!("[DEBUG] Remote connection successful (conn_id: {})", self.conn_id);
        self.send_socks_reply(
            0x00,
            resp.bind_host.as_deref().unwrap_or("0.0.0.0"),
            resp.bind_port.unwrap_or(0),
        )?;

        Ok(())
    }

    fn client_to_redis_loop(&self, mut read_sock: TcpStream) {
        let client = match redis::Client::open(self.redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("client_to_redis redis client error: {e}");
                self.close();
                return;
            }
        };

        let mut conn = match client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("client_to_redis redis connection error: {e}");
                self.close();
                return;
            }
        };

        let mut buf = vec![0u8; SOCKET_BUFFER];

        loop {
            if self.is_closed() {
                break;
            }

            match read_sock.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let payload = serde_json::json!({
                        "type": "data",
                        "data_b64": b64e(&buf[..n]),
                    });

                    let result: redis::RedisResult<i32> =
                        conn.publish(&self.c2s_channel, payload.to_string());

                    match result {
                        Ok(subscriber_count) => {
                            eprintln!("[DEBUG] Published {} bytes to {}, {} subscribers active", n, self.c2s_channel, subscriber_count);
                            if subscriber_count == 0 {
                                eprintln!("[WARNING] No subscribers for channel {}, data may be lost", self.c2s_channel);
                            }
                        }
                        Err(e) => {
                            eprintln!("[ERROR] Failed to publish to Redis channel {}: {}", self.c2s_channel, e);
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }

        self.close();
    }

    fn redis_to_client_loop(&self, mut write_sock: TcpStream) {
        let client = match redis::Client::open(self.redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("redis_to_client redis client error: {e}");
                self.close();
                return;
            }
        };

        let mut conn = match client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("redis_to_client redis connection error: {e}");
                self.close();
                return;
            }
        };

        let mut pubsub = conn.as_pubsub();
        if let Err(e) = pubsub.subscribe(&self.s2c_channel) {
            eprintln!("subscribe error: {e}");
            self.close();
            return;
        }
        if let Err(e) = pubsub.subscribe(&self.ctrl_channel) {
            eprintln!("subscribe error: {e}");
            self.close();
            return;
        }

        loop {
            if self.is_closed() {
                break;
            }

            let msg = match pubsub.get_message() {
                Ok(m) => m,
                Err(_) => break,
            };

            let channel: String = match msg.get_channel_name().to_string().parse() {
                Ok(c) => c,
                Err(_) => continue,
            };

            let payload_str: String = match msg.get_payload() {
                Ok(p) => p,
                Err(_) => continue,
            };

            if channel == self.ctrl_channel {
                let payload: serde_json::Value = match serde_json::from_str(&payload_str) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if payload.get("type").and_then(|v| v.as_str()) == Some("close") {
                    break;
                }
                continue;
            }

            if channel == self.s2c_channel {
                let payload: DataPayload = match serde_json::from_str(&payload_str) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                match payload.msg_type.as_str() {
                    "data" => {
                        if let Some(data_b64) = payload.data_b64.as_deref() {
                            match b64d(data_b64) {
                                Ok(bytes) => {
                                    if write_sock.write_all(&bytes).is_err() {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                    "close" => break,
                    _ => {}
                }
            }
        }

        self.close();
    }

    fn run(mut self) {
        let result = (|| -> io::Result<()> {
            self.handshake()?;
            let (host, port) = self.parse_request()?;
            self.open_remote(&host, port)?;

            let read_sock = self.client_sock.try_clone()?;
            let write_sock = self.client_sock.try_clone()?;

            let session_for_c2r = self.clone_for_thread();
            let session_for_r2c = self.clone_for_thread();

            let t1 = thread::spawn(move || {
                session_for_c2r.client_to_redis_loop(read_sock);
            });

            let t2 = thread::spawn(move || {
                session_for_r2c.redis_to_client_loop(write_sock);
            });

            let _ = t1.join();
            let _ = t2.join();

            Ok(())
        })();

        if let Err(e) = result {
            eprintln!("[{}] session error: {}", self.client_addr, e);
            self.close();
        }
    }

    fn clone_for_thread(&self) -> Self {
        Self {
            client_sock: self.client_sock.try_clone().expect("failed to clone client socket"),
            client_addr: self.client_addr,
            conn_id: self.conn_id.clone(),
            redis_url: self.redis_url.clone(),
            waiter: Arc::clone(&self.waiter),
            closed: Arc::clone(&self.closed),
            c2s_channel: self.c2s_channel.clone(),
            s2c_channel: self.s2c_channel.clone(),
            ctrl_channel: self.ctrl_channel.clone(),
            instance_id: self.instance_id.clone(),
        }
    }
}

fn run_client(redis_url: String, listen_host: &str, listen_port: u16, instance_id: String) -> io::Result<()> {
    let waiter = Arc::new(ResponseWaiter::new(redis_url.clone(), instance_id.clone()));
    let listener = TcpListener::bind((listen_host, listen_port))?;
    println!("SOCKS5 Redis client proxy listening on {}:{}", listen_host, listen_port);
    println!("Connected to server instance: {}", instance_id);

    for stream in listener.incoming() {
        match stream {
            Ok(client_sock) => {
                let client_addr = match client_sock.peer_addr() {
                    Ok(addr) => addr,
                    Err(e) => {
                        eprintln!("peer_addr error: {e}");
                        continue;
                    }
                };

                let session = TunnelSession::new(
                    client_sock,
                    client_addr,
                    redis_url.clone(),
                    Arc::clone(&waiter),
                );

                thread::spawn(move || {
                    session.run();
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }

    Ok(())
}

// SERVER CODE
#[derive(Clone)]
struct Tunnel {
    conn_id: String,
    remote_host: String,
    remote_port: u16,
    redis_url: String,
    closed: Arc<AtomicBool>,
    remote_sock: Arc<Mutex<Option<TcpStream>>>,
    c2s_channel: String,
    s2c_channel: String,
    ctrl_channel: String,
    instance_id: String,
}

impl Tunnel {
    fn new(conn_id: String, remote_host: String, remote_port: u16, redis_url: String, instance_id: String) -> Self {
        Self {
            c2s_channel: format!("socks5:data:c2s:{}:{}", instance_id, conn_id),
            s2c_channel: format!("socks5:data:s2c:{}:{}", instance_id, conn_id),
            ctrl_channel: format!("socks5:ctrl:{}:{}", instance_id, conn_id),
            conn_id,
            remote_host,
            remote_port,
            redis_url,
            closed: Arc::new(AtomicBool::new(false)),
            remote_sock: Arc::new(Mutex::new(None)),
            instance_id,
        }
    }

    fn connect_remote(&self) -> io::Result<()> {
        println!("[DEBUG] Attempting to connect to remote {}:{} (conn_id: {})", self.remote_host, self.remote_port, self.conn_id);
        
        // Try to connect with retry logic
        let mut attempts = 0;
        let max_attempts = 3;
        let mut sock = None;
        
        while attempts < max_attempts {
            attempts += 1;
            println!("[DEBUG] Connection attempt {} to {}:{} (conn_id: {})", attempts, self.remote_host, self.remote_port, self.conn_id);
            
            // First resolve the address, then connect with timeout
            let address = format!("{}:{}", self.remote_host, self.remote_port);
            let socket_addrs: Vec<std::net::SocketAddr> = match address.to_socket_addrs() {
                Ok(addrs) => addrs.collect(),
                Err(e) => {
                    println!("[DEBUG] DNS resolution failed for {}: {} (conn_id: {})", address, e, self.conn_id);
                    if attempts >= max_attempts {
                        return Err(e);
                    }
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
            };
            
            // Try connecting to the first resolved address
            if let Some(socket_addr) = socket_addrs.first() {
                println!("[DEBUG] Resolved {} to {} (conn_id: {})", address, socket_addr, self.conn_id);
                match TcpStream::connect_timeout(socket_addr, Duration::from_secs(10)) {
                    Ok(s) => {
                        sock = Some(s);
                        break;
                    },
                    Err(e) => {
                        println!("[DEBUG] Connection attempt {} failed: {} (conn_id: {})", attempts, e, self.conn_id);
                        if attempts >= max_attempts {
                            return Err(e);
                        }
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            } else {
                let err = io::Error::new(io::ErrorKind::AddrNotAvailable, "No addresses resolved");
                println!("[DEBUG] No addresses resolved for {} (conn_id: {})", address, self.conn_id);
                if attempts >= max_attempts {
                    return Err(err);
                }
                thread::sleep(Duration::from_secs(1));
            }
        }
        
        let sock = sock.unwrap();
        println!("[DEBUG] Successfully connected to remote {}:{} (conn_id: {})", self.remote_host, self.remote_port, self.conn_id);
        
        // Set more generous timeouts
        sock.set_read_timeout(Some(Duration::from_secs(30)))?;
        sock.set_write_timeout(Some(Duration::from_secs(30)))?;
        
        // Enable TCP keepalive
        sock.set_nodelay(true)?;

        let mut guard = match self.remote_sock.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = Some(sock);
        println!("[DEBUG] Remote socket stored successfully (conn_id: {})", self.conn_id);
        Ok(())
    }

    fn start(&self) -> io::Result<()> {
        println!("[DEBUG] Starting tunnel for conn_id: {}", self.conn_id);
        self.connect_remote()?;

        let t1 = self.clone();
        let t2 = self.clone();

        println!("[DEBUG] Spawning remote_to_redis_loop thread (conn_id: {})", self.conn_id);
        thread::spawn(move || t1.remote_to_redis_loop());
        
        println!("[DEBUG] Spawning redis_to_remote_loop thread (conn_id: {})", self.conn_id);
        thread::spawn(move || t2.redis_to_remote_loop());

        println!("[DEBUG] Tunnel threads started successfully (conn_id: {})", self.conn_id);
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn publish_to_client(&self, payload: &serde_json::Value) {
        let client = match redis::Client::open(self.redis_url.as_str()) {
            Ok(c) => c,
            Err(_) => return,
        };
        let mut conn = match client.get_connection() {
            Ok(c) => c,
            Err(_) => return,
        };

        let _: redis::RedisResult<i32> = conn.publish(&self.s2c_channel, payload.to_string());
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }

        {
            let mut guard = match self.remote_sock.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(sock) = guard.as_mut() {
                let _ = sock.shutdown(Shutdown::Both);
            }
            *guard = None;
        }

        self.publish_to_client(&serde_json::json!({ "type": "close" }));
    }

    fn remote_to_redis_loop(&self) {
        println!("[DEBUG] Starting remote_to_redis_loop (conn_id: {})", self.conn_id);
        let client = match redis::Client::open(self.redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                println!("[DEBUG] Failed to open Redis client in remote_to_redis_loop: {} (conn_id: {})", e, self.conn_id);
                self.close();
                return;
            }
        };
        let mut redis_conn = match client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                println!("[DEBUG] Failed to get Redis connection in remote_to_redis_loop: {} (conn_id: {})", e, self.conn_id);
                self.close();
                return;
            }
        };
        println!("[DEBUG] Redis connection established for remote_to_redis_loop (conn_id: {})", self.conn_id);

        let mut local_sock = {
            let guard = match self.remote_sock.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            match guard.as_ref() {
                Some(s) => match s.try_clone() {
                    Ok(c) => c,
                    Err(_) => {
                        self.close();
                        return;
                    }
                },
                None => {
                    self.close();
                    return;
                }
            }
        };

        let mut buf = vec![0u8; SOCKET_BUFFER];
        println!("[DEBUG] Starting read loop from remote socket (conn_id: {})", self.conn_id);

        loop {
            if self.is_closed() {
                println!("[DEBUG] Tunnel is closed, exiting remote_to_redis_loop (conn_id: {})", self.conn_id);
                break;
            }

            match local_sock.read(&mut buf) {
                Ok(0) => {
                    println!("[DEBUG] Remote socket closed (conn_id: {})", self.conn_id);
                    break;
                },
                Ok(n) => {
                    println!("[DEBUG] Read {} bytes from remote socket (conn_id: {})", n, self.conn_id);
                    
                    let payload = serde_json::json!({
                        "type": "data",
                        "data_b64": b64e(&buf[..n]),
                    });
                    let res: redis::RedisResult<i32> =
                        redis_conn.publish(&self.s2c_channel, payload.to_string());
                    if res.is_err() {
                        println!("[DEBUG] Failed to publish to Redis (conn_id: {})", self.conn_id);
                        break;
                    } else {
                        println!("[DEBUG] Published {} bytes to Redis channel {} (conn_id: {})", n, self.s2c_channel, self.conn_id);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
                    // This is normal for non-blocking reads, just continue
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(e) => {
                    println!("[DEBUG] Read error from remote socket: {} (conn_id: {})", e, self.conn_id);
                    break;
                }
            }
        }

        self.close();
    }

    fn redis_to_remote_loop(&self) {
        println!("[DEBUG] Starting redis_to_remote_loop (conn_id: {})", self.conn_id);
        let client = match redis::Client::open(self.redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                println!("[DEBUG] Failed to open Redis client in redis_to_remote_loop: {} (conn_id: {})", e, self.conn_id);
                self.close();
                return;
            }
        };
        let mut conn = match client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                println!("[DEBUG] Failed to get Redis connection in redis_to_remote_loop: {} (conn_id: {})", e, self.conn_id);
                self.close();
                return;
            }
        };

        // Set a longer read timeout to avoid frequent timeouts
        if let Err(e) = conn.set_read_timeout(Some(Duration::from_secs(30))) {
            println!("[DEBUG] Failed to set read timeout: {} (conn_id: {})", e, self.conn_id);
        }

        let mut pubsub = conn.as_pubsub();
        println!("[DEBUG] Subscribing to c2s channel: {} (conn_id: {})", self.c2s_channel, self.conn_id);
        if pubsub.subscribe(&self.c2s_channel).is_err() {
            println!("[DEBUG] Failed to subscribe to c2s channel (conn_id: {})", self.conn_id);
            self.close();
            return;
        }
        println!("[DEBUG] Subscribing to ctrl channel: {} (conn_id: {})", self.ctrl_channel, self.conn_id);
        if pubsub.subscribe(&self.ctrl_channel).is_err() {
            println!("[DEBUG] Failed to subscribe to ctrl channel (conn_id: {})", self.conn_id);
            self.close();
            return;
        }
        println!("[DEBUG] Successfully subscribed to both channels, waiting for messages (conn_id: {})", self.conn_id);

        let mut local_sock = {
            let guard = match self.remote_sock.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            match guard.as_ref() {
                Some(s) => match s.try_clone() {
                    Ok(c) => c,
                    Err(_) => {
                        self.close();
                        return;
                    }
                },
                None => {
                    self.close();
                    return;
                }
            }
        };

        // Set non-blocking mode on socket for better control
        if local_sock.set_nonblocking(true).is_err() {
            println!("[DEBUG] Failed to set socket non-blocking (conn_id: {})", self.conn_id);
        }

        let mut message_count = 0;
        let mut last_activity = std::time::Instant::now();

        loop {
            if self.is_closed() {
                println!("[DEBUG] Tunnel is closed, exiting redis_to_remote_loop (conn_id: {})", self.conn_id);
                break;
            }

            // Only show "waiting" message every 10th attempt to reduce noise
            if message_count % 10 == 0 {
                println!("[DEBUG] Attempt {} - Waiting for Redis pubsub message (conn_id: {})", message_count + 1, self.conn_id);
            }
            
            // Use connection timeout to avoid infinite blocking
            let msg = match pubsub.get_message() {
                Ok(m) => {
                    message_count += 1;
                    last_activity = std::time::Instant::now();
                    println!("[DEBUG] SUCCESS: Received message #{} from Redis pubsub (conn_id: {})", message_count, self.conn_id);
                    m
                },
                Err(e) => {
                    // Check the type of error
                    let error_str = e.to_string();
                    if error_str.contains("10060") || error_str.contains("timeout") || error_str.contains("timed out") {
                        // This is a normal timeout - no messages received, which is expected
                        if last_activity.elapsed() > Duration::from_secs(30) {
                            println!("[DEBUG] No messages for {:?}, still waiting... (conn_id: {})", last_activity.elapsed(), self.conn_id);
                        }
                    } else {
                        println!("[DEBUG] Pubsub error: {} (conn_id: {})", e, self.conn_id);
                    }
                    
                    // Check if we should give up after too long without messages
                    if last_activity.elapsed() > Duration::from_secs(300) && message_count == 0 {
                        println!("[DEBUG] No messages received for 5 minutes, connection may be broken (conn_id: {})", self.conn_id);
                        break;
                    }
                    continue;
                }
            };

            let channel = msg.get_channel_name().to_string();
            println!("[DEBUG] Message from channel: {} (conn_id: {})", channel, self.conn_id);
            
            let payload_str: String = match msg.get_payload() {
                Ok(p) => {
                    println!("[DEBUG] Raw payload: {} (conn_id: {})", p, self.conn_id);
                    p
                },
                Err(e) => {
                    println!("[DEBUG] Failed to get payload: {} (conn_id: {})", e, self.conn_id);
                    continue;
                }
            };

            let payload: serde_json::Value = match serde_json::from_str(&payload_str) {
                Ok(v) => {
                    println!("[DEBUG] Parsed payload JSON: {:?} (conn_id: {})", v, self.conn_id);
                    v
                },
                Err(e) => {
                    println!("[DEBUG] Failed to parse JSON payload: {} (conn_id: {})", e, self.conn_id);
                    continue;
                }
            };

            if channel == self.ctrl_channel {
                println!("[DEBUG] Received control message (conn_id: {})", self.conn_id);
                if payload.get("type").and_then(|v| v.as_str()) == Some("close") {
                    println!("[DEBUG] Received close command from control channel (conn_id: {})", self.conn_id);
                    break;
                }
                continue;
            }

            if channel == self.c2s_channel {
                match payload.get("type").and_then(|v| v.as_str()) {
                    Some("data") => {
                        println!("[DEBUG] *** PROCESSING DATA PACKET *** (conn_id: {})", self.conn_id);
                        let Some(data_b64) = payload.get("data_b64").and_then(|v| v.as_str()) else {
                            println!("[DEBUG] Missing data_b64 field (conn_id: {})", self.conn_id);
                            continue;
                        };
                        let bytes = match b64d(data_b64) {
                            Ok(b) => b,
                            Err(e) => {
                                println!("[DEBUG] Base64 decode error: {} (conn_id: {})", e, self.conn_id);
                                break;
                            }
                        };
                        println!("[DEBUG] Writing {} bytes to remote socket (conn_id: {})", bytes.len(), self.conn_id);
                        
                        // Reset socket to blocking for write
                        if local_sock.set_nonblocking(false).is_err() {
                            println!("[DEBUG] Failed to set socket blocking for write (conn_id: {})", self.conn_id);
                        }
                        
                        if local_sock.write_all(&bytes).is_err() {
                            println!("[DEBUG] Failed to write to remote socket (conn_id: {})", self.conn_id);
                            break;
                        } else {
                            println!("[DEBUG] *** SUCCESSFULLY WROTE {} BYTES TO DESTINATION *** (conn_id: {})", bytes.len(), self.conn_id);
                            
                            // Try to flush the socket to ensure data is sent immediately
                            if let Err(e) = local_sock.flush() {
                                println!("[DEBUG] Failed to flush socket: {} (conn_id: {})", e, self.conn_id);
                            }
                        }
                        
                        // Set back to non-blocking
                        if local_sock.set_nonblocking(true).is_err() {
                            println!("[DEBUG] Failed to set socket non-blocking after write (conn_id: {})", self.conn_id);
                        }
                    }
                    Some("close") => {
                        println!("[DEBUG] Received close command from c2s channel (conn_id: {})", self.conn_id);
                        break;
                    },
                    other => {
                        println!("[DEBUG] Received unknown message type: {:?} from c2s channel (conn_id: {})", other, self.conn_id);
                    }
                }
            }
        }

        println!("[DEBUG] Exiting redis_to_remote_loop after processing {} messages (conn_id: {})", message_count, self.conn_id);
        self.close();
    }
}

struct Server {
    redis_url: String,
    tunnels: Arc<Mutex<HashMap<String, Tunnel>>>,
    instance_id: String,
}

impl Server {
    fn new(redis_url: String, instance_id: String) -> Self {
        Self {
            redis_url,
            tunnels: Arc::new(Mutex::new(HashMap::new())),
            instance_id,
        }
    }

    fn clear_redis_state(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("[DEBUG] Clearing Redis state for instance {}...", self.instance_id);
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_connection()?;

        let control_request_stream = format!("socks5:control:requests:{}", self.instance_id);
        let control_response_stream = format!("socks5:control:responses:{}", self.instance_id);

        // Clear control streams for this instance
        println!("[DEBUG] Clearing control request stream: {}", control_request_stream);
        let _: redis::RedisResult<i32> = redis::cmd("DEL")
            .arg(&control_request_stream)
            .query(&mut conn);

        println!("[DEBUG] Clearing control response stream: {}", control_response_stream);
        let _: redis::RedisResult<i32> = redis::cmd("DEL")
            .arg(&control_response_stream)
            .query(&mut conn);

        // Clear any existing socks5 related keys for this instance
        let pattern = format!("socks5:*:{}:*", self.instance_id);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query(&mut conn)
            .unwrap_or_default();
        
        if !keys.is_empty() {
            println!("[DEBUG] Clearing {} existing socks5 keys for instance {}", keys.len(), self.instance_id);
            for key in &keys {
                println!("[DEBUG] Deleting key: {}", key);
            }
            let _: redis::RedisResult<i32> = redis::cmd("DEL")
                .arg(&keys)
                .query(&mut conn);
        }

        println!("[DEBUG] Redis state cleared successfully for instance {}", self.instance_id);
        Ok(())
    }

    fn send_control_response(&self, payload: &ControlResponse) {
        let client = match redis::Client::open(self.redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("redis client open error: {e}");
                return;
            }
        };

        let mut conn = match client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("redis get_connection error: {e}");
                return;
            }
        };

        let data = match serde_json::to_string(payload) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("serialize response error: {e}");
                return;
            }
        };

        let result: redis::RedisResult<String> = redis::cmd("XADD")
            .arg(format!("socks5:control:responses:{}", self.instance_id))
            .arg("*")
            .arg("data")
            .arg(data)
            .query(&mut conn);

        if let Err(e) = result {
            eprintln!("XADD response error: {e}");
        }
    }

    fn handle_open(&self, req: OpenRequest) {
        let conn_id = req.conn_id.clone();
        let tunnel = Tunnel::new(
            req.conn_id.clone(),
            req.host.clone(),
            req.port,
            self.redis_url.clone(),
            self.instance_id.clone(),
        );

        match tunnel.start() {
            Ok(()) => {
                {
                    let mut guard = match self.tunnels.lock() {
                        Ok(g) => g,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    guard.insert(conn_id.clone(), tunnel);
                }

                self.send_control_response(&ControlResponse {
                    conn_id,
                    ok: true,
                    bind_host: Some("0.0.0.0".to_string()),
                    bind_port: Some(0),
                    error: None,
                    traceback: None,
                });
            }
            Err(e) => {
                self.send_control_response(&ControlResponse {
                    conn_id: conn_id.clone(),
                    ok: false,
                    bind_host: None,
                    bind_port: None,
                    error: Some(format!("{}: {}", std::any::type_name::<io::Error>(), e)),
                    traceback: None,
                });
                tunnel.close();
            }
        }
    }

    fn handle_close(&self, req: CloseRequest) {
        let tunnel = {
            let mut guard = match self.tunnels.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.remove(&req.conn_id)
        };

        if let Some(tunnel) = tunnel {
            tunnel.close();
        }
    }

    fn run(&self) {
        // Clear Redis state before starting
        if let Err(e) = self.clear_redis_state() {
            eprintln!("Failed to clear Redis state: {e}");
            return;
        }

        let client = match redis::Client::open(self.redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("redis client open error: {e}");
                return;
            }
        };

        let mut conn = match client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("redis get_connection error: {e}");
                return;
            }
        };

        let control_request_stream = format!("socks5:control:requests:{}", self.instance_id);
        let mut last_id = "0".to_string();
        println!("SOCKS5 Redis server started with instance ID: {}", self.instance_id);
        println!("Clients should use this instance ID to connect: {}", self.instance_id);

        loop {
            println!("[DEBUG] Waiting for new packets from Redis stream: {}", control_request_stream);
            let reply: redis::RedisResult<redis::streams::StreamReadReply> = redis::cmd("XREAD")
                .arg("BLOCK")
                .arg(5000)
                .arg("COUNT")
                .arg(20)
                .arg("STREAMS")
                .arg(&control_request_stream)
                .arg(&last_id)
                .query(&mut conn);

            let reply = match reply {
                Ok(r) => {
                    println!("[DEBUG] Received {} streams from Redis", r.keys.len());
                    r
                },
                Err(e) => {
                    eprintln!("XREAD error: {e}");
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
            };

            for key in reply.keys {
                println!("[DEBUG] Processing stream key: {} with {} entries", key.key, key.ids.len());
                for entry in key.ids {
                    last_id = entry.id.clone();
                    println!("[DEBUG] Processing entry ID: {}", entry.id);

                    let Some(data_val) = entry.map.get("data") else {
                        println!("[DEBUG] Entry missing 'data' field, skipping");
                        continue;
                    };

                    let data_str: String = match redis::from_redis_value(data_val) {
                        Ok(s) => {
                            println!("[DEBUG] Received packet data: {}", s);
                            s
                        },
                        Err(e) => {
                            eprintln!("redis value parse error: {e}");
                            continue;
                        }
                    };

                    let value: serde_json::Value = match serde_json::from_str(&data_str) {
                        Ok(v) => {
                            println!("[DEBUG] Parsed JSON successfully: {:?}", v);
                            v
                        },
                        Err(e) => {
                            eprintln!("json parse error: {e}");
                            continue;
                        }
                    };

                    match value.get("type").and_then(|v| v.as_str()) {
                        Some("open") => {
                            println!("[DEBUG] Processing OPEN request");
                            match serde_json::from_value::<OpenRequest>(value) {
                                Ok(req) => {
                                    println!("[DEBUG] Open request for {}:{} (conn_id: {})", req.host, req.port, req.conn_id);
                                    self.handle_open(req)
                                },
                                Err(e) => eprintln!("open request parse error: {e}"),
                            }
                        },
                        Some("close") => {
                            println!("[DEBUG] Processing CLOSE request");
                            match serde_json::from_str::<CloseRequest>(&data_str) {
                                Ok(req) => {
                                    println!("[DEBUG] Close request for conn_id: {}", req.conn_id);
                                    self.handle_close(req)
                                },
                                Err(e) => eprintln!("close request parse error: {e}"),
                            }
                        },
                        other => {
                            eprintln!("unknown control request type: {:?}", other);
                        }
                    }
                }
            }
        }
    }
}

fn run_server(redis_url: String, instance_id: String) -> io::Result<()> {
    let server = Server::new(redis_url, instance_id);
    server.run();
    Ok(())
}

fn parse_log_level(level_str: &str) -> Option<log::LevelFilter> {
    match level_str.to_uppercase().as_str() {
        "DEBUG" => Some(log::LevelFilter::Debug),
        "INFO" => Some(log::LevelFilter::Info),
        "WARN" | "WARNING" => Some(log::LevelFilter::Warn),
        "ERROR" => Some(log::LevelFilter::Error),
        _ => None,
    }
}

fn init_logger(level: log::LevelFilter) {
    env_logger::Builder::new()
        .filter_level(level)
        .format_timestamp_secs()
        .init();
}

fn print_usage(program_name: &str) {
    eprintln!("Usage:");
    eprintln!("  {} [--log-level LEVEL] client [redis_host] [redis_port] [listen_host] [listen_port] [instance_id]", program_name);
    eprintln!("  {} [--log-level LEVEL] client [redis_host] [redis_port] [instance_id]", program_name);
    eprintln!("  {} [--log-level LEVEL] server [redis_host] [redis_port]", program_name);
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --log-level LEVEL, -l LEVEL   Set logging level (DEBUG, INFO, WARN, ERROR) [default: INFO]");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  {} --log-level DEBUG client localhost 6379 127.0.0.1 1080 abc123-def456", program_name);
    eprintln!("  {} -l WARN client localhost 6379 abc123-def456", program_name);
    eprintln!("  {} --log-level ERROR server localhost 6379", program_name);
    eprintln!("  {} client   # Interactive mode for client", program_name);
    eprintln!("  {} server   # Interactive mode for server", program_name);
    eprintln!();
    eprintln!("Modes:");
    eprintln!("  client - Run as SOCKS5 proxy client (listens for client connections)");
    eprintln!("  server - Run as SOCKS5 proxy server (connects to remote destinations)");
    eprintln!();
    eprintln!("Note: The server will generate and display an instance ID when started.");
    eprintln!("      Clients must use this instance ID to connect to the correct server.");;
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    
    // Parse log level argument
    let mut log_level = log::LevelFilter::Info; // Default to INFO
    let mut remaining_args = args.clone();
    let mut i = 1;
    
    while i < remaining_args.len() {
        match remaining_args[i].as_str() {
            "--log-level" | "-l" => {
                if i + 1 < remaining_args.len() {
                    match parse_log_level(&remaining_args[i + 1]) {
                        Some(level) => {
                            log_level = level;
                            remaining_args.remove(i + 1); // Remove log level value
                            remaining_args.remove(i); // Remove --log-level flag
                            continue; // Don't increment i since we removed items
                        }
                        None => {
                            eprintln!("Error: Invalid log level '{}'. Must be DEBUG, INFO, WARN, or ERROR.", remaining_args[i + 1]);
                            print_usage(&args[0]);
                            std::process::exit(1);
                        }
                    }
                } else {
                    eprintln!("Error: --log-level requires a value");
                    print_usage(&args[0]);
                    std::process::exit(1);
                }
            }
            _ => {
                i += 1;
            }
        }
    }
    
    // Initialize logger
    init_logger(log_level);
    info!("Starting application with log level: {}", log_level);
    
    if remaining_args.len() < 2 {
        print_usage(&args[0]);
        std::process::exit(1);
    }

    let mode = &remaining_args[1];
    
    match mode.as_str() {
        "client" => {
            let (redis_host, redis_port, listen_host, listen_port, instance_id) = if remaining_args.len() >= 7 {
                // Full arguments: program client redis_host redis_port listen_host listen_port instance_id
                let redis_host = remaining_args[2].clone();
                let redis_port = match remaining_args[3].parse::<u16>() {
                    Ok(p) => p,
                    Err(_) => {
                        error!("Invalid Redis port number '{}'", remaining_args[3]);
                        print_usage(&args[0]);
                        std::process::exit(1);
                    }
                };
                let listen_host = remaining_args[4].clone();
                let listen_port = match remaining_args[5].parse::<u16>() {
                    Ok(p) => p,
                    Err(_) => {
                        error!("Invalid listen port number '{}'", remaining_args[5]);
                        print_usage(&args[0]);
                        std::process::exit(1);
                    }
                };
                let instance_id = remaining_args[6].clone();
                
                info!("Client mode - Redis: {}:{}, Listen: {}:{}, Instance: {}", redis_host, redis_port, listen_host, listen_port, instance_id);
                (redis_host, redis_port, listen_host, listen_port, instance_id)
            } else if remaining_args.len() >= 5 {
                // Redis + instance: program client redis_host redis_port instance_id
                let redis_host = remaining_args[2].clone();
                let redis_port = match remaining_args[3].parse::<u16>() {
                    Ok(p) => p,
                    Err(_) => {
                        error!("Invalid Redis port number '{}'", remaining_args[3]);
                        print_usage(&args[0]);
                        std::process::exit(1);
                    }
                };
                let instance_id = remaining_args[4].clone();
                
                info!("Client mode - Redis: {}:{}, Listen: 127.0.0.1:1080 (default), Instance: {}", redis_host, redis_port, instance_id);
                (redis_host, redis_port, "127.0.0.1".to_string(), 1080, instance_id)
            } else {
                // Interactive mode
                info!("Client mode - Interactive configuration");
                let (redis_host, redis_port) = ask_redis_config()?;
                let instance_id = ask_instance_id()?;
                info!("SOCKS5 proxy will listen on: 127.0.0.1:1080 (default)");
                (redis_host, redis_port, "127.0.0.1".to_string(), 1080, instance_id)
            };
            
            let redis_url = format!("redis://{}:{}/", redis_host, redis_port);
            run_client(redis_url, &listen_host, listen_port, instance_id)
        },
        "server" => {
            let (redis_host, redis_port) = if remaining_args.len() >= 4 {
                // Command line args: program server redis_host redis_port
                let redis_host = remaining_args[2].clone();
                let redis_port = match remaining_args[3].parse::<u16>() {
                    Ok(p) => p,
                    Err(_) => {
                        error!("Invalid Redis port number '{}'", remaining_args[3]);
                        print_usage(&args[0]);
                        std::process::exit(1);
                    }
                };
                
                info!("Server mode - Redis: {}:{}", redis_host, redis_port);
                (redis_host, redis_port)
            } else {
                // Interactive mode
                info!("Server mode - Interactive configuration");
                ask_redis_config()?
            };
            
            // Generate a unique instance ID for this server
            let instance_id = Uuid::new_v4().to_string();
            let redis_url = format!("redis://{}:{}/", redis_host, redis_port);
            run_server(redis_url, instance_id)
        },
        _ => {
            error!("Invalid mode '{}'. Must be 'client' or 'server'.", mode);
            print_usage(&args[0]);
            std::process::exit(1);
        }
    }
}