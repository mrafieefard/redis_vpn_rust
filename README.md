# IranVPN Proxy

A combined SOCKS5 proxy implementation that uses Redis as a message broker to tunnel traffic between client and server components.

## Architecture

- **Client Mode**: Acts as a SOCKS5 proxy server that accepts client connections and forwards traffic through Redis
- **Server Mode**: Listens for Redis messages and establishes connections to remote destinations
- **Redis**: Acts as a message broker between client and server components

## Build

```bash
cargo build --release
```

## Usage

### Client Mode

The client mode runs a SOCKS5 proxy server that clients can connect to:

```bash
# Full configuration
./redis_vpn_rust client <redis_host> <redis_port> <listen_host> <listen_port>
./redis_vpn_rust client localhost 6379 127.0.0.1 1080

# Redis configuration only (listens on 127.0.0.1:1080 by default)  
./redis_vpn_rust client localhost 6379

# Interactive mode
./redis_vpn_rust client
```

### Server Mode  

The server mode connects to Redis and handles remote destination connections:

```bash
# Command line configuration
./redis_vpn_rust server <redis_host> <redis_port>
./redis_vpn_rust server localhost 6379

# Interactive mode
./redis_vpn_rust server
```

## Example Setup

1. Start Redis server:
   ```bash
   redis-server
   ```

2. Start the proxy server (handles remote connections):
   ```bash
   ./redis_vpn_rust server localhost 6379
   ```

3. Start the proxy client (accepts SOCKS5 connections):
   ```bash
   ./redis_vpn_rust client localhost 6379 127.0.0.1 1080
   ```

4. Configure your application to use SOCKS5 proxy at `127.0.0.1:1080`

## Architecture Flow

```
[SOCKS5 Client] → [Client Mode] → [Redis] → [Server Mode] → [Remote Destination]
                      ↑                                            ↓
                 [Listen 1080]                              [Connect to target]
```

1. SOCKS5 client connects to client mode on port 1080
2. Client mode publishes data to Redis channels
3. Server mode subscribes to Redis channels and forwards to remote destinations
4. Response data flows back through the same Redis channels

## Redis Channels

The system uses several Redis channels for communication:

- `socks5:control:requests` - Control requests (open/close connections)
- `socks5:control:responses` - Control responses 
- `socks5:data:c2s:{conn_id}` - Client-to-server data for each connection
- `socks5:data:s2c:{conn_id}` - Server-to-client data for each connection
- `socks5:ctrl:{conn_id}` - Control messages for specific connections

## Dependencies

- Redis server running and accessible
- Rust dependencies: redis, serde, serde_json, base64, uuid

## Features

- Full SOCKS5 protocol support
- Multiple concurrent connections
- Automatic connection cleanup
- Redis state clearing on server startup
- Detailed debug logging
- Command line and interactive configuration
- IPv4 and IPv6 support
- Domain name resolution