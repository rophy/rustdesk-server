# WebSocket/TCP Registration for Native Peers

**Issue:** [#3](https://github.com/rophy/rustdesk-server/issues/3)
**Date:** 2026-08-22

## Problem

The OSS rendezvous server (`hbbs`) rejects native peer registration over TCP/WebSocket with `NOT_SUPPORT`, forcing peers to use UDP. This prevents native peers from working in corporate environments where UDP is blocked (firewall egress rules, symmetric NAT, mandatory proxies).

The client already supports TCP-only mode (`allow-websocket=Y`, `disable-udp=Y`), but the server explicitly rejects it.

## Goal

Enable native peers to register and receive relay signals over TCP/WebSocket, so they work in UDP-blocked environments.

## Design

### 1. WS Peer Sink Storage

Add a new field on `RendezvousServer`:

```rust
ws_peers: Arc<Mutex<HashMap<String, Sink>>>,
```

Keyed by peer ID. Stores persistent WS/TCP sinks for TCP-registered peers. `Sink` stays defined in `rendezvous_server.rs`; the `Peer` struct is unchanged.

When the server needs to send a message to a peer, it checks `ws_peers` for a sink first. If not found, it falls back to UDP via `socket_addr` and `self.tx`.

### 2. Accept Registration over TCP/WS

Replace the `NOT_SUPPORT` response for `RegisterPk` over TCP/WS (`rendezvous_server.rs:612-619`) with the same validation and registration logic used in the UDP path:

- Validate uuid/pk not empty, id length >= 6
- Check IP blocker rate limiting
- Get or create peer via `pm.get_or(&id)`
- Validate uuid match and ip/pk consistency
- Call `pm.update_pk()` to update peer state
- Store the sink in `ws_peers` keyed by peer ID
- Send back `RegisterPkResponse` with `Result::OK`

Also accept `RegisterPeer` over TCP/WS (currently only handled in `handle_udp`):

- Look up peer, update `socket_addr` and `last_reg_time`
- Send back `RegisterPeerResponse`

Extract shared registration methods so both UDP and TCP/WS paths call the same code.

### 3. Persistent Connection Loop

Currently `handle_listener_inner` uses a hard 30s read timeout per message. For WS-registered peers, this is replaced with a persistent loop.

After a successful `RegisterPk` over TCP/WS:

1. Keep the connection alive (don't exit the message loop)
2. Wait for messages with a ~20s timeout
3. On timeout: send an empty-bytes heartbeat to the client
4. On empty-bytes echo from client: update `last_reg_time` on the peer
5. On `RegisterPk`: re-register (updates `last_reg_time`)
6. On other messages (`PunchHoleRequest`, `RequestRelay`, etc.): handle normally
7. On connection close or send error: remove from `ws_peers`, exit loop

The connection loop holds the stream (read side). The sink (write side) is stored in `ws_peers` and used by other tasks to push messages to the peer.

#### Heartbeat Timing

- Server sends heartbeat every ~20s (on read timeout)
- Client echoes back empty bytes, resetting `last_reg_time`
- `REG_TIMEOUT` is 30s, so 20s heartbeat interval is well within the window
- Client's default keep_alive is 60s (disconnects after 90s of silence), so 20s heartbeats are also well within the client's tolerance

### 4. Relay Signaling via WebSocket

When forwarding `RequestRelay` or `PunchHoleSent` to a target peer:

1. Check `ws_peers` for the target peer ID
2. If found: send through the WS sink. On failure, remove the entry from `ws_peers` and do step 3
3. If not found: send via UDP (existing behavior)

### 5. Last Registration Wins

- When a peer registers via TCP/WS: store sink in `ws_peers`, update `socket_addr` to TCP source address
- When a peer re-registers via UDP: remove any existing `ws_peers` entry for that peer ID
- No dual-mode: whichever registration came last is the active one

### 6. Cleanup

- **On send failure:** remove sink from `ws_peers`
- **On connection close:** remove sink from `ws_peers`
- **On UDP re-registration:** remove any existing `ws_peers` entry
- **Offline detection:** same as UDP; `last_reg_time.elapsed() >= REG_TIMEOUT` means offline

No periodic cleanup task needed. Dead connections are detected on send failure or when the heartbeat echo stops arriving (peer goes stale after 30s).

## Scalability

At 5K peers with persistent WebSocket connections:
- Memory: negligible (a few bytes per connection)
- Heartbeat load: ~250 pings/second (20s interval), trivial
- Only peers configured with `disable-udp=Y` use persistent WS; most peers continue using UDP

## Files Modified

- `src/rendezvous_server.rs` — primary: registration handling, persistent loop, relay signaling, `ws_peers` map
- `src/peer.rs` — minor: possibly a `last_reg_time` update helper

## Testing

**Unit tests:**
- `RegisterPk` over TCP/WS returns `OK` instead of `NOT_SUPPORT`
- Peer registered via WS appears in `ws_peers`
- Re-registration via UDP removes the `ws_peers` entry
- `last_reg_time` updates on empty-bytes echo receipt

**Integration tests (manual):**
- Deploy server, configure client with `allow-websocket=Y` and `disable-udp=Y`
- Verify client registers successfully and shows as online
- Verify remote desktop connection works (relay signaling reaches TCP-registered peer)
- Verify peer goes offline after disconnecting (within 30s)

## Client Behavior Reference

Gathered from the RustDesk client source (`src/rendezvous_mediator.rs`):

- TCP mode (`start_tcp`): sends `RegisterPk` only, not `RegisterPeer`
- After key confirmation, client stops sending `RegisterPk`
- Client echoes back empty bytes sent by the server (heartbeat)
- Client accepts `keep_alive` override in `RegisterPkResponse` (not used in this design; defaults are sufficient)
- Client disconnects after `keep_alive * 1.5` (default 90s) of silence
