# WebSocket/TCP Peer Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable native peers to register and receive relay signals over TCP/WebSocket so they work in corporate environments where UDP is blocked.

**Architecture:** Add a `ws_peers` map (`HashMap<String, Sink>`) to `RendezvousServer` for storing persistent WS/TCP sinks keyed by peer ID. Replace the `NOT_SUPPORT` response for `RegisterPk` over TCP/WS with real registration logic. Convert the 30s one-shot TCP read loop into a persistent connection loop with server-initiated heartbeats (~20s). When sending messages to peers (relay signals, punch hole), check `ws_peers` first, fall back to UDP.

**Tech Stack:** Rust, tokio, tokio-tungstenite, protobuf (hbb_common)

---

## File Structure

- **Modify: `src/rendezvous_server.rs`** — Add `ws_peers` field, implement WS registration in `handle_tcp`, add persistent connection loop in `handle_listener_inner`, add `ws_peers` lookup in relay/punch-hole send paths, add helper methods for sending to WS peers.
- **Modify: `src/peer.rs`** (minor) — No structural changes needed. `last_reg_time` updates happen through existing `LockPeer` write access.

---

### Task 1: Add `ws_peers` field to `RendezvousServer`

**Files:**
- Modify: `src/rendezvous_server.rs:83-91` (struct definition)
- Modify: `src/rendezvous_server.rs` (wherever `RendezvousServer` is constructed)

- [ ] **Step 1: Find where `RendezvousServer` is constructed**

Search for where the struct is initialized (likely in `start` or `start_with_bind`) to know where to add `ws_peers` initialization.

Run: `grep -n 'tcp_punch:' src/rendezvous_server.rs`

- [ ] **Step 2: Add `ws_peers` field to the struct**

In `src/rendezvous_server.rs`, add to the `RendezvousServer` struct:

```rust
#[derive(Clone)]
pub struct RendezvousServer {
    tcp_punch: Arc<Mutex<HashMap<SocketAddr, Sink>>>,
    ws_peers: Arc<Mutex<HashMap<String, Sink>>>,  // peer_id -> persistent WS/TCP sink
    pm: PeerMap,
    tx: Sender,
    relay_servers: Arc<RelayServers>,
    relay_servers0: Arc<RelayServers>,
    rendezvous_servers: Arc<Vec<String>>,
    inner: Arc<Inner>,
}
```

- [ ] **Step 3: Initialize `ws_peers` in the constructor**

Find the `RendezvousServer { ... }` initialization and add:

```rust
ws_peers: Default::default(),
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check 2>&1 | tail -20`
Expected: Compiles cleanly (or only pre-existing warnings).

- [ ] **Step 5: Commit**

```bash
git add src/rendezvous_server.rs
git commit -m "feat: add ws_peers map to RendezvousServer"
```

---

### Task 2: Add helper method to send a message to a WS peer by ID

**Files:**
- Modify: `src/rendezvous_server.rs` (add method to `impl RendezvousServer`)

- [ ] **Step 1: Add `send_to_ws_peer` method**

Add a method that tries to send a message via `ws_peers`, returning whether it succeeded. On send failure, remove the stale entry.

```rust
async fn send_to_ws_peer(&self, peer_id: &str, msg: RendezvousMessage) -> bool {
    let sink = self.ws_peers.lock().await.remove(peer_id);
    if let Some(mut s) = sink {
        if let Ok(bytes) = msg.write_to_bytes() {
            let ok = match &mut s {
                Sink::TcpStream(tcp) => tcp.send(Bytes::from(bytes)).await.is_ok(),
                Sink::Ws(ws) => ws.send(tungstenite::Message::Binary(bytes)).await.is_ok(),
            };
            if ok {
                self.ws_peers.lock().await.insert(peer_id.to_owned(), s);
                return true;
            }
            log::warn!("Failed to send to WS peer {}, removing sink", peer_id);
        }
        // send failed or serialize failed — sink is dropped (not re-inserted)
        true // we had a sink, just failed; don't fall back to UDP
    } else {
        false // no WS sink, caller should use UDP
    }
}
```

Note: We remove-then-reinsert to avoid holding the Mutex across the async send. On failure we intentionally don't re-insert (the sink is dead). We return `true` even on send failure because the peer was WS-registered — falling back to UDP for a WS-registered peer would send to a stale `socket_addr`.

- [ ] **Step 2: Verify it compiles**

Run: `cargo check 2>&1 | tail -20`
Expected: Warning about unused method (expected — we'll use it in later tasks).

- [ ] **Step 3: Commit**

```bash
git add src/rendezvous_server.rs
git commit -m "feat: add send_to_ws_peer helper method"
```

---

### Task 3: Handle `RegisterPk` over TCP/WS

**Files:**
- Modify: `src/rendezvous_server.rs:616-624` (replace `NOT_SUPPORT` block in `handle_tcp`)
- Modify: `src/rendezvous_server.rs:543-629` (`handle_tcp` method — add `registered_peer_id` return value)

This task replaces the `NOT_SUPPORT` response with real registration logic. The registration code from the UDP `RegisterPk` handler (lines 410-494) needs to be adapted for TCP.

- [ ] **Step 1: Change `handle_tcp` return type**

Currently `handle_tcp` returns `bool` (true = close connection). Change the return type to return an `Option<String>` representing a newly registered peer ID, so the caller (`handle_listener_inner`) knows to enter the persistent loop.

Change the signature:

```rust
async fn handle_tcp(
    &mut self,
    bytes: &[u8],
    sink: &mut Option<Sink>,
    addr: SocketAddr,
    key: &str,
    ws: bool,
) -> (bool, Option<String>) {
```

Update all existing `return true;` to `return (true, None);`, all `return false;` and trailing `false` to `return (false, None);` or `(false, None)`. There are several `return true` at lines 559, 573, and the trailing `false` at line 628.

- [ ] **Step 2: Replace the `NOT_SUPPORT` block with real registration**

Replace the `RegisterPk` match arm in `handle_tcp` (lines 616-624) with:

```rust
Some(rendezvous_message::Union::RegisterPk(rk)) => {
    if rk.uuid.is_empty() || rk.pk.is_empty() {
        return (false, None);
    }
    let id = rk.id;
    let ip = addr.ip().to_string();
    if id.len() < 6 {
        let mut msg_out = RendezvousMessage::new();
        msg_out.set_register_pk_response(RegisterPkResponse {
            result: UUID_MISMATCH.into(),
            ..Default::default()
        });
        Self::send_to_sink(sink, msg_out).await;
        return (false, None);
    } else if !self.check_ip_blocker(&ip, &id).await {
        let mut msg_out = RendezvousMessage::new();
        msg_out.set_register_pk_response(RegisterPkResponse {
            result: TOO_FREQUENT.into(),
            ..Default::default()
        });
        Self::send_to_sink(sink, msg_out).await;
        return (false, None);
    }
    let peer = self.pm.get_or(&id).await;
    let (changed, ip_changed) = {
        let peer = peer.read().await;
        if peer.uuid.is_empty() {
            (true, false)
        } else {
            if peer.uuid == rk.uuid {
                if peer.info.ip != ip && peer.pk != rk.pk {
                    log::warn!(
                        "Peer {} ip/pk mismatch: {}/{:?} vs {}/{:?}",
                        id, ip, rk.pk, peer.info.ip, peer.pk,
                    );
                    let mut msg_out = RendezvousMessage::new();
                    msg_out.set_register_pk_response(RegisterPkResponse {
                        result: UUID_MISMATCH.into(),
                        ..Default::default()
                    });
                    drop(peer);
                    Self::send_to_sink(sink, msg_out).await;
                    return (false, None);
                }
            } else {
                log::warn!(
                    "Peer {} uuid mismatch: {:?} vs {:?}",
                    id, rk.uuid, peer.uuid
                );
                let mut msg_out = RendezvousMessage::new();
                msg_out.set_register_pk_response(RegisterPkResponse {
                    result: UUID_MISMATCH.into(),
                    ..Default::default()
                });
                drop(peer);
                Self::send_to_sink(sink, msg_out).await;
                return (false, None);
            }
            let ip_changed = peer.info.ip != ip;
            (
                peer.uuid != rk.uuid || peer.pk != rk.pk || ip_changed,
                ip_changed,
            )
        }
    };
    let mut req_pk = peer.read().await.reg_pk;
    if req_pk.1.elapsed().as_secs() > 6 {
        req_pk.0 = 0;
    } else if req_pk.0 > 2 {
        let mut msg_out = RendezvousMessage::new();
        msg_out.set_register_pk_response(RegisterPkResponse {
            result: TOO_FREQUENT.into(),
            ..Default::default()
        });
        Self::send_to_sink(sink, msg_out).await;
        return (false, None);
    }
    req_pk.0 += 1;
    req_pk.1 = Instant::now();
    peer.write().await.reg_pk = req_pk;
    if ip_changed {
        let mut lock = IP_CHANGES.lock().await;
        if let Some((tm, ips)) = lock.get_mut(&id) {
            if tm.elapsed().as_secs() > IP_CHANGE_DUR {
                *tm = Instant::now();
                ips.clear();
                ips.insert(ip.clone(), 1);
            } else if let Some(v) = ips.get_mut(&ip) {
                *v += 1;
            } else {
                ips.insert(ip.clone(), 1);
            }
        } else {
            lock.insert(
                id.clone(),
                (Instant::now(), HashMap::from([(ip.clone(), 1)])),
            );
        }
    }
    if changed {
        self.pm.update_pk(id.clone(), peer, addr, rk.uuid, rk.pk, ip).await;
    }
    let mut msg_out = RendezvousMessage::new();
    msg_out.set_register_pk_response(RegisterPkResponse {
        result: register_pk_response::Result::OK.into(),
        ..Default::default()
    });
    Self::send_to_sink(sink, msg_out).await;

    // Store the WS sink for this peer and signal persistent mode
    if let Some(s) = sink.take() {
        self.ws_peers.lock().await.insert(id.clone(), s);
        log::info!("Peer {} registered via TCP/WS", id);
    }
    return (false, Some(id));
}
```

- [ ] **Step 3: Update callers of `handle_tcp`**

In `handle_listener_inner`, update the calls to `handle_tcp` to destructure the new return type. For now just handle the bool part — the persistent loop will come in Task 5.

In the WS branch (~line 1249):
```rust
while let Ok(Some(Ok(msg))) = timeout(30_000, b.next()).await {
    if let tungstenite::Message::Binary(bytes) = msg {
        let (close, _registered_id) = self.handle_tcp(&bytes, &mut sink, addr, key, ws).await;
        if close {
            break;
        }
    }
}
```

In the TCP branch (~line 1259):
```rust
while let Ok(Some(Ok(bytes))) = timeout(30_000, b.next()).await {
    let (close, _registered_id) = self.handle_tcp(&bytes, &mut sink, addr, key, ws).await;
    if close {
        break;
    }
}
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check 2>&1 | tail -20`
Expected: Compiles cleanly (warnings about unused `_registered_id` are fine).

- [ ] **Step 5: Commit**

```bash
git add src/rendezvous_server.rs
git commit -m "feat: accept RegisterPk over TCP/WS instead of NOT_SUPPORT"
```

---

### Task 4: Remove WS sink on UDP re-registration (last-registration-wins)

**Files:**
- Modify: `src/rendezvous_server.rs:410-494` (UDP `RegisterPk` handler)
- Modify: `src/rendezvous_server.rs:394-408` (UDP `RegisterPeer` handler / `update_addr`)

- [ ] **Step 1: Clear `ws_peers` entry on UDP `RegisterPk`**

In the UDP `RegisterPk` handler in `handle_udp`, after the successful registration (after line 486 `self.pm.update_pk(...)`), add:

```rust
self.ws_peers.lock().await.remove(&id);
```

Place it right before the `RegisterPkResponse` is sent (around line 488).

- [ ] **Step 2: Clear `ws_peers` entry on UDP `RegisterPeer`**

In `update_addr` (called by the `RegisterPeer` UDP handler), after updating `last_reg_time` (around line 649), add:

```rust
self.ws_peers.lock().await.remove(&id);
```

Place it after the `if let Some(old) = self.pm.get_in_memory(&id).await` block, before the response is sent.

- [ ] **Step 3: Verify it compiles**

Run: `cargo check 2>&1 | tail -20`

- [ ] **Step 4: Commit**

```bash
git add src/rendezvous_server.rs
git commit -m "feat: clear ws_peers entry on UDP re-registration"
```

---

### Task 5: Persistent connection loop for WS-registered peers

**Files:**
- Modify: `src/rendezvous_server.rs:1212-1270` (`handle_listener_inner`)

This is the core change: after a successful WS registration, keep the connection alive with heartbeats instead of exiting after the 30s timeout.

- [ ] **Step 1: Add `WS_HEARTBEAT_INTERVAL` constant**

Near the existing `REG_TIMEOUT` constant (line 50), add:

```rust
const WS_HEARTBEAT_INTERVAL: u64 = 20_000; // 20 seconds
```

- [ ] **Step 2: Rewrite the WS branch in `handle_listener_inner`**

Replace the WS branch of `handle_listener_inner` (lines 1249-1255) with a loop that supports persistent connections:

```rust
let ws_stream = tokio_tungstenite::accept_hdr_async(stream, callback).await?;
let (a, mut b) = ws_stream.split();
sink = Some(Sink::Ws(a));
let mut registered_peer_id: Option<String> = None;
loop {
    let read_timeout = if registered_peer_id.is_some() {
        WS_HEARTBEAT_INTERVAL
    } else {
        30_000
    };
    match timeout(read_timeout, b.next()).await {
        Ok(Some(Ok(msg))) => {
            if let tungstenite::Message::Binary(bytes) = msg {
                if bytes.is_empty() {
                    // Empty-bytes heartbeat echo from client
                    if let Some(ref id) = registered_peer_id {
                        if let Some(peer) = self.pm.get_in_memory(id).await {
                            peer.write().await.last_reg_time = Instant::now();
                        }
                    }
                    continue;
                }
                let (close, new_reg_id) = self.handle_tcp(&bytes, &mut sink, addr, key, ws).await;
                if let Some(id) = new_reg_id {
                    registered_peer_id = Some(id);
                }
                if close {
                    break;
                }
            }
        }
        Ok(Some(Err(e))) => {
            log::debug!("WS read error from {:?}: {}", addr, e);
            break;
        }
        Ok(None) => {
            // Stream ended
            break;
        }
        Err(_) => {
            // Timeout
            if registered_peer_id.is_some() {
                // Send heartbeat to registered peer
                let heartbeat = tungstenite::Message::Binary(Vec::new());
                if let Some(Sink::Ws(ref mut ws_sink)) = sink {
                    if ws_sink.send(heartbeat).await.is_err() {
                        log::debug!("Heartbeat send failed to {:?}, closing", addr);
                        break;
                    }
                } else {
                    break;
                }
            } else {
                // Not registered, normal 30s timeout — close
                break;
            }
        }
    }
}
// Cleanup: remove from ws_peers if this was a registered peer
if let Some(ref id) = registered_peer_id {
    self.ws_peers.lock().await.remove(id);
    log::info!("WS peer {} disconnected", id);
}
```

- [ ] **Step 3: Update the TCP (non-WS) branch similarly**

Replace the TCP branch (lines 1257-1263) with a similar loop. The TCP branch uses `BytesCodec` and `Bytes` instead of WS messages:

```rust
let (a, mut b) = Framed::new(stream, BytesCodec::new()).split();
sink = Some(Sink::TcpStream(a));
let mut registered_peer_id: Option<String> = None;
loop {
    let read_timeout = if registered_peer_id.is_some() {
        WS_HEARTBEAT_INTERVAL
    } else {
        30_000
    };
    match timeout(read_timeout, b.next()).await {
        Ok(Some(Ok(bytes))) => {
            if bytes.is_empty() {
                // Empty-bytes heartbeat echo from client
                if let Some(ref id) = registered_peer_id {
                    if let Some(peer) = self.pm.get_in_memory(id).await {
                        peer.write().await.last_reg_time = Instant::now();
                    }
                }
                continue;
            }
            let (close, new_reg_id) = self.handle_tcp(&bytes, &mut sink, addr, key, ws).await;
            if let Some(id) = new_reg_id {
                registered_peer_id = Some(id);
            }
            if close {
                break;
            }
        }
        Ok(Some(Err(e))) => {
            log::debug!("TCP read error from {:?}: {}", addr, e);
            break;
        }
        Ok(None) => {
            break;
        }
        Err(_) => {
            if registered_peer_id.is_some() {
                // Send heartbeat
                if let Some(Sink::TcpStream(ref mut tcp_sink)) = sink {
                    if tcp_sink.send(Bytes::new()).await.is_err() {
                        log::debug!("Heartbeat send failed to {:?}, closing", addr);
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }
}
if let Some(ref id) = registered_peer_id {
    self.ws_peers.lock().await.remove(id);
    log::info!("TCP peer {} disconnected", id);
}
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check 2>&1 | tail -20`

- [ ] **Step 5: Commit**

```bash
git add src/rendezvous_server.rs
git commit -m "feat: persistent connection loop with heartbeats for WS-registered peers"
```

---

### Task 6: Route relay signaling through WS when available

**Files:**
- Modify: `src/rendezvous_server.rs:561-573` (`RequestRelay` handler in `handle_tcp`)
- Modify: `src/rendezvous_server.rs:917-931` (`handle_tcp_punch_hole_request`)

- [ ] **Step 1: Update `RequestRelay` handler to check `ws_peers`**

In `handle_tcp`, replace the `RequestRelay` handler (lines 561-573). Note that `rf.id` is the target peer B's ID — we already have it, so we can check `ws_peers` directly:

```rust
Some(rendezvous_message::Union::RequestRelay(mut rf)) => {
    // there maybe several attempt, so sink can be none
    if let Some(sink) = sink.take() {
        self.tcp_punch.lock().await.insert(try_into_v4(addr), sink);
    }
    let target_id = rf.id.clone();
    if let Some(peer) = self.pm.get_in_memory(&target_id).await {
        let mut msg_out = RendezvousMessage::new();
        rf.socket_addr = AddrMangle::encode(addr).into();
        msg_out.set_request_relay(rf);
        // Try WS first, fall back to UDP
        if !self.send_to_ws_peer(&target_id, msg_out.clone()).await {
            let peer_addr = peer.read().await.socket_addr;
            self.tx.send(Data::Msg(msg_out.into(), peer_addr)).ok();
        }
    }
    return (true, None);
}
```

- [ ] **Step 2: Update `handle_tcp_punch_hole_request` to check `ws_peers`**

The `handle_tcp_punch_hole_request` method (lines 917-931) sends `PunchHole`/`FetchLocalAddr` to peer B via UDP. Update it to try WS first:

```rust
async fn handle_tcp_punch_hole_request(
    &mut self,
    addr: SocketAddr,
    ph: PunchHoleRequest,
    key: &str,
    ws: bool,
) -> ResultType<()> {
    let target_id = ph.id.clone();
    let (msg, to_addr) = self.handle_punch_hole_request(addr, ph, key, ws).await?;
    if let Some(peer_addr) = to_addr {
        // Try WS first, fall back to UDP
        if !self.send_to_ws_peer(&target_id, msg.clone()).await {
            self.tx.send(Data::Msg(msg.into(), peer_addr))?;
        }
    } else {
        self.send_to_tcp_sync(msg, addr).await?;
    }
    Ok(())
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check 2>&1 | tail -20`

- [ ] **Step 4: Run existing tests**

Run: `cargo test 2>&1 | tail -30`
Expected: All existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/rendezvous_server.rs
git commit -m "feat: route relay/punch-hole signaling through WS when available"
```

---

### Task 7: Handle `RegisterPeer` over TCP/WS

**Files:**
- Modify: `src/rendezvous_server.rs` (`handle_tcp` method — add `RegisterPeer` match arm)

The client may send `RegisterPeer` over TCP/WS for heartbeat re-registration. Currently this message type is only handled in `handle_udp`.

- [ ] **Step 1: Add `RegisterPeer` handling to `handle_tcp`**

In `handle_tcp`, add a new match arm before the `_ => {}` catch-all:

```rust
Some(rendezvous_message::Union::RegisterPeer(rp)) => {
    if rp.id.is_empty() {
        return (false, None);
    }
    if let Some(peer) = self.pm.get_in_memory(&rp.id).await {
        let mut w = peer.write().await;
        w.socket_addr = addr;
        w.last_reg_time = Instant::now();
    }
    let mut msg_out = RendezvousMessage::new();
    msg_out.set_register_peer_response(RegisterPeerResponse {
        request_pk: false,
        ..Default::default()
    });
    Self::send_to_sink(sink, msg_out).await;
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check 2>&1 | tail -20`

- [ ] **Step 3: Commit**

```bash
git add src/rendezvous_server.rs
git commit -m "feat: handle RegisterPeer over TCP/WS"
```

---

### Task 8: Integration verification

**Files:**
- No code changes — manual testing

- [ ] **Step 1: Build the server**

Run: `cargo build 2>&1 | tail -20`
Expected: Builds successfully.

- [ ] **Step 2: Run all tests**

Run: `cargo test 2>&1 | tail -30`
Expected: All tests pass.

- [ ] **Step 3: Review the diff**

Run: `git diff master..HEAD --stat` and `git log --oneline master..HEAD`
Verify: All commits are focused and the diff is limited to the expected files.

- [ ] **Step 4: Document manual test procedure**

The following manual tests should be performed with a real RustDesk client:

1. Configure client with `allow-websocket=Y`, `disable-udp=Y`
2. Point client at the modified server
3. Verify client registers successfully (check server logs for "Peer X registered via TCP/WS")
4. Verify client shows as online from another client
5. Verify remote desktop connection works between a WS-registered peer and a UDP peer
6. Disconnect the WS peer — verify it goes offline within 30s
7. Reconnect — verify it re-registers successfully
