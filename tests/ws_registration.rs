use futures_util::{SinkExt, StreamExt};
use hbb_common::{
    protobuf::{EnumOrUnknown, Message as _},
    rendezvous_proto::{
        register_pk_response, rendezvous_message, RegisterPk, RendezvousMessage,
    },
};
use std::{
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command},
    sync::{Mutex, OnceLock},
    time::Duration,
};
use tokio::time::timeout;

fn find_free_port_range() -> (u16, u16) {
    // hbbs uses: base (TCP+UDP), base-1 (NAT), base+2 (WS)
    // hbbr uses: base (TCP+UDP), base+2 (WS)
    // Find a base port where base-1, base, base+2 are all free.
    for _ in 0..100 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind to port 0");
        let base = listener.local_addr().unwrap().port();
        drop(listener);
        if base < 3 { continue; }
        let needed = [base - 1, base, base + 2];
        let all_free = needed.iter().all(|&p| {
            TcpListener::bind(("127.0.0.1", p)).is_ok()
        });
        if all_free {
            // Use base for hbbs, base+3 for hbbr (avoid overlap)
            let hbbr_base = base + 3;
            let hbbr_ports = [hbbr_base, hbbr_base + 2];
            let hbbr_free = hbbr_ports.iter().all(|&p| {
                TcpListener::bind(("127.0.0.1", p)).is_ok()
            });
            if hbbr_free {
                return (base, hbbr_base);
            }
        }
    }
    panic!("Could not find a free port range");
}

struct TestServer {
    _data_dir: PathBuf,
    hbbs_port: u16,
}

static SERVER: OnceLock<TestServer> = OnceLock::new();
static CHILD_PIDS: Mutex<Vec<u32>> = Mutex::new(Vec::new());

fn server() -> &'static TestServer {
    SERVER.get_or_init(|| TestServer::start())
}

#[cfg(unix)]
extern "C" fn cleanup_children() {
    if let Ok(pids) = CHILD_PIDS.lock() {
        for &pid in pids.iter() {
            unsafe { libc::kill(pid as i32, libc::SIGTERM); }
        }
        std::thread::sleep(Duration::from_millis(500));
        for &pid in pids.iter() {
            unsafe {
                let mut status = 0;
                libc::waitpid(pid as i32, &mut status, libc::WNOHANG);
            }
        }
    }
}

impl TestServer {
    fn start() -> Self {
        Self::build_binaries();

        let (hbbs_port, hbbr_port) = find_free_port_range();

        let data_dir = std::env::temp_dir().join(format!("rustdesk_test_{}", std::process::id()));
        std::fs::create_dir_all(&data_dir).expect("create temp data dir");

        // Spawn server processes from a dedicated thread that never exits,
        // so PR_SET_PDEATHSIG only fires when the entire process dies.
        let data_dir_clone = data_dir.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("server-spawner".into())
            .spawn(move || {
                let hbbr = Self::spawn(&Self::find_binary("hbbr"), &data_dir_clone, &[
                    "-p", &hbbr_port.to_string(),
                ], &[]);

                let hbbs = Self::spawn(&Self::find_binary("hbbs"), &data_dir_clone, &[
                    "-p", &hbbs_port.to_string(),
                    "-r", &format!("localhost:{}", hbbr_port),
                ], &[("WS_HEARTBEAT_INTERVAL", "500")]);

                tx.send((hbbr, hbbs)).unwrap();
                // Park forever — PR_SET_PDEATHSIG is tied to this thread's lifetime
                std::thread::park();
            })
            .expect("spawn server-spawner thread");
        let (hbbr, hbbs) = rx.recv().unwrap();

        {
            let mut pids = CHILD_PIDS.lock().unwrap();
            pids.push(hbbr.id());
            pids.push(hbbs.id());
        }

        #[cfg(unix)]
        unsafe { libc::atexit(cleanup_children); }

        let server = TestServer {
            _data_dir: data_dir,
            hbbs_port,
        };
        server.wait_ready();

        std::mem::forget(hbbs);
        std::mem::forget(hbbr);

        server
    }

    fn build_binaries() {
        let status = Command::new("cargo")
            .args(["build", "--bin", "hbbs", "--bin", "hbbr"])
            .status()
            .expect("Failed to run cargo build");
        assert!(status.success(), "cargo build failed");
    }

    #[cfg(unix)]
    fn spawn(bin: &PathBuf, data_dir: &PathBuf, args: &[&str], envs: &[(&str, &str)]) -> Child {
        use std::os::unix::process::CommandExt;
        unsafe {
            Command::new(bin)
                .args(args)
                .envs(envs.iter().copied())
                .current_dir(data_dir)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .pre_exec(|| {
                    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                    Ok(())
                })
                .spawn()
                .unwrap_or_else(|e| panic!("Failed to start {}: {}", bin.display(), e))
        }
    }

    #[cfg(not(unix))]
    fn spawn(bin: &PathBuf, data_dir: &PathBuf, args: &[&str], envs: &[(&str, &str)]) -> Child {
        Command::new(bin)
            .args(args)
            .envs(envs.iter().copied())
            .current_dir(data_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to start {}: {}", bin.display(), e))
    }

    fn find_binary(name: &str) -> PathBuf {
        let path = std::env::current_exe()
            .expect("locate test binary")
            .parent()
            .expect("test binary parent")
            .parent()
            .expect("deps parent")
            .join(name);
        assert!(path.exists(), "Binary not found: {}", path.display());
        path
    }

    fn ws_port(&self) -> u16 {
        self.hbbs_port + 2
    }

    fn wait_ready(&self) {
        let ws_addr = format!("127.0.0.1:{}", self.ws_port());
        for _ in 0..100 {
            if TcpStream::connect(&ws_addr).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("hbbs did not become ready on {} within 10s", ws_addr);
    }
}

static PROBED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

async fn wait_server_functional() -> &'static TestServer {
    let s = server();
    PROBED
        .get_or_init(|| async {
            let ws_port = s.ws_port();
            for _ in 0..30 {
                if let Ok((mut ws, _)) =
                    tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}", ws_port)).await
                {
                    let mut msg = RendezvousMessage::new();
                    msg.set_register_pk(RegisterPk {
                        id: "x".into(),
                        uuid: vec![0u8; 16].into(),
                        pk: vec![0u8; 32].into(),
                        ..Default::default()
                    });
                    let bytes = msg.write_to_bytes().unwrap();
                    if ws.send(tungstenite::Message::Binary(bytes)).await.is_ok() {
                        if let Ok(Some(Ok(_))) = timeout(Duration::from_secs(5), ws.next()).await {
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            panic!("hbbs did not become functionally ready within 15s");
        })
        .await;
    s
}

async fn ws_connect(
    s: &TestServer,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("ws://127.0.0.1:{}", s.ws_port());
    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("Failed to connect to hbbs WS");
    ws
}

async fn send_msg(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    msg: RendezvousMessage,
) {
    let bytes = msg.write_to_bytes().unwrap();
    ws.send(tungstenite::Message::Binary(bytes))
        .await
        .unwrap();
}

async fn recv_msg(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    timeout_ms: u64,
) -> Option<RendezvousMessage> {
    match timeout(Duration::from_millis(timeout_ms), ws.next()).await {
        Ok(Some(Ok(tungstenite::Message::Binary(data)))) => {
            if data.is_empty() {
                return None;
            }
            Some(RendezvousMessage::parse_from_bytes(&data).unwrap())
        }
        _ => None,
    }
}

fn make_register_pk(id: &str) -> RendezvousMessage {
    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: id.into(),
        uuid: vec![1u8; 16].into(),
        pk: vec![2u8; 32].into(),
        ..Default::default()
    });
    msg
}

fn extract_register_pk_result(
    msg: &RendezvousMessage,
) -> EnumOrUnknown<register_pk_response::Result> {
    match msg.union {
        Some(rendezvous_message::Union::RegisterPkResponse(ref resp)) => resp.result,
        ref other => panic!("Expected RegisterPkResponse, got {:?}", other),
    }
}

#[tokio::test]
async fn register_pk_over_ws_returns_ok() {
    let s = wait_server_functional().await;

    let mut ws = ws_connect(s).await;
    send_msg(&mut ws, make_register_pk("test-peer-reg-001")).await;

    let resp = recv_msg(&mut ws, 5000)
        .await
        .expect("No response from server");
    assert_eq!(
        extract_register_pk_result(&resp),
        EnumOrUnknown::from(register_pk_response::Result::OK),
    );
}

#[tokio::test]
async fn register_pk_short_id_rejected() {
    let s = wait_server_functional().await;

    let mut ws = ws_connect(s).await;
    send_msg(&mut ws, make_register_pk("abc")).await;

    let resp = recv_msg(&mut ws, 5000)
        .await
        .expect("No response from server");
    assert_eq!(
        extract_register_pk_result(&resp),
        EnumOrUnknown::from(register_pk_response::Result::UUID_MISMATCH),
    );
}

#[tokio::test]
async fn ws_connection_receives_heartbeat_after_registration() {
    let s = wait_server_functional().await;

    let mut ws = ws_connect(s).await;
    send_msg(&mut ws, make_register_pk("test-peer-hb-001")).await;

    let resp = recv_msg(&mut ws, 5000)
        .await
        .expect("No registration response");
    assert_eq!(
        extract_register_pk_result(&resp),
        EnumOrUnknown::from(register_pk_response::Result::OK),
    );

    // Server sends heartbeat (empty binary) at WS_HEARTBEAT_INTERVAL (500ms in tests)
    let heartbeat = timeout(Duration::from_secs(3), ws.next()).await;
    match heartbeat {
        Ok(Some(Ok(tungstenite::Message::Binary(data)))) => {
            assert!(data.is_empty(), "Heartbeat should be empty bytes");
        }
        other => panic!(
            "Expected empty binary heartbeat within 3s, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn register_pk_empty_uuid_gets_no_response() {
    let s = wait_server_functional().await;

    let mut ws = ws_connect(s).await;

    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "test-peer-empty-uuid".into(),
        uuid: Vec::new().into(),
        pk: vec![2u8; 32].into(),
        ..Default::default()
    });
    send_msg(&mut ws, msg).await;

    // Server silently ignores empty uuid
    let resp = recv_msg(&mut ws, 3000).await;
    assert!(resp.is_none(), "Empty uuid should get no response");
}
