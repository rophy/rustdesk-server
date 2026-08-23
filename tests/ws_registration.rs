use futures_util::{SinkExt, StreamExt};
use hbb_common::{
    protobuf::{EnumOrUnknown, Message as _},
    rendezvous_proto::{
        register_pk_response, rendezvous_message, RegisterPk, RendezvousMessage,
    },
};
use std::{
    net::TcpStream,
    path::PathBuf,
    process::{Child, Command},
    sync::{Mutex, OnceLock},
    time::Duration,
};
use tokio::time::timeout;

const HBBS_PORT: u16 = 31116;
const HBBR_PORT: u16 = 31117;

fn hbbs_ws_port() -> u16 {
    HBBS_PORT + 2
}

struct TestServer {
    _data_dir: PathBuf,
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

        let data_dir = std::env::temp_dir().join(format!("rustdesk_test_{}", std::process::id()));
        std::fs::create_dir_all(&data_dir).expect("create temp data dir");

        let hbbr = Self::spawn(&Self::find_binary("hbbr"), &data_dir, &[
            "-p", &HBBR_PORT.to_string(),
        ]);

        let hbbs = Self::spawn(&Self::find_binary("hbbs"), &data_dir, &[
            "-p", &HBBS_PORT.to_string(),
            "-r", &format!("localhost:{}", HBBR_PORT),
        ]);

        {
            let mut pids = CHILD_PIDS.lock().unwrap();
            pids.push(hbbr.id());
            pids.push(hbbs.id());
        }

        #[cfg(unix)]
        unsafe { libc::atexit(cleanup_children); }

        let server = TestServer {
            _data_dir: data_dir,
        };
        server.wait_ready();

        // Leak the Child handles — cleanup is via atexit + PR_SET_PDEATHSIG
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
    fn spawn(bin: &PathBuf, data_dir: &PathBuf, args: &[&str]) -> Child {
        use std::os::unix::process::CommandExt;
        unsafe {
            Command::new(bin)
                .args(args)
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
    fn spawn(bin: &PathBuf, data_dir: &PathBuf, args: &[&str]) -> Child {
        Command::new(bin)
            .args(args)
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

    fn wait_ready(&self) {
        let ws_addr = format!("127.0.0.1:{}", hbbs_ws_port());
        for _ in 0..100 {
            if TcpStream::connect(&ws_addr).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("hbbs did not become ready on {} within 10s", ws_addr);
    }
}

static PROBED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

async fn wait_server_functional() {
    server();
    if PROBED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        return;
    }
    // TCP listen doesn't mean the server is fully initialized.
    // Probe with a short-ID registration until we get a response.
    for _ in 0..30 {
        if let Ok((mut ws, _)) =
            tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}", hbbs_ws_port()))
                .await
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
}

async fn ws_connect(
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("ws://127.0.0.1:{}", hbbs_ws_port());
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
    wait_server_functional().await;

    let mut ws = ws_connect().await;
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
    wait_server_functional().await;

    let mut ws = ws_connect().await;
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
    wait_server_functional().await;

    let mut ws = ws_connect().await;
    send_msg(&mut ws, make_register_pk("test-peer-hb-001")).await;

    let resp = recv_msg(&mut ws, 5000)
        .await
        .expect("No registration response");
    assert_eq!(
        extract_register_pk_result(&resp),
        EnumOrUnknown::from(register_pk_response::Result::OK),
    );

    // Server sends heartbeat (empty binary) at ~20s timeout
    let heartbeat = timeout(Duration::from_secs(25), ws.next()).await;
    match heartbeat {
        Ok(Some(Ok(tungstenite::Message::Binary(data)))) => {
            assert!(data.is_empty(), "Heartbeat should be empty bytes");
        }
        other => panic!(
            "Expected empty binary heartbeat within 25s, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn register_pk_empty_uuid_gets_no_response() {
    wait_server_functional().await;

    let mut ws = ws_connect().await;

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
