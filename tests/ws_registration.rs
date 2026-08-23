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
    time::Duration,
};
use tokio::time::timeout;

const HBBS_PORT: u16 = 31116;
const HBBR_PORT: u16 = 31117;

fn hbbs_ws_port() -> u16 {
    HBBS_PORT + 2
}

struct TestServer {
    hbbs: Child,
    hbbr: Child,
    data_dir: PathBuf,
}

impl TestServer {
    fn start() -> Self {
        let data_dir = std::env::temp_dir().join(format!("rustdesk_test_{}", std::process::id()));
        std::fs::create_dir_all(&data_dir).expect("create temp data dir");

        let hbbr_bin = Self::find_binary("hbbr");
        let hbbs_bin = Self::find_binary("hbbs");

        let hbbr = Command::new(&hbbr_bin)
            .args(["-p", &HBBR_PORT.to_string()])
            .current_dir(&data_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("Failed to start hbbr");

        let hbbs = Command::new(&hbbs_bin)
            .args([
                "-p",
                &HBBS_PORT.to_string(),
                "-r",
                &format!("localhost:{}", HBBR_PORT),
            ])
            .current_dir(&data_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("Failed to start hbbs");

        let server = TestServer {
            hbbs,
            hbbr,
            data_dir,
        };
        server.wait_ready();
        server
    }

    fn find_binary(name: &str) -> PathBuf {
        let mut path = std::env::current_exe()
            .expect("locate test binary")
            .parent()
            .expect("test binary parent")
            .parent()
            .expect("deps parent")
            .to_path_buf();
        path.push(name);
        assert!(path.exists(), "Binary not found: {}", path.display());
        path
    }

    fn wait_ready(&self) {
        let ws_addr = format!("127.0.0.1:{}", hbbs_ws_port());
        for _ in 0..50 {
            if TcpStream::connect(&ws_addr).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("hbbs did not become ready on {} within 5s", ws_addr);
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.hbbs.kill().ok();
        self.hbbr.kill().ok();
        self.hbbs.wait().ok();
        self.hbbr.wait().ok();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
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
    let _server = TestServer::start();

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
    let _server = TestServer::start();

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
    let _server = TestServer::start();

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
    let _server = TestServer::start();

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
