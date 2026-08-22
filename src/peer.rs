use crate::common::*;
use crate::database;
use hbb_common::{
    bytes::Bytes,
    log,
    rendezvous_proto::*,
    tokio::sync::{Mutex, RwLock},
    ResultType,
};
use serde_derive::{Deserialize, Serialize};
use std::{collections::HashMap, collections::HashSet, net::SocketAddr, sync::Arc, time::Instant};

type IpBlockMap = HashMap<String, ((u32, Instant), (HashSet<String>, Instant))>;
type UserStatusMap = HashMap<Vec<u8>, Arc<(Option<Vec<u8>>, bool)>>;
type IpChangesMap = HashMap<String, (Instant, HashMap<String, i32>)>;
lazy_static::lazy_static! {
    pub(crate) static ref IP_BLOCKER: Mutex<IpBlockMap> = Default::default();
    pub(crate) static ref USER_STATUS: RwLock<UserStatusMap> = Default::default();
    pub(crate) static ref IP_CHANGES: Mutex<IpChangesMap> = Default::default();
}
pub const IP_CHANGE_DUR: u64 = 180;
pub const IP_CHANGE_DUR_X2: u64 = IP_CHANGE_DUR * 2;
pub const DAY_SECONDS: u64 = 3600 * 24;
pub const IP_BLOCK_DUR: u64 = 60;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct PeerInfo {
    #[serde(default)]
    pub(crate) ip: String,
}

pub(crate) struct Peer {
    pub(crate) socket_addr: SocketAddr,
    pub(crate) last_reg_time: Instant,
    pub(crate) guid: Vec<u8>,
    pub(crate) uuid: Bytes,
    pub(crate) pk: Bytes,
    // pub(crate) user: Option<Vec<u8>>,
    pub(crate) info: PeerInfo,
    // pub(crate) disabled: bool,
    pub(crate) reg_pk: (u32, Instant), // how often register_pk
}

impl Default for Peer {
    fn default() -> Self {
        Self {
            socket_addr: "0.0.0.0:0".parse().unwrap(),
            last_reg_time: get_expired_time(),
            guid: Vec::new(),
            uuid: Bytes::new(),
            pk: Bytes::new(),
            info: Default::default(),
            // user: None,
            // disabled: false,
            reg_pk: (0, get_expired_time()),
        }
    }
}

pub(crate) type LockPeer = Arc<RwLock<Peer>>;

#[derive(Clone)]
pub(crate) struct PeerMap {
    map: Arc<RwLock<HashMap<String, LockPeer>>>,
    pub(crate) db: database::Database,
}

impl PeerMap {
    pub(crate) async fn new() -> ResultType<Self> {
        let db = get_arg_opt("DB_URL").unwrap_or_else(|| {
            let mut db = "db_v2.sqlite3".to_owned();
            #[cfg(all(windows, not(debug_assertions)))]
            {
                if let Some(path) = hbb_common::config::Config::icon_path().parent() {
                    db = format!("{}\\{}", path.to_str().unwrap_or("."), db);
                }
            }
            #[cfg(not(windows))]
            {
                db = format!("./{db}");
            }
            db
        });
        log::info!("DB_URL={}", db);
        let pm = Self {
            map: Default::default(),
            db: database::Database::new(&db).await?,
        };
        Ok(pm)
    }

    #[inline]
    pub(crate) async fn update_pk(
        &mut self,
        id: String,
        peer: LockPeer,
        addr: SocketAddr,
        uuid: Bytes,
        pk: Bytes,
        ip: String,
    ) -> register_pk_response::Result {
        log::info!("update_pk {} {:?} {:?} {:?}", id, addr, uuid, pk);
        let (info_str, guid) = {
            let mut w = peer.write().await;
            w.socket_addr = addr;
            w.uuid = uuid.clone();
            w.pk = pk.clone();
            w.last_reg_time = Instant::now();
            w.info.ip = ip;
            (
                serde_json::to_string(&w.info).unwrap_or_default(),
                w.guid.clone(),
            )
        };
        if guid.is_empty() {
            match self.db.insert_peer(&id, &uuid, &pk, &info_str).await {
                Err(err) => {
                    log::error!("db.insert_peer failed: {}", err);
                    return register_pk_response::Result::SERVER_ERROR;
                }
                Ok(guid) => {
                    peer.write().await.guid = guid;
                }
            }
        } else {
            if let Err(err) = self.db.update_pk(&guid, &id, &pk, &info_str).await {
                log::error!("db.update_pk failed: {}", err);
                return register_pk_response::Result::SERVER_ERROR;
            }
            log::info!("pk updated instead of insert");
        }
        register_pk_response::Result::OK
    }

    #[inline]
    pub(crate) async fn get(&self, id: &str) -> Option<LockPeer> {
        let p = self.map.read().await.get(id).cloned();
        if p.is_some() {
            return p;
        } else if let Ok(Some(v)) = self.db.get_peer(id).await {
            let peer = Peer {
                guid: v.guid,
                uuid: v.uuid.into(),
                pk: v.pk.into(),
                // user: v.user,
                info: serde_json::from_str::<PeerInfo>(&v.info).unwrap_or_default(),
                // disabled: v.status == Some(0),
                ..Default::default()
            };
            let peer = Arc::new(RwLock::new(peer));
            self.map.write().await.insert(id.to_owned(), peer.clone());
            return Some(peer);
        }
        None
    }

    #[inline]
    pub(crate) async fn get_or(&self, id: &str) -> LockPeer {
        if let Some(p) = self.get(id).await {
            return p;
        }
        let mut w = self.map.write().await;
        if let Some(p) = w.get(id) {
            return p.clone();
        }
        let tmp = LockPeer::default();
        w.insert(id.to_owned(), tmp.clone());
        tmp
    }

    #[inline]
    pub(crate) async fn get_in_memory(&self, id: &str) -> Option<LockPeer> {
        self.map.read().await.get(id).cloned()
    }

    #[inline]
    pub(crate) async fn is_in_memory(&self, id: &str) -> bool {
        self.map.read().await.contains_key(id)
    }

    #[inline]
    pub(crate) async fn map_len(&self) -> usize {
        self.map.read().await.len()
    }

    pub(crate) async fn count_online(&self, threshold_secs: u64) -> usize {
        let peers: Vec<_> = self.map.read().await.values().cloned().collect();
        let now = Instant::now();
        let mut count = 0;
        for peer in &peers {
            let last_reg = peer.read().await.last_reg_time;
            if now
                .checked_duration_since(last_reg)
                .map_or(true, |elapsed| elapsed.as_secs() < threshold_secs)
            {
                count += 1;
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database;
    use hbb_common::tokio;

    async fn make_test_peer_map() -> PeerMap {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = std::env::temp_dir().join(format!("rustdesk_test_peer_{}.sqlite3", id));
        let _ = std::fs::remove_file(&tmp);
        PeerMap {
            map: Default::default(),
            db: database::Database::new(tmp.to_str().unwrap()).await.unwrap(),
        }
    }

    fn make_peer_with_reg_time(reg_time: Instant) -> LockPeer {
        Arc::new(RwLock::new(Peer {
            last_reg_time: reg_time,
            ..Default::default()
        }))
    }

    #[hbb_common::tokio::test]
    async fn map_len_empty() {
        let pm = make_test_peer_map().await;
        assert_eq!(pm.map_len().await, 0);
    }

    #[hbb_common::tokio::test]
    async fn map_len_with_peers() {
        let pm = make_test_peer_map().await;
        {
            let mut map = pm.map.write().await;
            map.insert("peer1".to_owned(), LockPeer::default());
            map.insert("peer2".to_owned(), LockPeer::default());
        }
        assert_eq!(pm.map_len().await, 2);
    }

    #[hbb_common::tokio::test]
    async fn count_online_no_peers() {
        let pm = make_test_peer_map().await;
        assert_eq!(pm.count_online(30).await, 0);
    }

    #[hbb_common::tokio::test]
    async fn count_online_with_recent_peer() {
        let pm = make_test_peer_map().await;
        {
            let mut map = pm.map.write().await;
            map.insert("peer1".to_owned(), make_peer_with_reg_time(Instant::now()));
        }
        assert_eq!(pm.count_online(30).await, 1);
    }

    #[hbb_common::tokio::test]
    async fn count_online_with_expired_peer() {
        let pm = make_test_peer_map().await;
        {
            let mut map = pm.map.write().await;
            map.insert("peer1".to_owned(), make_peer_with_reg_time(get_expired_time()));
        }
        assert_eq!(pm.count_online(30).await, 0);
    }

    #[hbb_common::tokio::test]
    async fn count_online_mixed() {
        let pm = make_test_peer_map().await;
        {
            let mut map = pm.map.write().await;
            map.insert("online".to_owned(), make_peer_with_reg_time(Instant::now()));
            map.insert("offline".to_owned(), make_peer_with_reg_time(get_expired_time()));
        }
        assert_eq!(pm.count_online(30).await, 1);
    }
}
