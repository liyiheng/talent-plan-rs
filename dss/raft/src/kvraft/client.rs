use crate::proto::kvraftpb::*;
use futures::Future;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

const ERR_RETRY_DUR: Duration = Duration::from_millis(200);

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    leader_id: AtomicUsize,
    req_id: AtomicU64,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        Clerk {
            name,
            servers,
            leader_id: AtomicUsize::new(0),
            req_id: AtomicU64::new(0),
        }
    }

    /// Increase the request id, and return the new value
    fn incr_req_id(&self) -> u64 {
        let prev = self.req_id.fetch_add(1, Ordering::SeqCst);
        prev + 1
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    pub fn get(&self, key: String) -> String {
        let req_id = self.incr_req_id();
        let req = GetRequest {
            key,
            client: self.name.clone(),
            req_id,
        };
        let server_cnt = self.servers.len();
        loop {
            let leader = self.leader_id.load(Ordering::Acquire);
            let fut = self.servers[leader].get(&req);
            if let Ok(v) = fut.wait() {
                if !v.wrong_leader && v.err.is_empty() {
                    return v.value;
                }
            }
            std::thread::sleep(ERR_RETRY_DUR);
            let leader = (leader + 1) % server_cnt;
            self.leader_id.store(leader, Ordering::Release);
        }
    }

    /// shared by Put and Append.
    fn put_append(&self, op: Op) {
        let n_servers = self.servers.len();
        let req_id = self.incr_req_id();
        let req = match op {
            Op::Put(k, v) => PutAppendRequest {
                key: k,
                value: v,
                op: 1,
                client: self.name.clone(),
                req_id,
            },
            Op::Append(k, v) => PutAppendRequest {
                key: k,
                value: v,
                op: 2,
                client: self.name.clone(),
                req_id,
            },
        };
        loop {
            let leader = self.leader_id.load(Ordering::Acquire);
            let fut = self.servers[leader].put_append(&req);
            if let Ok(v) = fut.wait() {
                if !v.wrong_leader && v.err.is_empty() {
                    return;
                }
            }
            std::thread::sleep(ERR_RETRY_DUR);
            let leader = (leader + 1) % n_servers;
            self.leader_id.store(leader, Ordering::Release);
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
