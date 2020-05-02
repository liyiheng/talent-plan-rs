use crate::proto::kvraftpb::*;
use crate::raft;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;

use crate::raft::errors::Error;
use futures::prelude::*;
use futures::sync::mpsc::unbounded;
use futures::sync::oneshot;
use futures::Stream;
use labrpc::RpcFuture;

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    data: Arc<RwLock<HashMap<String, String>>>,
    indexes: Arc<Mutex<HashMap<u64, Command>>>,
    last_reqs: Arc<Mutex<HashMap<String, u64>>>,
}

#[derive(Clone, PartialEq, Message)]
struct Command {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(string, optional, tag = "2")]
    value: Option<String>,
    #[prost(bool, tag = "3")]
    delete: bool,
    #[prost(int32, tag = "4")]
    op: i32,
    #[prost(string, tag = "5")]
    client: String,
    #[prost(uint64, tag = "6")]
    req_id: u64,
}

impl From<PutAppendRequest> for Command {
    fn from(req: PutAppendRequest) -> Command {
        Command {
            key: req.key,
            value: Some(req.value),
            delete: false,
            req_id: req.req_id,
            op: req.op,
            client: req.client,
        }
    }
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);
        let rf = raft::Node::new(rf);
        let server = KvServer {
            rf,
            me,
            maxraftstate,
            data: Arc::default(),
            indexes: Arc::default(),
            last_reqs: Arc::default(),
        };
        let data = server.data.clone();
        let indexes = server.indexes.clone();
        let last_reqs = server.last_reqs.clone();
        std::thread::spawn(move || {
            let _ = apply_ch.for_each(|msg| {
                if !msg.command_valid {
                    return Ok(());
                }
                let cmd: Command = labcodec::decode(&msg.command).unwrap();
                let cmd2 = cmd.clone();
                let mut req_ids_mutex = last_reqs.lock().unwrap();
                let req_id = req_ids_mutex.get(&cmd.client);
                if req_id.is_none() || *req_id.unwrap() < cmd.req_id {
                    req_ids_mutex.insert(cmd.client.clone(), cmd.req_id);
                    if cmd.delete {
                        data.write().unwrap().remove(&cmd.key);
                    } else {
                        data.write().unwrap().insert(cmd.key, cmd.value.unwrap());
                    }
                }
                indexes.lock().unwrap().insert(msg.command_index, cmd2);
                Ok(())
            });
            info!("apply_ch receiver finished");
        });

        server
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }
}

// Use a lock here, an easy implementation
#[derive(Clone)]
pub struct Node {
    server: Arc<Mutex<KvServer>>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        let server = Arc::new(Mutex::new(kv));
        Node { server }
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        self.server.lock().unwrap().rf.kill();
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        self.server.lock().unwrap().rf.get_state()
    }
}

impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        let GetRequest {
            key,
            client,
            req_id,
        } = arg;
        let cmd = Command {
            op: 3,
            key,
            value: None,
            delete: false,
            client,
            req_id,
        };
        let (sender, rx) = oneshot::channel();
        let server = self.server.clone();
        std::thread::spawn(move || {
            let wrong_leader = GetReply {
                err: String::new(),
                value: String::new(),
                wrong_leader: true,
            };
            if !server.lock().unwrap().rf.is_leader() {
                let _ = sender.send(Ok(wrong_leader));
                return;
            }
            let result = server.lock().unwrap().rf.start(&cmd);
            if let Err(e) = result {
                if let Error::NotLeader = e {
                    let _ = sender.send(Ok(wrong_leader));
                } else {
                    let _ = sender.send(Err(e));
                }
                return;
            }

            let (index, _term) = result.unwrap();
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let server = server.lock().unwrap();
                let mut indexes = server.indexes.lock().unwrap();
                if !indexes.contains_key(&index) {
                    continue;
                }
                let cmd_applied = indexes.get_mut(&index).take().unwrap();
                if cmd == *cmd_applied {
                    let reply_ok = GetReply {
                        wrong_leader: false,
                        value: server.data.read().unwrap().get(&cmd.key).unwrap().clone(),
                        err: "".to_owned(),
                    };
                    let _ = sender.send(Ok(reply_ok));
                    return;
                } else {
                    let _ = sender.send(Ok(wrong_leader));
                    return;
                }
            }

            let reply = GetReply {
                err: "Timeout".to_owned(),
                wrong_leader: false,
                value: "".to_owned(),
            };
            let _ = sender.send(Ok(reply));
        });
        Box::new(rx.then(|reply| match reply {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(e)) => Err(labrpc::Error::Other(e.to_string())),
            Err(e) => Err(labrpc::Error::Other(e.to_string())),
        }))
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        let wrong_leader = PutAppendReply {
            err: "Wrong leader".to_owned(),
            wrong_leader: true,
        };
        let reply_ok = PutAppendReply {
            err: String::default(),
            wrong_leader: false,
        };

        let server = self.server.clone();
        let cmd = Command::from(arg);
        let (sender, rx) = oneshot::channel();
        // TODO thread pool
        std::thread::spawn(move || {
            if !server.lock().unwrap().rf.is_leader() {
                let _ = sender.send(Ok(wrong_leader));
                return;
            }
            let result = server.lock().unwrap().rf.start(&cmd);
            if let Err(e) = result {
                if let Error::NotLeader = e {
                    let _ = sender.send(Ok(wrong_leader));
                } else {
                    let _ = sender.send(Err(e));
                }
                return;
            }
            let (index, _term) = result.unwrap();
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let server = server.lock().unwrap();
                let mut indexes = server.indexes.lock().unwrap();
                if !indexes.contains_key(&index) {
                    continue;
                }
                let cmd_applied = indexes.get_mut(&index).take().unwrap();
                if cmd == *cmd_applied {
                    let _ = sender.send(Ok(reply_ok));
                    return;
                } else {
                    let _ = sender.send(Ok(wrong_leader));
                    return;
                }
            }

            let reply = PutAppendReply {
                err: "Timeout".to_owned(),
                wrong_leader: false,
            };
            let _ = sender.send(Ok(reply));
        });
        Box::new(rx.then(|reply| match reply {
            Ok(Ok(reply)) => Ok(reply),
            _ => Err(labrpc::Error::Timeout),
        }))
    }
}
