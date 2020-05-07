use crate::proto::kvraftpb::*;
use crate::raft;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
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
    inner: Arc<Mutex<ServerData>>,
}

// ServerData warps fields of a KvServer that needed to be
// shared between threads
#[derive(Default)]
struct ServerData {
    last_commit: u64,
    data: HashMap<String, String>,
    cmd_chs: HashMap<u64, oneshot::Sender<Command>>,
    last_reqs: HashMap<String, u64>,
}

#[derive(Clone, PartialEq, Message)]
struct Command {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(string, optional, tag = "2")]
    value: Option<String>,
    #[prost(int32, tag = "3")]
    op: i32,
    #[prost(string, tag = "4")]
    client: String,
    #[prost(uint64, tag = "5")]
    req_id: u64,
}

impl From<PutAppendRequest> for Command {
    fn from(req: PutAppendRequest) -> Command {
        Command {
            key: req.key,
            value: Some(req.value),
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
            inner: Arc::default(),
            rf,
            me,
            maxraftstate,
        };
        let inner = server.inner.clone();
        // Spawn a new thread to receive applied commands
        std::thread::spawn(move || {
            let _ = apply_ch
                .filter(|msg| msg.command_valid)
                .map(|msg| {
                    let cmd: Command = labcodec::decode(&msg.command).unwrap();
                    (cmd, msg.command_index)
                })
                .for_each(|(cmd, index)| {
                    let mut inner = inner.lock().unwrap();
                    inner.last_commit = index;
                    let cmd2 = cmd.clone();
                    let req_id = inner.last_reqs.get(&cmd.client);
                    // Ignore executed reqeusts
                    if req_id.is_none() || *req_id.unwrap() < cmd.req_id {
                        inner.last_reqs.insert(cmd.client.clone(), cmd.req_id);

                        // 1. put 2. append 3. get
                        match cmd.op {
                            1 => {
                                inner.data.insert(cmd.key, cmd.value.unwrap());
                            }
                            2 => {
                                inner
                                    .data
                                    .entry(cmd.key)
                                    .or_default()
                                    .push_str(&cmd.value.unwrap_or_default());
                            }
                            3 => {}
                            _ => {}
                        }
                    }
                    if let Some(tx) = inner.cmd_chs.remove(&index) {
                        let _ = tx.send(cmd2);
                    }
                    Ok(())
                })
                .wait();
            warn!("apply_ch receiver finished");
        });
        server
    }

    // Start a command, and check raft state size if needed, if the size touched
    // the limitation, create a snapshot
    fn start(&self, cmd: &Command) -> Result<(u64, u64), raft::errors::Error> {
        let result = self.rf.start(cmd);
        if let Some(max_size) = self.maxraftstate {
            if max_size < self.rf.get_persist_size() {
                let inner = self.inner.lock().unwrap();
                let mut cmds = Vec::with_capacity(inner.data.len());
                for (k, v) in inner.data.iter() {
                    // TODO duplicate reqeusts checking
                    cmds.push(Command {
                        op: 1,
                        key: k.clone(),
                        value: Some(v.clone()),
                        req_id: 0,
                        client: "TODO".to_owned(),
                    });
                }
                self.rf.snapshot(cmds, inner.last_commit)?;
            }
        }
        result
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
            client,
            req_id,
        };
        let wrong_leader = GetReply {
            err: String::new(),
            value: String::new(),
            wrong_leader: true,
        };
        if !self.is_leader() {
            return Box::new(futures::future::ok(wrong_leader));
        }

        let server = self.server.clone();
        let (sender, rx) = oneshot::channel();

        // Should use combinators of futures here.
        // But used a new thread and  the `wait()` method of a future,
        // it's a bad idea, but very easy.
        std::thread::spawn(move || {
            let result = server.lock().unwrap().start(&cmd);
            if let Err(e) = result {
                if let Error::NotLeader = e {
                    let _ = sender.send(Ok(wrong_leader));
                } else {
                    let _ = sender.send(Err(e));
                }
                return;
            }
            let (index, _) = result.unwrap();

            // If there is a sender for 'index' already,
            // it was put by another request
            let inner = server.lock().unwrap().inner.clone();
            if inner.lock().unwrap().cmd_chs.contains_key(&index) {
                let _ = sender.send(Ok(wrong_leader));
                return;
            }

            // Create a channel to wait the command be applied
            let rx = {
                let (tx, rx) = oneshot::channel();
                inner.lock().unwrap().cmd_chs.insert(index, tx);
                rx
            };

            if let Some(cmd_applied) = recv_with_timeout(rx) {
                // cmd_applied is same with cmd means this server is the leader,
                // and the command was commited.
                if cmd == cmd_applied {
                    let v = inner
                        .lock()
                        .unwrap()
                        .data
                        .get(&cmd.key)
                        .cloned()
                        .unwrap_or_default();
                    let reply_ok = GetReply {
                        wrong_leader: false,
                        value: v,
                        err: "".to_owned(),
                    };
                    let _ = sender.send(Ok(reply_ok));
                    return;
                } else {
                    let _ = sender.send(Ok(wrong_leader));
                    return;
                }
            }
            let err = Err(raft::errors::Error::Rpc(labrpc::Error::Timeout));
            let _ = sender.send(err);
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
        if !self.is_leader() {
            return Box::new(futures::future::ok(wrong_leader));
        }

        let server = self.server.clone();
        let cmd = Command::from(arg);
        let (sender, rx) = oneshot::channel();
        std::thread::spawn(move || {
            let result = server.lock().unwrap().start(&cmd);
            if let Err(e) = result {
                if let Error::NotLeader = e {
                    let _ = sender.send(Ok(wrong_leader));
                } else {
                    let _ = sender.send(Err(e));
                }
                return;
            }
            let (index, _term) = result.unwrap();
            let inner = server.lock().unwrap().inner.clone();
            if inner.lock().unwrap().cmd_chs.contains_key(&index) {
                let _ = sender.send(Ok(wrong_leader));
                return;
            }
            let rx = {
                let (tx, rx) = oneshot::channel();
                inner.lock().unwrap().cmd_chs.insert(index, tx);
                rx
            };
            if let Some(cmd_applied) = recv_with_timeout(rx) {
                if cmd == cmd_applied {
                    let _ = sender.send(Ok(reply_ok));
                    return;
                } else {
                    let _ = sender.send(Ok(wrong_leader));
                    return;
                }
            }
            let err = Err(raft::errors::Error::Rpc(labrpc::Error::Timeout));
            let _ = sender.send(err);
        });
        Box::new(rx.then(|reply| match reply {
            Ok(Ok(reply)) => Ok(reply),
            _ => Err(labrpc::Error::Timeout),
        }))
    }
}

fn recv_with_timeout(rx: oneshot::Receiver<Command>) -> Option<Command> {
    if let Ok((cmd_applied, _)) = rx
        .map_err(|_| ())
        .map(Some)
        .select(
            futures_timer::Delay::new(Duration::from_secs(5))
                .map_err(|_| ())
                .map(|_| None),
        )
        .wait()
    {
        cmd_applied
    } else {
        None
    }
}
