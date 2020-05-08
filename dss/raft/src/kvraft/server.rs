use crate::proto::kvraftpb::*;
use crate::raft;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::raft::errors::Error;
use futures::prelude::*;
use futures::sync::mpsc::unbounded;
use futures::sync::mpsc::UnboundedSender;
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
    cmd_chs: HashMap<u64, oneshot::Sender<(Command, String)>>,
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
                    let value = inner.data.get(&cmd2.key).cloned().unwrap_or_default();
                    if let Some(tx) = inner.cmd_chs.remove(&index) {
                        let _ = tx.send((cmd2, value));
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

    fn handle_event(&self, event: Event) {
        match event {
            Event::PutAppend(arg, sender) => {
                self.handle_put_append(arg, sender);
            }
            Event::Get(arg, sender) => {
                self.handle_get(arg, sender);
            }
            Event::Shutdown => unreachable!(),
        }
    }
    fn handle_get(&self, arg: GetRequest, sender: oneshot::Sender<GetReply>) {
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
        if !self.rf.is_leader() {
            let _ = sender.send(wrong_leader);
            return;
        }

        let result = self.start(&cmd);
        if let Err(e) = result {
            if let Error::NotLeader = e {
                let _ = sender.send(wrong_leader);
            } else {
                let _ = sender.send(GetReply {
                    err: e.to_string(),
                    value: "".to_owned(),
                    wrong_leader: false,
                });
            }
            return;
        }
        let (index, _) = result.unwrap();
        let mut inner = self.inner.lock().unwrap();
        if inner.cmd_chs.contains_key(&index) {
            let _ = sender.send(wrong_leader);
            return;
        }
        // Create a channel to wait the command be applied
        let rx = {
            let (tx, rx) = oneshot::channel();
            inner.cmd_chs.insert(index, tx);
            rx
        };
        std::thread::spawn(move || {
            let _ = rx
                .select2(futures_timer::Delay::new(Duration::from_secs(5)))
                .and_then(move |either| {
                    match either {
                        futures::future::Either::A(((cmd_applied, value), _)) => {
                            let r = GetReply {
                                value,
                                err: "".to_owned(),
                                wrong_leader: false,
                            };
                            if cmd_applied == cmd {
                                let _ = sender.send(r);
                            } else {
                                let _ = sender.send(wrong_leader);
                            }
                        }
                        futures::future::Either::B(_) => {
                            let _ = sender.send(GetReply {
                                err: "Timeout ".to_owned(),
                                value: "".to_owned(),
                                wrong_leader: false,
                            });
                        }
                    };
                    Ok(())
                })
                .wait();
        });
    }

    fn handle_put_append(&self, arg: PutAppendRequest, sender: oneshot::Sender<PutAppendReply>) {
        let wrong_leader = PutAppendReply {
            err: "Wrong leader".to_owned(),
            wrong_leader: true,
        };
        let reply_ok = PutAppendReply {
            err: String::default(),
            wrong_leader: false,
        };
        if !self.rf.is_leader() {
            let _ = sender.send(wrong_leader);
            return;
        }
        let cmd = Command::from(arg);
        let result = self.start(&cmd);
        if let Err(e) = result {
            if let Error::NotLeader = e {
                let _ = sender.send(wrong_leader);
            } else {
                let _ = sender.send(PutAppendReply {
                    err: e.to_string(),
                    wrong_leader: false,
                });
            }
            return;
        }
        let (index, _term) = result.unwrap();
        let mut inner = self.inner.lock().unwrap();
        if inner.cmd_chs.contains_key(&index) {
            // Already a sender there
            let _ = sender.send(wrong_leader);
            return;
        }
        let rx = {
            let (tx, rx) = oneshot::channel();
            inner.cmd_chs.insert(index, tx);
            rx
        };
        // An ugly but easy implementation
        std::thread::spawn(move || {
            let _ = rx
                .select2(futures_timer::Delay::new(Duration::from_secs(5)))
                .and_then(move |either| {
                    match either {
                        futures::future::Either::A(((cmd_applied, _), _)) => {
                            if cmd_applied == cmd {
                                let _ = sender.send(reply_ok);
                            } else {
                                let _ = sender.send(wrong_leader);
                            }
                        }
                        futures::future::Either::B(_) => {
                            let _ = sender.send(PutAppendReply {
                                err: "Timeout ".to_owned(),
                                wrong_leader: false,
                            });
                        }
                    };
                    Ok(())
                })
                .wait();
        });
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
    event_sender: UnboundedSender<Event>,
}

// Node communicate with KvServer through Event and
// a channel
enum Event {
    Shutdown,
    PutAppend(PutAppendRequest, oneshot::Sender<PutAppendReply>),
    Get(GetRequest, oneshot::Sender<GetReply>),
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        let server = Arc::new(Mutex::new(kv));
        let (tx, rx) = unbounded();
        let node = Node {
            server: server.clone(),
            event_sender: tx,
        };
        std::thread::spawn(move || {
            rx.take_while(|e| {
                if let Event::Shutdown = e {
                    Ok(false)
                } else {
                    Ok(true)
                }
            })
            .for_each(|event| {
                server.lock().unwrap().handle_event(event);
                Ok(())
            })
            .wait()
            .unwrap();
        });
        node
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        let _ = self.event_sender.unbounded_send(Event::Shutdown);
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
    // Send args and a sender to KvServer, return the receiver as a RpcFuture
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        let (a, b) = oneshot::channel();
        let _ = self.event_sender.unbounded_send(Event::Get(arg, a));
        Box::new(b.then(|reply| match reply {
            Ok(reply) => Ok(reply),
            Err(e) => Err(labrpc::Error::Other(e.to_string())),
        }))
    }

    // Send args and a sender to KvServer, return the receiver as a RpcFuture
    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        let (a, b) = oneshot::channel();
        let _ = self.event_sender.unbounded_send(Event::PutAppend(arg, a));
        Box::new(b.then(|reply| match reply {
            Ok(reply) => Ok(reply),
            Err(e) => Err(labrpc::Error::Other(e.to_string())),
        }))
    }
}
