use crate::proto::kvraftpb::*;
use crate::proto::raftpb::RaftClient;
use crate::raft;
use crate::raft::errors::Error;
use crate::raft::ApplyMsg;
use futures::future;
use futures::prelude::*;
use futures::sync::mpsc::unbounded;
use futures::sync::mpsc::UnboundedReceiver;
use futures::sync::mpsc::UnboundedSender;
use futures::sync::oneshot;
use futures::Stream;
use futures_timer::Delay;
use labrpc::RpcFuture;
use std::collections::HashMap;
use std::time::Duration;

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    last_applied: u64,
    data: HashMap<String, String>,
    cmd_chs: HashMap<u64, oneshot::Sender<(Command, String)>>,
    last_reqs: HashMap<String, u64>,
    apply_ch: Option<UnboundedReceiver<ApplyMsg>>,
}

/// Snapshot contains applied states and request ids of each client
#[derive(Clone, PartialEq, Message)]
struct Snapshot {
    #[prost(string, repeated, tag = "1")]
    keys: Vec<String>,
    #[prost(string, repeated, tag = "2")]
    values: Vec<String>,
    #[prost(string, repeated, tag = "3")]
    clients: Vec<String>,
    #[prost(uint64, repeated, tag = "4")]
    req_ids: Vec<u64>,
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
        servers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        let snapshot_data = persister.snapshot();
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);
        let rf = raft::Node::new(rf);
        let mut server = KvServer {
            last_applied: 0,
            rf: rf.clone(),
            me,
            maxraftstate,
            apply_ch: Some(apply_ch),
            cmd_chs: HashMap::default(),
            last_reqs: HashMap::default(),
            data: HashMap::default(),
        };
        if !snapshot_data.is_empty() {
            let snapshot = labcodec::decode(&snapshot_data).unwrap();
            server.apply_snapshot(snapshot);
        }
        server
    }

    // Create Snapshot with data needed
    fn dump_snapshot(&self) -> Snapshot {
        let data_len = self.data.len();
        let mut keys = Vec::with_capacity(data_len);
        let mut values = Vec::with_capacity(data_len);
        for (k, v) in self.data.iter() {
            keys.push(k.clone());
            values.push(v.clone());
        }
        let n_clients = self.last_reqs.len();
        let mut clients = Vec::with_capacity(n_clients);
        let mut req_ids = Vec::with_capacity(n_clients);
        for (k, v) in self.last_reqs.iter() {
            clients.push(k.clone());
            req_ids.push(*v);
        }
        Snapshot {
            keys,
            values,
            clients,
            req_ids,
        }
    }

    // apply the snapshot to states
    fn apply_snapshot(&mut self, snapshot: Snapshot) {
        let Snapshot {
            keys,
            values,
            clients,
            req_ids,
        } = snapshot;
        for (k, v) in keys.into_iter().zip(values.into_iter()) {
            self.data.insert(k, v);
        }
        for (k, v) in clients.into_iter().zip(req_ids.into_iter()) {
            self.last_reqs.insert(k, v);
        }
    }

    /// Dispatch events
    fn handle_event(&mut self, event: Event) {
        match event {
            Event::PutAppend(arg, sender) => {
                self.handle_put_append(arg, sender);
            }
            Event::Get(arg, sender) => {
                self.handle_get(arg, sender);
            }
            Event::Apply(msg) => {
                self.handle_apply_msg(msg);
            }
            Event::GetRaftState(sender) => {
                let _ = sender.send(self.rf.get_state());
            }
            Event::Shutdown => unreachable!(),
        }
    }
    fn handle_apply_msg(&mut self, msg: ApplyMsg) {
        if !msg.command_valid {
            if let Ok(snapshot) = labcodec::decode(&msg.command) {
                self.apply_snapshot(snapshot);
            } else {
                error!("Failed to decode snapshot");
            }
            return;
        }
        let cmd: Command = labcodec::decode(&msg.command).unwrap();
        let index = msg.command_index;
        self.last_applied = index;
        let cmd2 = cmd.clone();
        let req_id = self.last_reqs.get(&cmd.client);
        // Ignore executed reqeusts
        if req_id.is_none() || *req_id.unwrap() < cmd.req_id {
            self.last_reqs.insert(cmd.client.clone(), cmd.req_id);
            // 1. put 2. append 3. get
            match cmd.op {
                1 => {
                    self.data.insert(cmd.key, cmd.value.unwrap());
                }
                2 => {
                    self.data
                        .entry(cmd.key)
                        .or_default()
                        .push_str(&cmd.value.unwrap_or_default());
                }
                3 => {}
                _ => {}
            }
        }
        let value = self.data.get(&cmd2.key).cloned().unwrap_or_default();
        if let Some(tx) = self.cmd_chs.remove(&index) {
            let _ = tx.send((cmd2, value));
        }
        if let Some(max_size) = self.maxraftstate {
            if max_size * 9 / 10 < self.rf.get_persist_size() {
                let snapshot = self.dump_snapshot();
                let mut data = vec![];
                if let Ok(()) = labcodec::encode(&snapshot, &mut data) {
                    let _ = self.rf.snapshot(data, self.last_applied);
                }
            }
        }
    }

    fn handle_get(&mut self, arg: GetRequest, sender: oneshot::Sender<GetReply>) {
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

        let result = self.rf.start(&cmd);
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
        if self.cmd_chs.contains_key(&index) {
            let _ = sender.send(wrong_leader);
            return;
        }
        // Create a channel to wait the command be applied
        let rx = {
            let (tx, rx) = oneshot::channel();
            self.cmd_chs.insert(index, tx);
            rx
        };
        std::thread::spawn(move || {
            let _ = rx
                .select2(Delay::new(Duration::from_secs(1)))
                .and_then(move |either| {
                    match either {
                        future::Either::A(((cmd_applied, value), _)) => {
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
                        future::Either::B(_) => {
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

    fn handle_put_append(
        &mut self,
        arg: PutAppendRequest,
        sender: oneshot::Sender<PutAppendReply>,
    ) {
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
        let result = self.rf.start(&cmd);
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
        let rx = {
            let (tx, rx) = oneshot::channel();
            self.cmd_chs.insert(index, tx);
            rx
        };
        // An ugly but easy implementation
        std::thread::spawn(move || {
            let _ = rx
                .select2(Delay::new(Duration::from_secs(1)))
                .and_then(move |either| {
                    match either {
                        future::Either::A(((cmd_applied, _), _)) => {
                            if cmd_applied == cmd {
                                let _ = sender.send(reply_ok);
                            } else {
                                let _ = sender.send(wrong_leader);
                            }
                        }
                        future::Either::B(_) => {
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

// Node communicate with KvServer through a channel
#[derive(Clone)]
pub struct Node {
    event_sender: UnboundedSender<Event>,
}

enum Event {
    Shutdown,
    PutAppend(PutAppendRequest, oneshot::Sender<PutAppendReply>),
    Get(GetRequest, oneshot::Sender<GetReply>),
    Apply(ApplyMsg),
    GetRaftState(oneshot::Sender<raft::State>),
}

impl Node {
    pub fn new(mut kv: KvServer) -> Node {
        let apply_ch = kv.apply_ch.take().unwrap();
        let (tx, rx) = unbounded();
        let node = Node { event_sender: tx };
        std::thread::spawn(move || {
            let kv = std::cell::RefCell::new(kv);
            let _ = apply_ch
                .map(Event::Apply)
                .select(rx)
                .take_while(|e| {
                    if let Event::Shutdown = e {
                        kv.borrow().rf.kill();
                        Ok(false)
                    } else {
                        Ok(true)
                    }
                })
                .for_each(|event| {
                    kv.borrow_mut().handle_event(event);
                    Ok(())
                })
                .wait();
            info!("Comsumer thread finished");
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
        let (tx, rx) = oneshot::channel();
        self.event_sender
            .unbounded_send(Event::GetRaftState(tx))
            .unwrap();
        rx.wait().unwrap()
    }
}

impl KvService for Node {
    // Send args and a sender to KvServer, return the receiver as a RpcFuture
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.event_sender.unbounded_send(Event::Get(arg, tx)) {
            return Box::new(future::err(labrpc::Error::Other(e.to_string())));
        }
        Box::new(rx.then(|reply| match reply {
            Ok(reply) => Ok(reply),
            Err(e) => Err(labrpc::Error::Other(e.to_string())),
        }))
    }

    // Send args and a sender to KvServer, return the receiver as a RpcFuture
    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.event_sender.unbounded_send(Event::PutAppend(arg, tx)) {
            return Box::new(future::err(labrpc::Error::Other(e.to_string())));
        }
        Box::new(rx.then(|reply| match reply {
            Ok(reply) => Ok(reply),
            Err(e) => Err(labrpc::Error::Other(e.to_string())),
        }))
    }
}
