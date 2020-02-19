use rand::Rng;
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use futures::future;
use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use labrpc::RpcFuture;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}
// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    voted_for: Option<u64>,
    last_log_index: u64,    // TODO get last index from Persister
    last_commit_index: u64, // TODO get from Persister
    last_heartbeat: Instant,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            voted_for: None,
            last_log_index: 0,
            last_commit_index: 0,
            last_heartbeat: Instant::now(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        let _ = apply_ch;
        rf
        //crate::your_code_here((rf, apply_ch))
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             Ok(())
        //         }),
        // );
        // rx
        // ```
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        let peer = &self.peers[server];
        peer.spawn(
            peer.request_vote(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    let _ = tx.send(res);
                    Ok(())
                }),
        );
        rx
        //crate::your_code_here((server, args, tx, rx))
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, &Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>, // TODO tmp code
}
use std::sync::MutexGuard;
impl Node {
    fn send_heartbeat(mut guard: MutexGuard<'_, Raft>) {
        let args = AppendEntriesArgs {
            leader_id: guard.me as u64,
            leader_commit: guard.last_commit_index,
            entries: vec![],
            term: guard.state.term,
            prev_log_term: 0,  // TODO
            prev_log_index: 0, // TODO
        };
        for i in 0..guard.peers.len() {
            if i == guard.me {
                // TODO logs
                guard.last_heartbeat = Instant::now();
                continue;
            }
            //raft.peers.get(i).unwrap().append_entries(&args);
            let peer = guard.peers.get(i).unwrap();
            peer.spawn(
                peer.append_entries(&args)
                    .map_err(Error::Rpc)
                    .then(|_res| Ok(())),
            );
        }
    }
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let raft = Arc::new(Mutex::new(raft));
        let rf = raft.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(200));
            if rf.lock().unwrap().state.is_leader() {
                let raft = rf.lock().unwrap();
                Self::send_heartbeat(raft);
                continue;
            }
            // Since the tester limits the frequency of RPC calls,
            // election timeout is larger than 150ms-300ms in section5.2,
            let millis = rand::thread_rng().gen_range(1000, 4000);
            if rf.lock().unwrap().last_heartbeat.elapsed() > Duration::from_millis(millis) {
                // election
                let receivers = {
                    let mut raft = rf.lock().unwrap();
                    raft.state = Arc::new(State {
                        term: raft.state.term + 1,
                        is_leader: false,
                    });
                    raft.voted_for = Some(raft.me as u64);
                    raft.voted_for = Some(raft.me as u64);
                    let args = RequestVoteArgs {
                        candidate_id: raft.me as u64,
                        last_log_index: raft.last_log_index,
                        last_log_term: raft.state.term,
                        term: raft.state.term,
                    };
                    let mut receivers = Vec::with_capacity(raft.peers.len());
                    for i in 0..raft.peers.len() {
                        if i == raft.me {
                            continue;
                        }
                        let receiver = raft.send_request_vote(i, &args);
                        receivers.push(receiver);
                    }
                    receivers
                };
                let mut votes = 1;
                // TODO sleep
                let rpc_timeout = Duration::from_millis(200);
                std::thread::sleep(rpc_timeout);
                for rx in receivers.into_iter() {
                    if let Ok(Ok(reply)) = rx.try_recv() {
                        if reply.vote_granted {
                            votes += 1;
                        }
                    }
                }
                let mut raft = rf.lock().unwrap();
                let is_leader = votes > raft.peers.len() / 2;
                raft.state = Arc::new(State {
                    term: raft.state.term,
                    is_leader,
                });
                raft.voted_for = None;
                if is_leader {
                    Self::send_heartbeat(raft);
                }
            }
        });
        Node { raft }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        self.raft.lock().unwrap().start(command)
        // crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // crate::your_code_here(())
        self.raft.lock().unwrap().state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // crate::your_code_here(())
        self.raft.lock().unwrap().state.is_leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        let mut raft = self.raft.lock().unwrap();
        let current_term = raft.state.term();
        let mut resp = RequestVoteReply {
            term: current_term,
            vote_granted: false,
        };
        if raft.voted_for.is_some() && raft.voted_for != Some(args.candidate_id) {
            return Box::new(future::ok(resp));
        }
        if raft.last_log_index > args.last_log_index || current_term > args.term {
            return Box::new(future::ok(resp));
        }
        raft.voted_for = Some(args.candidate_id);
        raft.state = Arc::new(State {
            term: current_term,
            is_leader: false,
        });
        resp.vote_granted = true;
        Box::new(future::ok(resp))
        // Your code here (2A, 2B).
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        let current_term = raft.state.term();
        let mut resp = AppendEntriesReply {
            term: current_term,
            success: false,
        };
        if args.term < current_term {
            return Box::new(future::ok(resp));
        }
        // TODO
        // 1. Reply false if term < currentTerm (§5.1)
        // 2. Reply false if log doesn’t contain an entry at prevLogIndex
        // whose term matches prevLogTerm (§5.3)
        // 3. If an existing entry conflicts with a new one (same index
        // but different terms), delete the existing entry and all that
        // follow it (§5.3)
        // 4. Append any new entries not already in the log
        // 5. If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)
        //
        raft.state = Arc::new(State {
            is_leader: args.leader_id == raft.me as u64,
            term: args.term,
        });
        raft.voted_for = None;
        resp.success = true;

        raft.last_heartbeat = Instant::now();
        Box::new(future::ok(resp))
    }
}
