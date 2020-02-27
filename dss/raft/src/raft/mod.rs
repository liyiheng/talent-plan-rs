use rand::Rng;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use futures::future;
use futures::sync::mpsc::UnboundedSender;
use futures::sync::oneshot;
use futures::{Future, Stream};
use futures_timer::{Delay, Interval};
use labrpc::{Error as RpcError, RpcFuture};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

const RPC_TIMEOUT: Duration = Duration::from_millis(10);

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

#[derive(Clone, PartialEq, Message)]
struct PersistentState {
    #[prost(uint64, tag = "1")]
    current_term: u64,
    #[prost(uint64, optional, tag = "2")]
    voted_for: Option<u64>,
    #[prost(message, repeated, tag = "3")]
    log: Vec<LogEntry>,
}

#[derive(Default)]
struct LeaderState {
    next_index: Vec<usize>,
    match_index: Vec<usize>,
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
    // voted_for: Option<u64>,
    apply_ch: UnboundedSender<ApplyMsg>,
    event_ch: Option<UnboundedSender<Event>>,
    commit_index: usize,
    last_applied: usize,
    last_heartbeat: Instant,
    persistent_state: PersistentState,
    leader_state: LeaderState,
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
        let persistent_state = labcodec::decode::<PersistentState>(&raft_state).unwrap();
        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            apply_ch,
            event_ch: None,
            commit_index: 0,
            last_applied: 0,
            last_heartbeat: Instant::now(),
            persistent_state,
            leader_state: LeaderState::default(),
        };
        // initialize from state persisted before a crash
        rf.restore(&raft_state);
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
    ) -> oneshot::Receiver<Result<RequestVoteReply>> {
        let (tx, rx) = oneshot::channel();
        // let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        if server == self.me {
            let _ = tx.send(Ok(RequestVoteReply {
                vote_granted: true,
                term: self.state.term,
            }));
            return rx;
        }
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

    // Trait Message cannot be made into object, using handle_cmd and Vec<u8>
    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if !self.state.is_leader() {
            return Err(Error::NotLeader);
        }
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        if !self.state.is_leader() {
            return Err(Error::NotLeader);
        }
        let entry = LogEntry {
            data: buf,
            term: self.state.term,
        };
        self.persistent_state.log.push(entry);
        let (tx, _rx) = oneshot::channel();
        self.send_heartbeat(Some(tx));
        // rx.wait()
        Ok((self.persistent_state.log.len() as u64, self.state.term))
        // Your code here (2B).
    }

    fn handle_cmd2(&mut self, cmd: Vec<u8>, tx: oneshot::Sender<Reply>) {
        if !self.state.is_leader() {
            let _ = tx.send(Reply::Cmd(Err(Error::NotLeader)));
            return;
        }
        let entry = LogEntry {
            data: cmd,
            term: self.state.term,
        };
        self.persistent_state.log.push(entry);
        self.send_heartbeat(Some(tx));
    }

    fn send_heartbeat(&mut self, sender: Option<oneshot::Sender<Reply>>) {
        let mut receivers = Vec::with_capacity(self.peers.len());
        for i in 0..self.peers.len() {
            let rx = self
                .append_entries_to(i)
                .map(Some)
                .map_err(|_| ())
                .select(Delay::new(RPC_TIMEOUT).map_err(|_| ()).map(|_| None));
            receivers.push(rx);
        }
        let peer = &self.peers[self.me];
        let event_sender = self.event_ch.clone().unwrap();
        let peer_count = self.peers.len();
        let (index, term) = (self.persistent_state.log.len(), self.state.term);
        peer.spawn({
            futures::stream::futures_unordered(receivers)
                .fold(vec![], |mut acc, (v, _)| {
                    if let Some(v) = v {
                        acc.push(v);
                    }
                    future::ok(acc)
                })
                .then(move |replies| {
                    let timeout_reply = Reply::Cmd(Err(Error::Rpc(RpcError::Timeout)));
                    if replies.is_err() {
                        if let Some(sender) = sender {
                            let _ = sender.send(timeout_reply);
                        }
                        return Ok(());
                    }
                    let replies = replies.ok().unwrap();
                    if replies.is_empty() {
                        if let Some(sender) = sender {
                            let _ = sender.send(timeout_reply);
                        }
                        return Ok(());
                    }
                    let mut amt = 0;
                    for (i, reply) in replies {
                        if reply.is_err() {
                            continue;
                        }
                        let reply = reply.unwrap();
                        // TODO move to append_entries_to
                        let _ = event_sender.unbounded_send(Event::AppendEntriesResult(
                            i,
                            index,
                            reply.clone(),
                        ));
                        if reply.success {
                            amt += 1;
                        } else if term < reply.term {
                            if let Some(sender) = sender {
                                let _ = sender.send(Reply::Cmd(Err(errors::Error::NotLeader)));
                            }
                            return Ok(());
                        }
                    }
                    if let Some(sender) = sender {
                        if amt > peer_count / 2 {
                            let _ = sender.send(Reply::Cmd(Ok((index as u64, term))));
                        } else {
                            let _ = sender.send(timeout_reply);
                        }
                    }
                    Ok(())
                })
        });
    }
}

impl Raft {
    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let current_term = self.state.term();
        let mut resp = AppendEntriesReply {
            term: current_term,
            success: false,
        };
        // 1. Reply false if term < currentTerm (§5.1)
        if args.term < current_term {
            warn!(
                "{} refused {}, {} < {}",
                self.me, args.leader_id, args.term, current_term
            );
            return resp;
        }
        // 2. Reply false if log doesn’t contain an entry at prevLogIndex
        // whose term matches prevLogTerm (§5.3)
        let prev_i = args.prev_log_index as usize;
        if prev_i > 0 {
            let prev_t = args.prev_log_term;
            let prev_log = self.persistent_state.log.get(prev_i - 1);
            if prev_log.is_none() || prev_log.unwrap().term != prev_t {
                warn!(
                    "{} refused {}, doesn’t contain an entry at prev_log_index",
                    self.me, args.leader_id
                );

                return resp;
            }
        }
        // 3. If an existing entry conflicts with a new one (same index
        // but different terms), delete the existing entry and all that
        // follow it (§5.3)
        // 4. Append any new entries not already in the log
        let last_i = prev_i + args.entries.len();
        for (j, entry) in args.entries.into_iter().enumerate() {
            let t = entry.term;
            let i = prev_i + j;
            if i >= self.persistent_state.log.len() {
                self.persistent_state.log.push(entry);
            } else if self.persistent_state.log[i].term != t {
                self.persistent_state.log[i] = entry;
            }
        }
        if last_i < self.persistent_state.log.len() {
            self.persistent_state.log.split_off(last_i);
        }
        // 5. If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)
        if args.leader_commit > self.commit_index as u64 {
            let i = self.persistent_state.log.len();
            self.commit_index = i.min(args.leader_commit as usize);
        }
        if args.term != self.state.term {
            warn!(
                "{} => {}, term: {} => {}",
                args.leader_id, self.me, self.state.term, args.term,
            );
        }
        self.update_state(false, args.term);
        self.persistent_state.voted_for = None;
        resp.success = true;
        self.last_heartbeat = Instant::now();
        resp
    }

    fn update_state(&mut self, is_leader: bool, term: u64) {
        let t = self.state.term;
        if t > term {
            error!("TERM OF {} DECREASED FROM {} TO {}", self.me, t, term);
        }
        self.state = Arc::new(State { is_leader, term });
    }

    // index starts from 1
    fn get_log(&self, index: usize) -> Option<&LogEntry> {
        if index == 0 {
            None
        } else {
            self.persistent_state.log.get(index - 1)
        }
    }

    fn handle_vote_request(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let current_term = self.state.term();
        let mut resp = RequestVoteReply {
            term: current_term,
            vote_granted: false,
        };
        if current_term > args.term {
            warn!("{} refused {} , term", self.me, args.candidate_id);
            return resp;
        }

        let voted_for = self.persistent_state.voted_for;
        if current_term == args.term && voted_for.is_some() && voted_for != Some(args.candidate_id)
        {
            warn!(
                "{} refused {} , voted_for:{:?}",
                self.me, args.candidate_id, voted_for
            );
            return resp;
        }
        let last_log_index = self.persistent_state.log.len() as u64;
        let last_log_term = self
            .persistent_state
            .log
            .last()
            .map(|l| l.term)
            .unwrap_or(0);
        if last_log_index > args.last_log_index || last_log_term > args.term {
            return resp;
        }
        self.persistent_state.voted_for = Some(args.candidate_id);
        self.update_state(false, args.term);
        resp.vote_granted = true;
        resp
    }

    fn start_election(&mut self) {
        let term = self.state.term + 1;
        self.update_state(false, term);
        self.persistent_state.voted_for = Some(self.me as u64);
        let args = RequestVoteArgs {
            candidate_id: self.me as u64,
            last_log_index: self.persistent_state.log.len() as u64,
            last_log_term: self.state.term,
            term: self.state.term,
        };
        let mut receivers = Vec::with_capacity(self.peers.len());
        for i in 0..self.peers.len() {
            let rx = self.send_request_vote(i, &args).map_err(|_| ()).map(Some);
            let rx = rx.select(Delay::new(RPC_TIMEOUT).map_err(|_| ()).map(|_| None));
            receivers.push(rx);
        }
        let rx = futures::stream::futures_unordered(receivers);
        let event_sender = self.event_ch.clone().unwrap();
        let fut = rx
            .fold(vec![], |mut acc, (v, _)| {
                if let Some(v) = v {
                    acc.push(v);
                }
                future::ok(acc)
            })
            .map_err(|_| RpcError::Timeout)
            .then(move |result| match result {
                Ok(v) => {
                    if v.is_empty() {
                        error!("VOTE RESULT IS EMPTY");
                        Ok(())
                    } else {
                        let mut votes = 0;
                        for e in v {
                            if let Ok(e) = e {
                                if e.vote_granted {
                                    votes += 1;
                                }
                            }
                        }
                        let _ = event_sender.unbounded_send(Event::VoteResult(term, votes));
                        Ok(())
                    }
                }
                Err(_) => Ok(()),
            });
        self.peers[self.me].spawn(fut);
    }

    fn append_entries_to(
        &mut self,
        i: usize,
    ) -> oneshot::Receiver<(usize, Result<AppendEntriesReply>)> {
        let (sender, rx) = oneshot::channel();
        if i == self.me {
            self.last_heartbeat = Instant::now();
            let _ = sender.send((
                self.me,
                Ok(AppendEntriesReply {
                    success: true,
                    term: self.state.term,
                }),
            ));
            return rx;
        }
        let peer = &self.peers[i];
        let i_next = self.leader_state.next_index[i];
        let next = self.persistent_state.log.len() + 1;
        let entries = if i_next == next {
            vec![]
        } else {
            let mut entries = vec![];
            let src = &self.persistent_state.log[i_next - 1..];
            entries.extend(src.iter().cloned());
            entries
        };
        let prev_term = self.get_log(i_next - 1).map(|l| l.term).unwrap_or(0);
        let args = AppendEntriesArgs {
            entries,
            leader_commit: self.commit_index as u64,
            leader_id: self.me as u64,
            prev_log_index: i_next as u64 - 1,
            term: self.state.term,
            prev_log_term: prev_term,
        };
        peer.spawn({
            peer.append_entries(&args)
                .map_err(Error::Rpc)
                .then(move |reply| {
                    info!("Result from {}, {:?}", i, reply);
                    let _ = sender.send((i, reply));
                    Ok(())
                })
        });
        rx
    }

    fn handle_vote_result(&mut self, term: u64, cnt: usize) {
        if term < self.state.term {
            return;
        }
        let is_leader = cnt > self.peers.len() / 2;
        self.update_state(is_leader, self.state.term);
        self.persistent_state.voted_for = None;
        if is_leader {
            let log_size = self.persistent_state.log.len();
            self.leader_state = LeaderState {
                next_index: vec![log_size + 1; self.peers.len()],
                match_index: vec![0; self.peers.len()],
            };
            self.send_heartbeat(None);
        }
    }

    fn step(&mut self) {
        while self.commit_index > self.last_applied {
            self.last_applied += 1;
            let _ = self.apply_ch.unbounded_send(ApplyMsg {
                command_valid: true,
                command_index: self.last_applied as u64,
                command: self.persistent_state.log[self.last_applied - 1]
                    .data
                    .clone(),
            });
        }
        // Heartbeat
        if self.state.is_leader() {
            // If there exists an N such that N > commitIndex, a majority
            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            // set commitIndex = N (§5.3, §5.4).
            let max_match = *self.leader_state.match_index.iter().max().unwrap();
            let majority = self.peers.len() / 2;
            if max_match > self.commit_index {
                for n in (self.commit_index + 1..=max_match).rev() {
                    let c = self
                        .leader_state
                        .match_index
                        .iter()
                        .filter(|v| **v >= n)
                        .count();
                    if c > majority && self.persistent_state.log[n - 1].term == self.state.term {
                        self.commit_index = n;
                        break;
                    }
                }
            }
            self.send_heartbeat(None);
            return;
        }
        // Since the tester limits the frequency of RPC calls,
        // election timeout is larger than 150ms-300ms in section5.2,
        let millis = rand::thread_rng().gen_range(500, 1000);
        let election_timeout = Duration::from_millis(millis);
        if self.last_heartbeat.elapsed() > election_timeout {
            self.last_heartbeat = Instant::now();
            info!("{} start election", self.me);
            self.start_election();
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
        let _ = &self.apply_ch;
        let _ = &self.last_applied;
        let _ = &self.leader_state.next_index;
        let _ = &self.leader_state.match_index;
        let (tx, _) = oneshot::channel();
        self.handle_cmd2(vec![], tx);
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
    sender: UnboundedSender<(Args, oneshot::Sender<Reply>)>,
    raft: Arc<Mutex<Raft>>,
}

enum Args {
    RequestVote(RequestVoteArgs),
    AppendEntries(AppendEntriesArgs),
    Cmd(Vec<u8>),
}

#[derive(Clone)]
enum Reply {
    RequestVote(RequestVoteReply),
    AppendEntries(AppendEntriesReply),
    Cmd(Result<(u64, u64)>),
}

enum Event {
    VoteResult(u64, usize),
    // peer_id, index, reply
    AppendEntriesResult(usize, usize, AppendEntriesReply),
}

impl Node {
    fn start_raft_thread(
        raft: Arc<Mutex<Raft>>,
    ) -> UnboundedSender<(Args, oneshot::Sender<Reply>)> {
        let (tx, rx) = futures::sync::mpsc::unbounded();
        let (event_tx, event_rx) = futures::sync::mpsc::unbounded();
        raft.lock().unwrap().event_ch = Some(event_tx);
        std::thread::spawn(move || {
            let rx = rx
                .map_err(|_| ())
                .map(|(args, sender): (Args, oneshot::Sender<Reply>)| (None, Some((args, sender))));
            let event_rx = event_rx.map_err(|_| ()).map(|v: Event| (Some(v), None));

            Interval::new(Duration::from_millis(200))
                .map(|_| (None, None))
                .map_err(|_| ())
                .select(rx)
                .select(event_rx)
                .for_each(|(event, args)| {
                    {
                        let r = raft.lock().unwrap();
                        if r.state.is_leader {
                            info!(
                                "LeaderInterval {} {}, {:?}",
                                r.me,
                                r.state.term,
                                r.persistent_state.log.last()
                            );
                        }
                    };
                    if let Some(args) = args {
                        match args {
                            (Args::RequestVote(args), tx) => {
                                let resp = raft.lock().unwrap().handle_vote_request(args);
                                let _ = tx.send(Reply::RequestVote(resp));
                            }
                            (Args::AppendEntries(args), tx) => {
                                let resp = raft.lock().unwrap().handle_append_entries(args);
                                let _ = tx.send(Reply::AppendEntries(resp));
                            }
                            (Args::Cmd(dat), tx) => {
                                raft.lock().unwrap().handle_cmd2(dat, tx);
                            }
                        }
                        return Ok(());
                    }
                    if let Some(event) = event {
                        match event {
                            Event::VoteResult(term, cnt) => {
                                raft.lock().unwrap().handle_vote_result(term, cnt);
                            }
                            Event::AppendEntriesResult(peer, index, reply) => {
                                let mut rf = raft.lock().unwrap();
                                if reply.success {
                                    rf.leader_state.match_index[peer] = index;
                                    rf.leader_state.next_index[peer] = index + 1;
                                } else if reply.term > rf.state.term {
                                    rf.update_state(false, reply.term);
                                    warn!(
                                        "{} got higher term, not a leader now {}",
                                        rf.me, rf.state.is_leader
                                    );
                                } else if rf.leader_state.next_index[peer] > 1 {
                                    rf.leader_state.next_index[peer] -= 1;
                                    let _ = rf.append_entries_to(peer);
                                }
                            }
                        }
                        return Ok(());
                    }
                    raft.lock().unwrap().step();
                    Ok(())
                })
                .wait()
                .unwrap();
        });
        tx
    }
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let raft = Arc::new(Mutex::new(raft));
        let tx = Self::start_raft_thread(raft.clone());
        Node { sender: tx, raft }
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
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(errors::Error::Encode)?;
        let (tx, rx) = oneshot::channel();
        let result = self
            .sender
            .unbounded_send((Args::Cmd(buf), tx))
            .map_err(|e| RpcError::Other(e.to_string()));
        if let Err(e) = result {
            return Err(errors::Error::Rpc(RpcError::Other(e.to_string())));
        }
        if let Reply::Cmd(result) = rx.wait().unwrap() {
            result
        } else {
            unreachable!()
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let State { term, .. } = self.get_state();
        term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let State { is_leader, .. } = self.get_state();
        is_leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        self.raft.lock().unwrap().state.as_ref().clone()
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
        info!("request_vote {:?}", args);
        let (tx, rx) = oneshot::channel();
        let result = self
            .sender
            .unbounded_send((Args::RequestVote(args), tx))
            .map_err(|e| RpcError::Other(e.to_string()));
        if let Err(e) = result {
            return Box::new(future::err(e));
        }
        Box::new(rx.then(|reply| match reply {
            Ok(Reply::RequestVote(reply)) => Ok(reply),
            _ => Err(RpcError::Timeout),
        }))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        {
            let rf = self.raft.lock().unwrap();
            info!(
                "append_entries {}:{} got {}:{}",
                rf.me, rf.state.term, args.leader_id, args.term
            );
        }
        let (tx, rx) = oneshot::channel();
        let result = self
            .sender
            .unbounded_send((Args::AppendEntries(args), tx))
            .map_err(|e| RpcError::Other(e.to_string()));
        if let Err(e) = result {
            return Box::new(future::err(e));
        }
        Box::new(rx.then(|reply| match reply {
            Ok(Reply::AppendEntries(reply)) => Ok(reply),
            _ => Err(RpcError::Timeout),
        }))
    }
}
