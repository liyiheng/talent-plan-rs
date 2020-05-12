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

const RPC_TIMEOUT: Duration = Duration::from_millis(50);
const MIN_ELECTION_TIMEOUT: Duration = Duration::from_millis(150);
const INTERVAL_PERIOD: Duration = Duration::from_millis(50);

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

/// PersistentState contains all raft state that
/// need to be persisted
#[derive(Clone, PartialEq, Message)]
struct PersistentState {
    #[prost(uint64, tag = "1")]
    current_term: u64,
    #[prost(uint64, optional, tag = "2")]
    voted_for: Option<u64>,
    #[prost(message, repeated, tag = "3")]
    log: Vec<LogEntry>,
    /// Last included index of snapshot
    #[prost(uint64, tag = "4")]
    last_included_index: u64,
    /// Last included term of snapshot
    #[prost(uint64, tag = "5")]
    last_included_term: u64,
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
    apply_ch: UnboundedSender<ApplyMsg>,
    event_ch: Option<UnboundedSender<Event>>,
    commit_index: usize,
    last_applied: usize,
    timeout_at: Instant,
    // Record last heartbeat instant, to skip unnecessary heartbeats
    last_hb: Instant,
    persistent_state: PersistentState,
    persist_dat_size: usize,
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
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            apply_ch,
            event_ch: None,
            commit_index: 0,
            last_applied: 0,
            timeout_at: Instant::now(),
            last_hb: Instant::now(),
            persistent_state: PersistentState::default(),
            persist_dat_size: 0,
            leader_state: LeaderState::default(),
        };
        // initialize from state persisted before a crash
        rf.restore();
        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        let mut data = vec![];
        labcodec::encode(&self.persistent_state, &mut data).unwrap();
        self.persist_dat_size = data.len();
        self.persister.save_raft_state(data);
    }

    /// save persistent state and snapshot
    fn persist_all(&mut self, snapshot: Vec<u8>) {
        let mut data = vec![];
        labcodec::encode(&self.persistent_state, &mut data).unwrap();
        self.persist_dat_size = data.len();
        self.persister.save_state_and_snapshot(data, snapshot);
    }

    /// restore previously persisted state.
    fn restore(&mut self) {
        let data = self.persister.raft_state();
        if !data.is_empty() {
            let persistent_state = labcodec::decode::<PersistentState>(&data).unwrap();
            self.persistent_state = persistent_state;
        }
        self.persist_dat_size = data.len();
        let last_i = self.persistent_state.last_included_index as usize;

        // Snapshot was applied by kvraft
        self.last_applied = self.last_applied.max(last_i);
        self.commit_index = self.commit_index.max(last_i);
        self.state = Arc::new(State {
            is_leader: false,
            term: self.persistent_state.current_term,
        });
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
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if !self.state.is_leader() {
            return Err(Error::NotLeader);
        }
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        let entry = LogEntry {
            data: buf,
            term: self.state.term,
        };
        self.persistent_state.log.push(entry);
        self.persist();
        self.send_heartbeat();
        let (index, term) = (self.last_log_index(), self.state.term);
        Ok((index as u64, term))
    }

    fn snapshot(&mut self, data: Vec<u8>, last_applied: u64) -> Result<()> {
        if !self.state.is_leader() {
            return Err(Error::NotLeader);
        }
        let last_t = self.get_log(last_applied as usize).unwrap().term;
        let prev_last_included = self.persistent_state.last_included_index;
        self.persistent_state.last_included_term = last_t;
        self.persistent_state.last_included_index = last_applied;

        let discard_pos = (last_applied - prev_last_included) as usize;
        if self.persistent_state.log.len() < discard_pos {
            self.persistent_state.log.clear();
        } else {
            self.persistent_state.log.drain(0..discard_pos);
        }

        self.persist_all(data);
        for i in 0..self.peers.len() {
            self.install_snapshot_to(i);
        }
        Ok(())
    }

    fn send_heartbeat(&mut self) {
        for i in 0..self.peers.len() {
            self.append_entries_to(i);
        }
        self.last_hb = Instant::now();
    }
}

impl Raft {
    fn reset_timer(&mut self) {
        // Since the tester limits the frequency of RPC calls,
        // election timeout is larger than 150ms-300ms in section5.2,
        let p = MIN_ELECTION_TIMEOUT.as_millis() as u64;
        let millis = rand::thread_rng().gen_range(p, p * 2);
        let dur = Duration::from_millis(millis);
        self.timeout_at = Instant::now() + dur;
        if self.event_ch.is_none() {
            return;
        }
        let event_ch = self.event_ch.clone().unwrap();
        let fut = Delay::new_at(self.timeout_at).then(move |_| {
            let _ = event_ch.unbounded_send(Event::StartElection);
            Ok(())
        });
        self.peers[self.me].spawn(fut)
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::Shutdown => {}
            Event::StartElection => {
                if self.timeout_at > Instant::now() {
                    // Drop this event
                    return;
                }
                if self.state.is_leader {
                    return;
                }
                self.reset_timer();
                info!("{} start election", self.me);
                self.start_election();
            }
            Event::VoteResult(term, cnt) => {
                self.handle_vote_result(term, cnt);
            }
            Event::RequestVote(args, tx) => {
                let resp = self.handle_vote_request(args);
                let _ = tx.send(Reply::RequestVote(resp));
            }
            Event::AppendEntries(args, tx) => {
                let resp = self.handle_append_entries(args);
                let _ = tx.send(Reply::AppendEntries(resp));
            }
            Event::InstallSnapshotResult(peer, index, reply) => {
                if reply.term > self.state.term {
                    self.update_state(false, reply.term);
                    self.persist();
                } else {
                    if self.leader_state.match_index[peer] < index {
                        self.leader_state.match_index[peer] = index;
                    }
                    if self.leader_state.next_index[peer] < index + 1 {
                        self.leader_state.next_index[peer] = index + 1;
                    }
                }
            }
            Event::InstallSnapshot(args, sender) => {
                self.handle_install_snapshot(args, sender);
            }
            Event::AppendEntriesResult(peer, index, reply) => {
                if reply.success {
                    self.leader_state.match_index[peer] = index;
                    self.leader_state.next_index[peer] = index + 1;
                } else if reply.term > self.state.term {
                    self.update_state(false, reply.term);
                    self.persist();
                    info!(
                        "{} got higher term, not a leader now {}",
                        self.me, self.state.is_leader
                    );
                } else if self.state.is_leader {
                    let mut last_i = reply.last_index_with_term as usize;
                    if last_i == 0 {
                        last_i = 1;
                    }
                    let old = self.leader_state.next_index[peer];
                    self.leader_state.next_index[peer] = last_i;
                    let ni = self.leader_state.next_index[peer];
                    info!("Decrease next_index of {}: {}=>{}", peer, old, ni);
                    self.append_entries_to(peer);
                }
            }
        }
    }

    fn handle_append_entries(&mut self, mut args: AppendEntriesArgs) -> AppendEntriesReply {
        let current_term = self.state.term();
        let mut resp = AppendEntriesReply {
            term: current_term,
            success: false,
            last_index_with_term: 0,
        };
        // 1. Reply false if term < currentTerm (§5.1)
        if args.term < current_term {
            warn!(
                "{} refused {}, term {} < {}",
                self.me, args.leader_id, args.term, current_term
            );
            return resp;
        }
        // 2. Reply false if log doesn’t contain an entry at prevLogIndex
        // whose term matches prevLogTerm (§5.3)
        let prev_i = args.prev_log_index as usize;
        if prev_i > 0 {
            let prev_t = args.prev_log_term;
            let t = if prev_i == self.persistent_state.last_included_index as usize {
                Some(self.persistent_state.last_included_term)
            } else {
                self.get_log(prev_i).map(|l| l.term)
            };
            if t.is_none() || t.unwrap() != prev_t {
                warn!(
                    "{} refused {}, doesn’t contain an entry at prev_log_index",
                    self.me, args.leader_id
                );
                if t.is_none() {
                    resp.last_index_with_term = self.last_log_index() as u64;
                } else {
                    let mut last_i_with_t = 0;
                    for (i, l) in self.persistent_state.log.iter().enumerate().rev() {
                        if l.term == prev_t {
                            last_i_with_t = i as u64 + 1;
                            break;
                        }
                    }
                    resp.last_index_with_term = last_i_with_t;
                }
                return resp;
            }
        }

        // 3. If an existing entry conflicts with a new one (same index
        // but different terms), delete the existing entry and all that
        // follow it (§5.3)
        // 4. Append any new entries not already in the log
        if prev_i >= self.persistent_state.last_included_index as usize {
            let start_pos = prev_i - self.persistent_state.last_included_index as usize;
            self.persistent_state.log.drain(start_pos..);
        } else {
            // Handle case: earlier AppendEntries RPC comes later than InstallSnapshot RPC
            self.persistent_state.log.clear();
            let extra = self.persistent_state.last_included_index as usize - prev_i;
            let tail = args.entries.len().min(extra);
            args.entries.drain(..tail);
        }
        self.persistent_state
            .log
            .extend(args.entries.iter().cloned());

        // 5. If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)
        if args.leader_commit > self.commit_index as u64 {
            let i = self.last_log_index();
            self.commit_index = i.min(args.leader_commit as usize);
        }
        self.update_state(false, args.term);
        self.persistent_state.voted_for = None;
        resp.success = true;
        self.reset_timer();
        self.persist();
        resp
    }

    fn update_state(&mut self, is_leader: bool, term: u64) {
        self.state = Arc::new(State { is_leader, term });
        self.persistent_state.current_term = term;
    }

    // index starts from 1
    fn get_log(&self, index: usize) -> Option<&LogEntry> {
        // index starts with 1
        if index == 0 {
            return None;
        }
        let last_included = self.persistent_state.last_included_index;
        // the log was compacted in snapshot
        if index as u64 <= last_included {
            return None;
        }
        let vec_index = index - last_included as usize - 1;
        self.persistent_state.log.get(vec_index)
    }

    fn handle_vote_request(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let current_term = self.state.term();
        let mut resp = RequestVoteReply {
            term: current_term,
            vote_granted: false,
        };
        if current_term > args.term {
            info!(
                "{} refused {}, term: {}>{}",
                self.me, args.candidate_id, current_term, args.term
            );
            return resp;
        }

        let voted_for = self.persistent_state.voted_for;
        if current_term == args.term && voted_for.is_some() && voted_for != Some(args.candidate_id)
        {
            info!(
                "{} refused {}, voted for {:?}",
                self.me, args.candidate_id, voted_for
            );
            return resp;
        }
        let last_log_index = self.last_log_index() as u64;
        let last_log_term = self.last_log_term();
        // §5.4.1  Raft determines which of two logs is more up-to-date
        // by comparing the index and term of the last entries in the
        // logs. If the logs have last entries with different terms, then
        // the log with the later term is more up-to-date. If the logs
        // end with the same term, then whichever log is longer is
        // more up-to-date.
        let is_up_to_date = if args.last_log_term > last_log_term {
            true
        } else if args.last_log_term == last_log_term {
            args.last_log_index >= last_log_index
        } else {
            false
        };
        if !is_up_to_date {
            info!(
                "{} refused {}, log not up-to-date, last_i:{},last_t:{}, arg.last_i:{}, arg.last_t:{}",
                self.me, args.candidate_id, last_log_index, last_log_term, args.last_log_index, args.last_log_term
            );
            return resp;
        }
        info!(
            "[handle_vote_request] {} voted for {}",
            self.me, args.candidate_id,
        );
        self.reset_timer();
        self.persistent_state.voted_for = Some(args.candidate_id);
        self.update_state(false, args.term);
        self.persist();
        resp.vote_granted = true;
        resp
    }

    fn last_log_index(&self) -> usize {
        self.persistent_state.last_included_index as usize + self.persistent_state.log.len()
    }
    fn last_log_term(&self) -> u64 {
        self.persistent_state
            .log
            .last()
            .map(|l| l.term)
            .unwrap_or(self.persistent_state.last_included_term)
    }

    fn start_election(&mut self) {
        let term = self.state.term + 1;
        self.update_state(false, term);
        self.persistent_state.voted_for = Some(self.me as u64);
        self.persist();
        let last_term = self.last_log_term();
        let args = RequestVoteArgs {
            candidate_id: self.me as u64,
            last_log_index: self.last_log_index() as u64,
            last_log_term: last_term,
            term: self.state.term,
        };
        let mut receivers = Vec::with_capacity(self.peers.len());
        for i in 0..self.peers.len() {
            let rx = self.send_request_vote(i, &args).map_err(|_| ());
            let rx = rx.select(
                Delay::new(RPC_TIMEOUT)
                    .map_err(|_| ())
                    .map(|_| Err(Error::Rpc(RpcError::Timeout))),
            );
            receivers.push(rx);
        }
        let rx = futures::stream::futures_unordered(receivers);
        let event_sender = self.event_ch.clone().unwrap();
        let me = self.me;
        let fut = rx
            .fold(vec![], move |mut acc, (v, _)| {
                acc.push(v);
                future::ok(acc)
            })
            .map_err(|_| RpcError::Timeout)
            .then(move |result| match result {
                Ok(v) => {
                    let mut votes = 0;
                    let mut max_term = term;
                    for e in v {
                        info!("Vote result of {}: {:?}", me, e);
                        if let Ok(e) = e {
                            if e.vote_granted {
                                votes += 1;
                            } else if e.term > term {
                                max_term = e.term;
                                break;
                            }
                        }
                    }
                    let _ = event_sender.unbounded_send(Event::VoteResult(max_term, votes));
                    Ok(())
                }
                Err(_) => Ok(()),
            });
        self.peers[self.me].spawn(fut);
    }

    fn handle_install_snapshot(
        &mut self,
        args: InstallSnapshotArgs,
        sender: oneshot::Sender<InstallSnapshotReply>,
    ) {
        let cur_term = self.state.term;
        let reply = InstallSnapshotReply { term: cur_term };

        // Reply immediately if term < currentTerm
        if cur_term > args.term {
            let _ = sender.send(reply);
            return;
        }
        self.update_state(false, args.term);

        // It's unnecessary to install it if local snapshot is newer
        if self.persistent_state.last_included_index >= args.last_included_index {
            let _ = sender.send(reply);
            return;
        }
        // Discard all logs before 'last_included_index + 1'
        // head is vec-index of the first log left
        let old_include = self.persistent_state.last_included_index;
        let head = (args.last_included_index - old_include) as usize;
        if head > self.persistent_state.log.len() {
            self.persistent_state.log.clear();
        } else {
            self.persistent_state.log.drain(0..head as usize);
        }

        self.persistent_state.last_included_index = args.last_included_index;
        self.persistent_state.last_included_term = args.last_included_term;
        self.persist_all(args.data.clone());
        let last_i = args.last_included_index as usize;
        if self.last_applied < last_i {
            self.apply_ch
                .unbounded_send(ApplyMsg {
                    command_valid: false,
                    command: args.data,
                    command_index: args.leader_id, // command_index is meaningless here, use it for debuging
                })
                .unwrap();
        }
        self.commit_index = last_i.max(self.commit_index);
        self.last_applied = last_i.max(self.last_applied);
        let _ = sender.send(reply);
    }

    fn install_snapshot_to(&self, i: usize) {
        if i == self.me {
            return;
        }
        let peer = &self.peers[i];
        let event_sender = self.event_ch.clone().unwrap();
        let snapshot_args = InstallSnapshotArgs {
            leader_id: self.me as u64,
            last_included_index: self.persistent_state.last_included_index,
            last_included_term: self.persistent_state.last_included_term,
            term: self.state.term,
            data: self.persister.snapshot(),
        };
        let last_included_index = snapshot_args.last_included_index as usize;
        let me = self.me;
        peer.spawn({
            peer.install_snapshot(&snapshot_args)
                .map_err(Error::Rpc)
                .then(move |reply| {
                    if let Ok(reply) = reply {
                        let _ = event_sender.unbounded_send(Event::InstallSnapshotResult(
                            i,
                            last_included_index,
                            reply,
                        ));
                    } else {
                        // Resend needed?
                        info!("{} failed to install_snapshot_to {}", me, i);
                    }
                    Ok(())
                })
        });
    }

    fn append_entries_to(&mut self, i: usize) {
        let index = self.last_log_index() as usize;
        let event_sender = self.event_ch.clone().unwrap();
        if i == self.me {
            self.reset_timer();
            let reply = AppendEntriesReply {
                success: true,
                term: self.state.term,
                last_index_with_term: self.last_log_index() as u64 + 1,
            };
            let _ = event_sender.unbounded_send(Event::AppendEntriesResult(i, index, reply));
            return;
        }
        let peer = &self.peers[i];
        let i_next = self.leader_state.next_index[i];
        let next = self.last_log_index() as usize + 1;
        // If i_next < first_index, install_snapshot
        let last_included_index = self.persistent_state.last_included_index as usize;
        if i_next != next && i_next <= last_included_index && last_included_index > 0 {
            self.install_snapshot_to(i);
            return;
        }
        let entries = if i_next == next {
            vec![]
        } else {
            let mut entries = vec![];
            let last_i_i = self.persistent_state.last_included_index as usize;
            let src = &self.persistent_state.log[i_next - last_i_i - 1..];
            entries.extend(src.iter().cloned());
            entries
        };
        let prev_term = if i_next as u64 - 1 == self.persistent_state.last_included_index {
            self.persistent_state.last_included_term
        } else {
            self.get_log(i_next - 1).map(|l| l.term).unwrap_or(0)
        };
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
                    if let Ok(reply) = reply {
                        let _ = event_sender.unbounded_send(Event::AppendEntriesResult(
                            i,
                            index,
                            reply.clone(),
                        ));
                    }
                    Ok(())
                })
        });
    }

    fn handle_vote_result(&mut self, term: u64, cnt: usize) {
        if term < self.state.term {
            return;
        }
        if self.persistent_state.voted_for != Some(self.me as u64) {
            // It's a follower now
            return;
        }
        let is_leader = cnt > self.peers.len() / 2;
        self.update_state(is_leader, term);
        self.persistent_state.voted_for = None;
        // if is_leader && self.last_log_index() > 0 {
        //     self.persistent_state
        //         .log
        //         .push(LogEntry { term, data: vec![] });
        // }
        self.persist();
        if is_leader {
            let log_size = self.last_log_index();
            self.leader_state = LeaderState {
                next_index: vec![log_size + 1; self.peers.len()],
                match_index: vec![0; self.peers.len()],
            };
            self.send_heartbeat();
        }
    }

    fn try_commit(&mut self) {
        while self.commit_index > self.last_applied {
            let t = self
                .get_log(self.last_applied + 1)
                .map(|l| l.term)
                .unwrap_or_default();
            info!(
                "apply log, t:{}, i:{}, peer {}, is_leader:{}, last_i:{},last_i_i:{}",
                t,
                self.last_applied + 1,
                self.me,
                self.state.is_leader,
                self.last_log_index(),
                self.persistent_state.last_included_index,
            );
            let log = self.get_log(self.last_applied + 1);
            let _ = self.apply_ch.unbounded_send(ApplyMsg {
                command_valid: true,
                command_index: self.last_applied as u64 + 1,
                command: log.unwrap().data.clone(),
            });
            self.last_applied += 1;
        }
    }

    fn step(&mut self) {
        if !self.state.is_leader() {
            return;
        }
        // If there exists an N such that N > commitIndex, a majority
        // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4).
        let prev_commit_i = self.commit_index;
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
                if c <= majority {
                    continue;
                }
                let log_term = self.get_log(n).map(|l| l.term).unwrap_or_default();
                if log_term == self.state.term {
                    self.commit_index = n;
                    break;
                }
            }
        }
        if self.last_hb.elapsed() >= INTERVAL_PERIOD || prev_commit_i != self.commit_index {
            self.send_heartbeat();
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
    // Use sender to communicate with raft thread,
    // sometimes it's convenient to use Mutex directly
    sender: UnboundedSender<Event>,
    raft: Arc<Mutex<Raft>>,
}

#[derive(Debug)]
enum Event {
    RequestVote(RequestVoteArgs, oneshot::Sender<Reply>),
    AppendEntries(AppendEntriesArgs, oneshot::Sender<Reply>),
    InstallSnapshot(InstallSnapshotArgs, oneshot::Sender<InstallSnapshotReply>),
    VoteResult(u64, usize),
    // peer_id, index, reply
    AppendEntriesResult(usize, usize, AppendEntriesReply),
    // peer_id, index, reply
    InstallSnapshotResult(usize, usize, InstallSnapshotReply),
    Shutdown,
    StartElection,
}

#[derive(Clone, Debug)]
enum Reply {
    RequestVote(RequestVoteReply),
    AppendEntries(AppendEntriesReply),
}

impl Node {
    /// Get size of persistent logs in bytes
    pub fn get_persist_size(&self) -> usize {
        self.raft.lock().unwrap().persist_dat_size
    }

    fn start_raft_thread(raft: Arc<Mutex<Raft>>) -> UnboundedSender<Event> {
        let (event_tx, event_rx) = futures::sync::mpsc::unbounded();
        raft.lock().unwrap().event_ch = Some(event_tx.clone());
        raft.lock().unwrap().reset_timer();
        std::thread::spawn(move || {
            let event_rx = event_rx.map_err(|_| ()).map(Some);
            Interval::new(INTERVAL_PERIOD)
                .map(|_| None)
                .map_err(|_| ())
                .select(event_rx)
                .take_while(|event| {
                    let has_next = if let Some(Event::Shutdown) = event {
                        info!("Peer {} shutdown", raft.lock().unwrap().me);
                        raft.lock().unwrap().persist();
                        false
                    } else {
                        true
                    };
                    future::ok(has_next)
                })
                .for_each(|event| {
                    let mut rf = raft.lock().unwrap();
                    if let Some(event) = event {
                        rf.handle_event(event);
                    } else {
                        rf.step();
                        info!(
                            "Peer {}, is_leader:{}, last_log_index:{}",
                            rf.me,
                            rf.state.is_leader,
                            rf.last_log_index()
                        );
                    }
                    rf.try_commit();
                    Ok(())
                })
                .wait()
                .unwrap();
        });
        event_tx
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
        let x = self.raft.lock().unwrap().start(command);
        info!("Start: {:?}, result:{:?}", command, x);
        x
    }

    /// Create a snapshot and clear all logs in persistent_state
    /// cmds are current states of the server.
    pub fn snapshot(&self, data: Vec<u8>, last_applied: u64) -> Result<()> {
        self.raft.lock().unwrap().snapshot(data, last_applied)
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
        let _ = self.sender.unbounded_send(Event::Shutdown);
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        let (tx, rx) = oneshot::channel();
        let result = self
            .sender
            .unbounded_send(Event::RequestVote(args, tx))
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
        let (tx, rx) = oneshot::channel();
        let result = self
            .sender
            .unbounded_send(Event::AppendEntries(args, tx))
            .map_err(|e| RpcError::Other(e.to_string()));
        if let Err(e) = result {
            return Box::new(future::err(e));
        }
        Box::new(rx.then(|reply| match reply {
            Ok(Reply::AppendEntries(reply)) => Ok(reply),
            _ => Err(RpcError::Timeout),
        }))
    }

    /// Install a whole snapshot once,
    /// so we don't need 'byte' and 'done' in args
    fn install_snapshot(&self, args: InstallSnapshotArgs) -> RpcFuture<InstallSnapshotReply> {
        let (tx, rx) = oneshot::channel();
        let result = self
            .sender
            .unbounded_send(Event::InstallSnapshot(args, tx))
            .map_err(|e| RpcError::Other(e.to_string()));
        if let Err(e) = result {
            return Box::new(future::err(e));
        }
        Box::new(rx.then(|reply| match reply {
            Ok(reply) => Ok(reply),
            _ => Err(RpcError::Timeout),
        }))
    }
}
