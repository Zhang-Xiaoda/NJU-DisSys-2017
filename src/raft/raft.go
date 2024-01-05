package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// 枚举状态
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// 参数常量
const (
	HEARTBEAT_TIMEOUT    = 100
	MIN_ELECTION_TIMEOUT = 150
	MAX_ELECTION_TIMEOUT = 300
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	CurrentTerm int
	VotedFor    int // maybe voting for ?
	Logs        []LogEntry

	// Volatile state on all servers
	CommitIndex int
	LastApplied int

	// Volatile state on leaders
	NextIndex  []int
	MatchIndex []int

	state         int
	applyChannel  chan ApplyMsg
	electionTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.CurrentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	// Arguments:
	// 	term candidate’s term
	// 	candidateId candidate requesting vote
	// 	lastLogIndex index of candidate’s last log entry (§5.4)
	// 	lastLogTerm term of candidate’s last log entry (§5.4)
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	// Results:
	// 	term currentTerm, for candidate to update itself
	// 	voteGranted true means candidate received vote
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Arguments:
	// 	term leader’s term
	// 	leaderId so follower can redirect clients
	// 	prevLogIndex index of log entry immediately preceding new ones
	// 	prevLogTerm term of prevLogIndex entry
	// 	entries[] log entries to store (empty for heartbeat; may send more than one for efficiency)
	// 	leaderCommit leader’s commitIndex
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Results:
	// 	term currentTerm, for leader to update itself
	// 	success true if follower contained entry matching
	// 	prevLogIndex and prevLogTerm
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false
	if args.Term > rf.CurrentTerm {
		rf.beFollower(args.Term, true)
	}
	lastTerm := 0
	if len(rf.Logs) > 0 {
		lastTerm = rf.Logs[len(rf.Logs)-1].Term
	}
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && aUpToData(args.LastLogTerm, args.LastLogIndex, lastTerm, len(rf.Logs)) {
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = args.Term
		// fmt.PrintLn(rf.me, "vote for", args.CandidateId)
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		// fmt.PrintLn(rf.me, "reject append entries from", args.LeaderId)
		return
	}
	// TODO 校验leader的日志

	// 维护日志

	rf.beFollower(args.Term, true)
	reply.Success = true
	reply.Term = args.Term
	// fmt.PrintLn(rf.me, "receive append entries from", args.LeaderId)
}

func aUpToData(aTerm, aIndex, bTerm, bIndex int) bool {
	if aTerm != bTerm {
		return aTerm > bTerm
	}
	return aIndex >= bIndex
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// be Follower, update term and reset election timer if neccessary
func (rf *Raft) beFollower(newTerm int, resetElectionTimer bool) {
	rf.mu.Lock()
	rf.state = Follower
	rf.CurrentTerm = newTerm
	rf.VotedFor = -1
	rf.persist()
	if resetElectionTimer {
		rf.resetElectionTimer(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
	}
	// fmt.PrintLn(rf.me, "become follower at", newTerm)
	rf.mu.Unlock()
}

func (rf *Raft) resetElectionTimer(min, max int) {
	// if !rf.electionTimer.Stop() {
	// 	<-rf.electionTimer.C
	// }
	nextTimeout := min + rand.Intn(max-min)
	// fmt.PrintLn(rf.me, "reset election timer to", nextTimeout)
	rf.electionTimer.Reset(time.Duration(nextTimeout) * time.Millisecond)
}

// be Leader
func (rf *Raft) beLeader() {
	rf.mu.Lock()
	// fmt.PrintLn(rf.me, "become leader at", rf.CurrentTerm)
	rf.state = Leader
	rf.VotedFor = -1
	rf.persist()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.NextIndex[i] = len(rf.Logs)
		rf.MatchIndex[i] = 0
	}
	rf.mu.Unlock()
}

// be Candidate, term++, vote for self
func (rf *Raft) beCandidate() {
	rf.mu.Lock()
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.state = Candidate
	rf.persist()
	// fmt.PrintLn(rf.me, "become candidate at", rf.CurrentTerm)
	rf.mu.Unlock()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Logs = []LogEntry{}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.applyChannel = applyCh
	rf.electionTimer = time.NewTimer(MAX_ELECTION_TIMEOUT * time.Millisecond)
	rf.beFollower(0, true)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.tryElection()
	return rf
}

func (rf *Raft) tryElection() {
	// wait for rf.electionTimer
	for {
		<-rf.electionTimer.C
		// fmt.PrintLn(rf.me, "try to become candidate")
		if rf.state == Leader {
			// Leader -> Leader, heart beating
			// fmt.PrintLn(rf.me, "already leader, heart beat")
			rf.heartBeat()
			continue
		}
		if rf.state == Follower {
			// Follower -> Candidate
			rf.beCandidate()
			voteResult := make(chan bool, len(rf.peers))
			voteCount := 0
			voteResult <- true // self vote
			for i := range rf.peers {
				if i == rf.me {
					// don't send request vote to self
					continue
				}
				args := RequestVoteArgs{
					Term:         rf.CurrentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.Logs),
					LastLogTerm:  0,
				}
				if len(rf.Logs) > 0 {
					args.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
				}
				reply := RequestVoteReply{}

				go func(dst int) {
					// fmt.PrintLn(rf.me, "send request vote to", dst)
					if rf.sendRequestVote(dst, args, &reply) {
						// fmt.PrintLn(rf.me, "receive request vote reply from", dst, "term", reply.Term, "vote granted", reply.VoteGranted)
						if reply.Term == rf.CurrentTerm {
							// 防止收到过期的投票
							voteResult <- reply.VoteGranted
						} else if reply.Term > rf.CurrentTerm {
							rf.beFollower(reply.Term, true)
							voteResult <- false
						}
					} else {
						// fmt.PrintLn(rf.me, "send request vote to", dst, "failed")
						// TODO
						voteResult <- false
					}
				}(i)
			}

			time.Sleep(100 * time.Millisecond)
			if rf.state == Candidate {
				// fmt.PrintLn(rf.me, "still candidate, counting votes")
				for range rf.peers {
					select {
					case voteGranted := <-voteResult:
						if voteGranted {
							voteCount++
							// fmt.PrintLn(rf.me, "vote count", voteCount)
						}
					default:
						// fmt.PrintLn(rf.me, "failed to collect vote from some peers, try another round later")
					}
				}
				if voteCount > len(rf.peers)/2 {
					// become leader
					// fmt.PrintLn(rf.me, "should become leader")
					rf.beLeader()
					rf.heartBeat()
				} else {
					// election failed
					// fmt.PrintLn(rf.me, "election failed, try another round later")
					rf.resetElectionTimer(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
					continue
				}

			}
		}
		if rf.state == Candidate {
			// Candidate -> Candidate
			rf.beCandidate()
			voteResult := make(chan bool, len(rf.peers))
			voteCount := 0
			voteResult <- true // self vote
			for i := range rf.peers {
				if i == rf.me {
					// don't send request vote to self
					continue
				}
				args := RequestVoteArgs{
					Term:         rf.CurrentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.Logs),
					LastLogTerm:  0,
				}
				if len(rf.Logs) > 0 {
					args.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
				}
				reply := RequestVoteReply{}

				go func(dst int) {
					// fmt.PrintLn(rf.me, "send request vote to", dst)
					if rf.sendRequestVote(dst, args, &reply) {
						// fmt.PrintLn(rf.me, "receive request vote reply from", dst, "term", reply.Term, "vote granted", reply.VoteGranted)
						if reply.Term == rf.CurrentTerm {
							// 防止收到过期的投票
							voteResult <- reply.VoteGranted
						} else if reply.Term > rf.CurrentTerm {
							rf.beFollower(reply.Term, true)
							voteResult <- false
						}
					} else {
						// fmt.PrintLn(rf.me, "send request vote to", dst, "failed")
						// TODO
						voteResult <- false
					}
				}(i)
			}

			time.Sleep(100 * time.Millisecond)
			if rf.state == Candidate {
				// fmt.PrintLn(rf.me, "still candidate, counting votes")
				for range rf.peers {
					select {
					case voteGranted := <-voteResult:
						if voteGranted {
							voteCount++
							// fmt.PrintLn(rf.me, "vote count", voteCount)
						}
					default:
						// fmt.PrintLn(rf.me, "failed to collect vote from some peers, try another round later")
					}
				}
				if voteCount > len(rf.peers)/2 {
					// become leader
					// fmt.PrintLn(rf.me, "should become leader")
					rf.beLeader()
					rf.heartBeat()
					continue
				} else {
					// election failed
					// fmt.PrintLn(rf.me, "election failed, try another round later")
					rf.resetElectionTimer(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
					continue
				}

			}
		}
	}
}

func (rf *Raft) heartBeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(dst int) {
			// fmt.PrintLn(rf.me, "send heart beat to", dst)
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: len(rf.Logs),
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: rf.CommitIndex,
			}
			reply := AppendEntriesReply{}
			ok := rf.peers[dst].Call("Raft.AppendEntries", args, &reply)
			if ok {
				// fmt.PrintLn(rf.me, "receive heartbeat from", dst)
			} else {
				// fmt.PrintLn(rf.me, "failed to send heartbeat to", dst)
			}
		}(i)
	}
	rf.resetElectionTimer(MIN_ELECTION_TIMEOUT/2, MIN_ELECTION_TIMEOUT)
}
