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
	"fmt"
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
	HEARTBEAT_TIMEOUT = 100
	ELECTION_TIMEOUT  = 300
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
	VotedFor    int
	Logs        []LogEntry

	// Volatile state on all servers
	CommitIndex int
	LastApplied int

	// Volatile state on leaders
	NextIndex  []int
	MatchIndex []int

	state          int
	applyChannel   chan ApplyMsg
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
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
	// Your code here.
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

// be Follower
func (rf *Raft) beFollower(newTerm int, resetElectionTimer bool) {
	rf.state = Follower
	rf.CurrentTerm = newTerm
	rf.persist()
	if resetElectionTimer {
		if !rf.electionTimer.Stop() {
			<-rf.electionTimer.C
		}
		// ELECTION_TIMEOUT + rand
		nextTimeout := ELECTION_TIMEOUT + rand.Intn(ELECTION_TIMEOUT/4)
		rf.electionTimer.Reset(time.Duration(nextTimeout) * time.Millisecond)
	}
	fmt.Println(rf.me, "become follower at", newTerm)
}

// be Leader
func (rf *Raft) beLeader() {
}

// be Candidate
func (rf *Raft) beCandidate() {
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
	rf.Logs = []LogEntry{{0, nil}} // Logs first index is 1
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.applyChannel = applyCh
	rf.electionTimer = time.NewTimer(ELECTION_TIMEOUT * time.Millisecond)
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
	}
}
