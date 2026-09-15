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
	//	"bytes"

	"slices"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int // Start from 1
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* Persistent state */
	currentTerm int        // latest term server has seen
	votedFor    int        // candidate server voted for
	logs        []LogEntry // log of entries committed to the log

	/* Volatile state */
	commitIndex int // index of highest log entry known to be committed (start from 0)
	lastApplied int // index of highest log entry known to be applied to the service (start from 0)

	/* Leader state */
	nextIndex  []int // index of the next log entry to send to the peer
	matchIndex []int // index of the highest log entry known to be replicated on peer

	// node status
	tick             func()
	heartbeatTimeout int
	electionTimeout  int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

	state StateType

	needPersist bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == StateLeader
	return term, isleader
}

func (rf *Raft) deferPersist() {
	if rf.needPersist {
		rf.persist()
		rf.needPersist = false
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate asking for vote
	LastLogTerm  int // candidate's last log term
	LastLogIndex int // candidate's last log index
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.deferPersist()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Never grant a vote to a candidate from an older term.
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = args.Term
	}

	myLastLogIndex := len(rf.logs) - 1
	myLastLogTerm := 0
	if myLastLogIndex > 0 {
		myLastLogTerm = rf.logs[myLastLogIndex].Term
	}

	// Make sure it's the only vote in current term, or if got retried, still vote for the previous one
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	isLogsUpToDate := (args.LastLogTerm > myLastLogTerm) || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex)
	if isLogsUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.needPersist = true
		rf.resetElectionTimer()
	}
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.deferPersist()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.resetElectionTimer()
		reply.Term = args.Term
	}

	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	for i, appendingEntry := range args.Entries {
		appendingIndex := args.PrevLogIndex + i + 1

		if appendingIndex < len(rf.logs) {
			existedEntry := rf.logs[appendingIndex]
			if existedEntry.Term != appendingEntry.Term {
				rf.logs = rf.logs[:appendingIndex]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				rf.needPersist = true
				// term not match, truncate the following and append new entries from leader
				break
			}
			// index matched do nothing, move next
		} else {
			// fill in the missed entries
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.needPersist = true
			break
		}
	}
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}

	for i := range rf.commitIndex - rf.lastApplied {
		DPrintf("[AppendEntries]: Applied %d", i)
	}
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.deferPersist()
	term = rf.currentTerm
	isLeader = rf.state == StateLeader
	if !isLeader {
		return index, term, isLeader
	}
	index = len(rf.logs)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.logs = append(rf.logs, LogEntry{Command: command, Term: rf.currentTerm})
	rf.needPersist = true
	rf.broadcastAppendEntries()
	/* if there is only one node, then no bcast works */
	rf.maybeAdvanceCommitIndex()
	return index, term, isLeader
}

/* Deep copy of log slice */
func (rf *Raft) logEntries(from int) []LogEntry {
	return append([]LogEntry{}, rf.logs[from:]...)
}

func (rf *Raft) broadcastAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesToPeer(i)
	}
}

func (rf *Raft) sendAppendEntriesToPeer(id int) {
	for {
		args, ok := rf.prepareAppendEntries(id)
		if !ok {
			return
		}

		var reply AppendEntriesReply

		if !rf.sendAppendEntries(id, args, &reply) {
			return
		}

		retry := rf.handleAppendEntriesReply(id, args, &reply)
		if !retry {
			return
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.deferPersist()
	sentNextIndex := args.PrevLogIndex + 1

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return false
	}

	if args.Term != rf.currentTerm || rf.state != StateLeader || sentNextIndex != rf.nextIndex[id] {
		return false
	}

	if reply.Success {
		rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[id] = rf.matchIndex[id] + 1
		rf.maybeAdvanceCommitIndex()
		return false
	} else {
		rf.nextIndex[id] = max(rf.nextIndex[id]-1, 1)
		return true
	}
}

func (rf *Raft) prepareAppendEntries(id int) (*AppendEntriesArgs, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return nil, false
	}
	prevLogIndex := rf.nextIndex[id] - 1
	prevLogTerm := rf.logs[prevLogIndex].Term
	entries := rf.logEntries(rf.nextIndex[id])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return &args, true
}

func (rf *Raft) maybeAdvanceCommitIndex() {
	indices := append([]int{}, rf.matchIndex...)
	slices.Sort(indices)
	quorumIndex := indices[(len(indices)-1)/2]
	if quorumIndex > rf.commitIndex && rf.logs[quorumIndex].Term == rf.currentTerm {
		rf.commitIndex = quorumIndex
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetRandomizedElectionTimeout() {
	rf.randomizedElectionTimeout = rf.electionTimeout + globalRand.Intn(rf.electionTimeout)
}

// resetElectionTimer records contact that is allowed to postpone a new
// election: a valid AppendEntries RPC or a vote we actually granted.
// rf.mu must be held by the caller.
func (rf *Raft) resetElectionTimer() {
	rf.electionElapsed = 0
}

// resetHeartbeatTimer schedules the next heartbeat interval after a leader
// has sent its current round of heartbeats.
// rf.mu must be held by the caller.
func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatElapsed = 0
}

func (rf *Raft) tickHeartbeat() {
	rf.heartbeatElapsed++

	if rf.heartbeatElapsed >= rf.heartbeatTimeout {
		rf.resetHeartbeatTimer()
		rf.broadcastAppendEntries()
	}
}

func (rf *Raft) tickElection() {

	rf.electionElapsed++
	if rf.electionElapsed >= rf.randomizedElectionTimeout {
		rf.startElection()
		DPrintf("[tickElection] %d become leader", rf.me)
	}
}

func (rf *Raft) startElection() {
	defer rf.deferPersist()
	rf.resetRandomizedElectionTimeout()
	rf.needPersist = true
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = StateCandidate
	rf.resetElectionTimer()
	DPrintf("[startElection] %d become candidate in term %d", rf.me, rf.currentTerm)

	lastIndex := len(rf.logs) - 1
	lastTerm := rf.logs[lastIndex].Term

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  lastTerm,
		LastLogIndex: lastIndex,
	}

	votesGranted := 1

	if len(rf.peers) == 1 {
		rf.becomeLeader()
		return
	}

	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go func(id int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(id, &args, &reply); ok {
				rf.mu.Lock()

				// Judge the reply.Term first, learned the latest info from reply
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				// then check whether the original RPC is expired
				if rf.currentTerm != args.Term || rf.state != StateCandidate {
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {
					votesGranted++
					if votesGranted > len(rf.peers)/2 {
						rf.becomeLeader()
					}
				}
				rf.mu.Unlock()
			}
		}(id)
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = StateLeader
	rf.tick = rf.tickHeartbeat

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		if i == rf.me {
			rf.matchIndex[i] = len(rf.logs) - 1
		} else {
			rf.matchIndex[i] = 0
		}
	}
	rf.heartbeatElapsed = rf.heartbeatTimeout
}

// becomeFollower performs the state transition required when this peer
// discovers a newer term. Callers decide whether the election timer should be
// reset: receiving a RequestVote alone must not reset it; granting that vote
// does.
//
// This mirrors etcd's approach of centralizing role transitions instead of
// updating term, vote, and role independently in each RPC handler.
// rf.mu must be held by the caller.
func (rf *Raft) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.needPersist = true
	}
	rf.state = StateFollower
	rf.tick = rf.tickElection
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		rf.tick()
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		var idxList []int
		var logList []LogEntry
		rf.mu.Lock()
		for i := range rf.commitIndex - rf.lastApplied {
			idx := rf.lastApplied + i + 1
			idxList = append(idxList, idx)
			logList = append(logList, rf.logs[idx])
		}
		rf.mu.Unlock()
		for i, idx := range idxList {
			applyCh <- ApplyMsg {
				CommandValid: true,
				Command: logList[i].Command,
				CommandIndex: idx,
			}
			rf.mu.Lock()
			rf.lastApplied = idx
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{{Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	rf.heartbeatTimeout = 10
	rf.electionTimeout = 20
	rf.heartbeatElapsed = 0
	rf.electionElapsed = 0
	rf.randomizedElectionTimeout = 0
	rf.needPersist = false
	rf.tick = rf.tickElection
	rf.state = StateFollower

	rf.resetElectionTimer()
	rf.resetRandomizedElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier(applyCh)

	return rf
}
