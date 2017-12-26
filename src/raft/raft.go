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

import "sync"
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"

const HeartBeatInterval = 300 * time.Millisecond

func SetElectionTimeout() time.Duration {
	return (500 + time.Duration(rand.Int63()%200)) * time.Millisecond
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistant states
	currentTerm int
	votedFor    int
	// log

	// Volatile states for all servers
	commitIndex int
	lastApplied int

	// Volatile states for leaders

	// Internal states
	killed         bool
	electTimeout   time.Duration
	lastActiveTime time.Time
	state          string
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
	isleader = rf.state == "Leader"
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RequestVote %d -> %d, t%d", rf.me, args.CandidateId, args.Term)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// TODO: also check if candidate's log is up-to-date
	if args.Term > rf.currentTerm {
		// Reset state to be follower if needed
		rf.currentTerm = args.Term
		rf.lastActiveTime = time.Now()
		rf.state = "Follower"
		rf.votedFor = -1
		go Follower(rf.currentTerm, rf)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("Vote %d -> %d", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
	}

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// TODO other fields
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	DPrintf("Sending AppendEntries %d -> %d", args.LeaderId, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// TODO: Implement actual append logic
	reply.Term = args.Term
	reply.Success = true

	DPrintf("Server %d : receive heartbeat", rf.me)
	rf.lastActiveTime = time.Now()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	if rf.state != "Follower" {
		rf.state = "Follower"
		go Follower(rf.currentTerm, rf)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.killed = true
}

func Follower(term int, rf *Raft) {
	DPrintf("Follower(Server %d, t%d), electTimeout %d", rf.me, term, rf.electTimeout)
	for {
		rf.mu.Lock()
		if rf.killed {
			rf.mu.Unlock()
			break
		}
		if rf.currentTerm != term {
			// Others have turned raft into a new state
			rf.mu.Unlock()
			break
		}
		currTime := time.Now()
		if currTime.After(rf.lastActiveTime.Add(rf.electTimeout)) {
			// Turn to candidate state
			rf.state = "Candidate"
			go Candidate(rf.currentTerm, rf)
			rf.mu.Unlock()
			break
		}
		// sleep until it is possible to turn into candidate
		sleepTime := rf.lastActiveTime.Add(rf.electTimeout + time.Millisecond).Sub(currTime)
		DPrintf("Follower(server %d, t%d) Sleep %d ..", rf.me, rf.currentTerm, sleepTime)
		rf.mu.Unlock()
		time.Sleep(sleepTime)

	}
	DPrintf("Follower(Server %d, t%d) exit", rf.me, term)
}

func Candidate(term int, rf *Raft) {
	DPrintf("Candidate(Server %d, t%d) ", rf.me, term)
	for {
		rf.mu.Lock()
		if rf.killed {
			rf.mu.Unlock()
			break
		}
		if rf.currentTerm != term || rf.state != "Candidate" {
			// Others come in and modify the state
			rf.mu.Unlock()
			break
		}
		term += 1
		rf.currentTerm += 1
		rf.electTimeout = SetElectionTimeout()
		// Vote for self
		DPrintf("Candidate(Server %d, t%d) invoking voting, elect timeout = %d",
			rf.me, term, rf.electTimeout)
		votes := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &RequestVoteArgs{term, rf.me}
				reply := &RequestVoteReply{}
				go func(t int, server int) {
					ok := rf.sendRequestVote(server, args, reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// Term has been refreshed
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.lastActiveTime = time.Now()
						rf.state = "Follower"
						go Follower(rf.currentTerm, rf)
						return
					}
					// Outdated
					if t < rf.currentTerm {
						return
					}
					// Update votes
					if reply.VoteGranted {
						votes += 1
						if votes == len(rf.peers)/2+1 {
							rf.state = "Leader"
							go Leader(t, rf)
						}
					}

				}(term, i)
			}
		}
		sleepTime := rf.electTimeout
		DPrintf("Candidate(server %d, t%d) Sleep %d ..", rf.me, rf.currentTerm, sleepTime)
		rf.mu.Unlock()
		time.Sleep(sleepTime)
	}
	DPrintf("Candidate(Server %d, t%d) exit", rf.me, term)
}

func Leader(term int, rf *Raft) {
	DPrintf("Leader(Server %d, t%d)", rf.me, term)
	for {
		rf.mu.Lock()
		if rf.killed {
			rf.mu.Unlock()
			break
		}
		if term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			// Heartbeats
			if i != rf.me {
				args := &AppendEntriesArgs{term, rf.me}
				reply := &AppendEntriesReply{}
				go func(t int, server int) {
					ok := rf.sendAppendEntries(server, args, reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// Term has been refreshed
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.lastActiveTime = time.Now()
						rf.state = "Follower"
						go Follower(rf.currentTerm, rf)
						return
					}

				}(term, i)
			}
		}
		sleepTime := HeartBeatInterval
		DPrintf("Leader(server %d, t%d) Sleep %d ..", rf.me, rf.currentTerm, sleepTime)
		rf.mu.Unlock()
		time.Sleep(sleepTime)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.killed = false
	//Follower state
	rf.lastActiveTime = time.Now()
	rf.electTimeout = SetElectionTimeout()
	rf.state = "Follower"
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go Follower(rf.currentTerm, rf)
	return rf
}
