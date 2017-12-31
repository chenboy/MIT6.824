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

import "sort"
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

type LogEntry struct {
	Command interface{}
	Term    int
}

type notifyEntry struct {
	peer int
	term int
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
	log         []LogEntry

	// Volatile states for all servers
	commitIndex int
	lastApplied int

	// Volatile states for leaders
	nextIndex  []int
	matchIndex []int

	// Internal states
	killed         bool
	electTimeout   time.Duration
	lastActiveTime time.Time
	state          string
	// Channel do not need to be protected by the mutex
	// Peer no to send for the leader
	notifyChan chan notifyEntry
	applyCh    chan ApplyMsg
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	// Receive updated term
	if args.Term > rf.currentTerm {
		// Reset state to be follower if needed
		rf.currentTerm = args.Term
		rf.lastActiveTime = time.Now()
		rf.state = "Follower"
		rf.votedFor = -1
		go Follower(rf.currentTerm, rf)
	}
	// Check not voted
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		// Check log up-to-date
		(rf.log[len(rf.log)-1].Term < args.LastLogTerm ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm &&
				len(rf.log)-1 <= args.LastLogIndex)) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("Vote %d -> %d, t%d", rf.me, args.CandidateId, args.Term)
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	// Empty if heartbeat
	Entry        []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// DPrintf("Sending AppendEntries %d -> %d", args.LeaderId, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Reject not up-to-date leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastActiveTime = time.Now()
	// Current log doesn't match leader's log, roll back and retry
	// DPrintf("args.PrevLogIndex : %d", args.PrevLogIndex)
	if args.Entry != nil && args.PrevLogIndex >= 0 &&
		(len(rf.log) <= args.PrevLogIndex ||
			rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = args.Term
	reply.Success = true

	// DPrintf("Server %d : receive heartbeat", rf.me)
	// Set state
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	if rf.state != "Follower" {
		rf.state = "Follower"
		go Follower(rf.currentTerm, rf)
	}

	DPrintf("Server %d : AppendEntries (%d -> %d) t%d",
		rf.me, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entry), rf.currentTerm)
	// Apply log if any
	for logIdx := args.PrevLogIndex + 1; logIdx-args.PrevLogIndex <= len(args.Entry); logIdx++ {
		if !(logIdx < len(rf.log)) ||
			!(rf.log[logIdx].Term == args.Entry[logIdx-args.PrevLogIndex-1].Term) {
			// Remove all log entries from the point that doesn't match
			if logIdx < len(rf.log) {
				rf.log = rf.log[:logIdx]
			}
			rf.log = append(rf.log, args.Entry[logIdx-args.PrevLogIndex-1])
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		prevIndex := rf.commitIndex + 1
		currIndex := args.LeaderCommit
		rf.commitIndex = args.LeaderCommit
		go func() {
			for idx := prevIndex; idx <= currIndex; idx++ {
				// We are safe to use rf.log because committed logs will not be changed
				rf.applyCh <- ApplyMsg{Index: idx,
					Command: rf.log[idx].Command}
			}
		}()

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == "Leader"
	term := rf.currentTerm
	index := len(rf.log)
	if !isLeader {
		return index, term, isLeader
	}
	DPrintf("Server %d : Start to commit command at index %d for t%d",
		rf.me, term, index)
	rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
	for i := 0; i < len(rf.peers); i++ {
		go func(idx int) {
			rf.notifyChan <- notifyEntry{idx, term}
		}(i)
	}
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
	// close(rf.notifyChan)
}

func Follower(term int, rf *Raft) {
	DPrintf("Follower(Server %d, t%d)", rf.me, term)
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
		// DPrintf("Follower(server %d, t%d) Sleep %d ..", rf.me, rf.currentTerm, sleepTime)
		rf.mu.Unlock()
		time.Sleep(sleepTime)

	}
	// DPrintf("Follower(Server %d, t%d) exit", rf.me, term)
}

func Candidate(term int, rf *Raft) {
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
		DPrintf("Candidate(Server %d, t%d) ", rf.me, term)
		// Vote for self
		// DPrintf("Candidate(Server %d, t%d) invoking voting, elect timeout = %d",
		//   rf.me, term, rf.electTimeout)
		votes := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				Term := rf.log[len(rf.log)-1].Term
				args := &RequestVoteArgs{term, rf.me, len(rf.log) - 1, Term}
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
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = len(rf.log)
							}
							go LeaderHandler(t, rf)
							go Leader(t, rf)
						}
					}

				}(term, i)
			}
		}
		sleepTime := rf.electTimeout
		// DPrintf("Candidate(server %d, t%d) Sleep %d ..", rf.me, rf.currentTerm, sleepTime)
		rf.mu.Unlock()
		time.Sleep(sleepTime)
	}
	// DPrintf("Candidate(Server %d, t%d) exit", rf.me, term)
}

// Sends heartbeats periodically
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
				args := &AppendEntriesArgs{term, rf.me, 0, 0, make([]LogEntry, 0),
					rf.commitIndex}
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
		// DPrintf("Leader(server %d, t%d) Sleep %d ..", rf.me, rf.currentTerm, sleepTime)
		rf.mu.Unlock()
		time.Sleep(sleepTime)
	}
}

// Leaders notify followers to do update
func LeaderHandler(term int, rf *Raft) {
	for {
		notify, ok := <-rf.notifyChan
		if !ok {
			return
		}
		DPrintf("Server %d : receive notification peer : %d term : %d",
			rf.me, notify.peer, notify.term)
		rf.mu.Lock()
		if rf.killed {
			rf.mu.Unlock()
			return
		}
		// Outdated msg
		if notify.term != rf.currentTerm {
			DPrintf("outdated msg")
			rf.mu.Unlock()
			continue
		}
		// Outdated leader
		if term != rf.currentTerm {
			go func() {
				rf.notifyChan <- notify
			}()
			rf.mu.Unlock()
			return
		}
		// Prepare for applying the corresponding log entry
		index := rf.nextIndex[notify.peer]
		if index < len(rf.log) {
			DPrintf("Prepare to sync log entries (%d -> %d) Server (%d -> %d, in t%d)",
				index, len(rf.log), rf.me, notify.peer, term)
			prevTerm := 0
			if index > 0 {
				prevTerm = rf.log[index-1].Term
			}
			entries := rf.log[index:]
			args := &AppendEntriesArgs{term, rf.me, index - 1, prevTerm,
				entries, rf.commitIndex}
			// Try send and get back msg
			go appendEntryReplyHandler(rf, term, args, notify)
		}
		rf.mu.Unlock()
	}
}

func appendEntryReplyHandler(rf *Raft, term int, args *AppendEntriesArgs, notify notifyEntry) {
	DPrintf("Sync : Server %d -> %d, t%d", rf.me, notify.peer, term)
	reply := &AppendEntriesReply{}
	if rf.me != notify.peer {
		ok := rf.sendAppendEntries(notify.peer, args, reply)
		if !ok {
			go func() {
				DPrintf("Failed to sync, try again for peer %d in t%d",
					notify.peer, notify.term)
				rf.notifyChan <- notify
			}()
			return
		}
	} else {
		reply.Success = true
		reply.Term = term
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Outdated
	if term < reply.Term {
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.lastActiveTime = time.Now()
			rf.state = "Follower"
			go Follower(rf.currentTerm, rf)
		}
		return
	}
	// Not success due to conflict
	if !reply.Success {
		// decr index and retry
		rf.nextIndex[notify.peer]--
		go func() {
			rf.notifyChan <- notify
		}()
		return
	}
	// Update nextIndex, matchIndex and commitIndex
	if rf.nextIndex[notify.peer] < args.PrevLogIndex+len(args.Entry)+1 {
		rf.nextIndex[notify.peer] = args.PrevLogIndex + len(args.Entry) + 1
		rf.matchIndex[notify.peer] = rf.nextIndex[notify.peer]
		// Get the lowest idx that gets majority vote
		sortedMatchIndex := make([]int, len(rf.matchIndex))
		// DPrintf("matchIndex len %d", len(rf.matchIndex))
		copy(sortedMatchIndex, rf.matchIndex)
		sort.Sort(sort.Reverse(sort.IntSlice(sortedMatchIndex)))
		// Start a new go routine to commit for current term
		if sortedMatchIndex[len(sortedMatchIndex)/2] > 0 &&
			rf.log[sortedMatchIndex[len(sortedMatchIndex)/2]-1].Term == term {
			prevIndex := rf.commitIndex + 1
			currIndex := sortedMatchIndex[len(sortedMatchIndex)/2] - 1
			rf.commitIndex = currIndex
			go func() {
				for idx := prevIndex; idx <= currIndex; idx++ {
					DPrintf("Before send : idx %d", idx)
					rf.applyCh <- ApplyMsg{Index: idx,
						Command: rf.log[idx].Command}
				}
			}()
		}
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
	// Persistent
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	// volatile
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.notifyChan = make(chan notifyEntry, len(rf.peers))

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
