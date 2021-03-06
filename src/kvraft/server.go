package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string

	// To detect duplicate
	ClientID int
	SeqNo    int
}

type Notify struct {
	ClientID int
	SeqNo    int
	// If this is Get, set the value
	Value string
	Err   Err
}
type LastCommitEntry struct {
	SeqNo int
	// In case client retry
	Err   Err
	Value string
}

type RaftKV struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// KvStore and LastCommit needs to be saved in a snapshot(together with prevTerm
	// and prevIndex which are needed in raft)
	KvStore map[string]string
	// The last SeqNo a client has committed, since a client will retry
	// indefinitely, we're guaranteed to commit each clients command in SeqNo order
	LastCommit map[int]LastCommitEntry
	PrevTerm   int
	PrevIndex  int

	// Map index -> list of RPC waiting for
	// All RPC resulting in modifying some index of the map must be guaranteed
	// to be added to the map before the main loop handle the commited result for
	// that index
	notifyChans map[int][]chan Notify
	killed      bool
}

// Try to send and register callback, caller must hold kv.mu, return nil if
// current server is not leader, the caller is responsible for closing the
// channel
func (kv *RaftKV) RegisterCallback(op Op) chan Notify {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil
	}
	chans, ok := kv.notifyChans[index]
	if !ok {
		chans = make([]chan Notify, 0)
	}
	// Only the main loop will send to the channel *once* when the index commit
	chans = append(chans, make(chan Notify, 1))
	kv.notifyChans[index] = chans
	return chans[len(chans)-1]
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	reply.ServerID = kv.me
	entry, ok := kv.LastCommit[args.ClientID]
	if ok && entry.SeqNo > args.SeqNo {
		// Outdated request, do not need to do anything, since the client already
		// timeout
		kv.mu.Unlock()
		return
	}
	if ok && entry.SeqNo == args.SeqNo {
		// Caller send duplicate message
		reply.WrongLeader = false
		reply.Err = entry.Err
		reply.Value = entry.Value
		kv.mu.Unlock()
		return
	}
	op := Op{args.Key, "", "Get", args.ClientID, args.SeqNo}
	notifyChan := kv.RegisterCallback(op)
	if notifyChan == nil {
		reply.WrongLeader = true
		reply.LeaderID = kv.rf.GetLeaderId()
		DPrintf("Server %d : Wrong Leader(%d)", kv.me, reply.LeaderID)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	notify := <-notifyChan
	if notify.ClientID != args.ClientID || notify.SeqNo != args.SeqNo {
		// For some reason (maybe leader change), the log has not committed
		reply.WrongLeader = true
		reply.LeaderID = kv.rf.GetLeaderId()
		DPrintf("Server %d : Wrong Leader(%d)", kv.me, reply.LeaderID)
		return
	}

	reply.WrongLeader = false
	reply.Err = notify.Err
	reply.Value = notify.Value
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("Server %d : %s(%s, %s)", kv.me, args.Op, args.Key, args.Value)
	reply.ServerID = kv.me
	entry, ok := kv.LastCommit[args.ClientID]
	if ok && entry.SeqNo > args.SeqNo {
		// Outdated request, do not need to do anything, since the client already
		// timeout
		kv.mu.Unlock()
		return
	}
	if ok && entry.SeqNo == args.SeqNo {
		// Caller send duplicate message
		reply.WrongLeader = false
		reply.Err = entry.Err
		kv.mu.Unlock()
		return
	}
	op := Op{args.Key, args.Value, args.Op, args.ClientID, args.SeqNo}
	notifyChan := kv.RegisterCallback(op)
	if notifyChan == nil {
		reply.WrongLeader = true
		reply.LeaderID = kv.rf.GetLeaderId()
		DPrintf("Server %d : Wrong Leader(%d)", kv.me, reply.LeaderID)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	defer close(notifyChan)
	// DPrintf("Server %d : PutAppend() Wait for reply", kv.me)
	notify := <-notifyChan
	// DPrintf("Server %d : PutAppend() Got reply", kv.me)
	if notify.ClientID != args.ClientID || notify.SeqNo != args.SeqNo {
		// For some reason (maybe leader change), the log has not committed
		reply.WrongLeader = true
		reply.LeaderID = kv.rf.GetLeaderId()
		DPrintf("Server %d : Wrong Leader(%d)", kv.me, reply.LeaderID)
		return
	}

	reply.WrongLeader = false
	reply.Err = notify.Err
	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// Get from ApplyCh, apply to the map notify waiting RPC, etc.
func (kv *RaftKV) MainLoop() {
	for {
		msg := <-kv.applyCh
		op := msg.Command.(Op)
		// Only the mainloop will check last commit so no need to hold lock
		kv.mu.Lock()
		// New op, the state machine should apply
		notify := Notify{op.ClientID, op.SeqNo, "", OK}
		// DPrintf("Server %d : got %s(%d)", kv.me, op.Op, op.SeqNo)
		oldEntry, ok := kv.LastCommit[op.ClientID]
		if !ok || oldEntry.SeqNo == op.SeqNo-1 {
			// The new entry is the one to apply
			var entry LastCommitEntry
			entry.SeqNo = op.SeqNo
			switch op.Op {
			case "Put":
				kv.KvStore[op.Key] = op.Value
				entry.Value = op.Value
				entry.Err = OK
			case "Append":
				value := kv.KvStore[op.Key]
				kv.KvStore[op.Key] = value + op.Value
				entry.Value = value + op.Value
				entry.Err = OK
			case "Get":
				entry.Err = OK
				value, ok := kv.KvStore[op.Key]
				if !ok {
					entry.Err = ErrNoKey
				}
				entry.Value = value
			}
			kv.LastCommit[op.ClientID] = entry
			notify.Value = entry.Value
			notify.Err = entry.Err
		} else if oldEntry.SeqNo == op.SeqNo {
			// Client may resend the same operation because it does not know the previous
			// one succeeded, so we just send previous result back
			notify.Value = oldEntry.Value
			notify.Err = oldEntry.Err
		}
		// Send over channel and then free space
		chans := kv.notifyChans[msg.Index]
		for idx := 0; idx < len(chans); idx++ {
			chans[idx] <- notify
		}
		delete(kv.notifyChans, msg.Index)
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) saveSnapshot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.KvStore)
	e.Encode(kv.LastCommit)
	e.Encode(kv.PrevIndex)
	e.Encode(kv.PrevTerm)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
}

func (kv *RaftKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.KvStore)
	d.Decode(&kv.LastCommit)
	d.Decode(&kv.PrevIndex)
	d.Decode(&kv.PrevTerm)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(LastCommitEntry{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.LastCommit = make(map[int]LastCommitEntry)
	kv.KvStore = make(map[string]string)
	kv.killed = false
	kv.notifyChans = make(map[int][]chan Notify)
	kv.persister = persister
	kv.PrevTerm = 0
	kv.PrevIndex = 0
	kv.readSnapshot(persister.ReadSnapshot())
	go kv.MainLoop()

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	return kv
}
