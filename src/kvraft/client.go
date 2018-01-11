package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID int
	seqNo    int
	leader   int
	// map (server ID -> index in the array)
	indexByServer map[int]int
	// Servers we do not know ID yet
	unknownIndex []int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seqNo = 0
	// Currently only a hack, but in real world client id should be easy to get
	// (may not need to go through raft to get client id)
	ck.clientID = int(nrand())
	ck.leader = -1
	ck.indexByServer = make(map[int]int)
	ck.unknownIndex = make([]int, len(servers))
	for idx := 0; idx < len(servers); idx++ {
		ck.unknownIndex[idx] = idx
	}
	return ck
}

func (ck *Clerk) RegisterServer(idx int, server int) {
	// If we do not know this index yet, record it
	for i := 0; i < len(ck.unknownIndex); i++ {
		if idx == ck.unknownIndex[i] {
			ck.indexByServer[server] = idx
			lastIdx := len(ck.unknownIndex) - 1
			DPrintf("idx : %d, last : %d", idx, lastIdx)
			// Remove idx from unknown index
			ck.unknownIndex[i], ck.unknownIndex[lastIdx] =
				ck.unknownIndex[lastIdx], ck.unknownIndex[i]
			ck.unknownIndex = ck.unknownIndex[:lastIdx]
		}
	}

}

const RPC_TIMEOUT = 500 * time.Millisecond

// Add timeout to prevent block
func (ck *Clerk) SendRPC(rpc string, idx int, args interface{}, reply interface{}) bool {
	okCh := make(chan bool, 1)
	go func() {
		ok := ck.servers[idx].Call(rpc, args, reply)
		okCh <- ok
	}()
	go func() {
		time.Sleep(RPC_TIMEOUT)
		okCh <- false
	}()
	return <-okCh
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	seqNo := ck.seqNo
	ck.seqNo++
	args := GetArgs{key, ck.clientID, seqNo}
	for {
		DPrintf("Client %d(%d) : Get(%s)", ck.clientID, ck.seqNo, key)
		reply := GetReply{}
		var ok bool
		var idx int
		if ck.leader == -1 {
			idx = int(nrand()) % len(ck.servers)
		} else {
			DPrintf("Client %d(%d) : Send to server %d", ck.clientID, ck.seqNo, ck.leader)
			var hasKey bool
			idx, hasKey = ck.indexByServer[ck.leader]
			if !hasKey {
				idx = ck.unknownIndex[int(nrand())%len(ck.unknownIndex)]
			}
		}
		ok = ck.SendRPC("RaftKV.Get", idx, &args, &reply)
		if !ok {
			ck.leader = -1
			// DPrintf("Client %d(%d) : Retry", ck.clientID, ck.seqNo)
			continue
		}
		ck.RegisterServer(idx, reply.ServerID)
		if reply.WrongLeader {
			ck.leader = reply.LeaderID
			continue
		}
		if reply.Err == ErrNoKey {
			break
		}
		return reply.Value
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	seqNo := ck.seqNo
	ck.seqNo++
	args := PutAppendArgs{key, value, op, ck.clientID, seqNo}
	for {
		DPrintf("Client %d : %s(%s, %s)", ck.clientID, op, key, value)
		reply := PutAppendReply{}
		var ok bool
		var idx int
		if ck.leader == -1 {
			idx = int(nrand()) % len(ck.servers)
		} else {
			DPrintf("Client %d(%d) : %s(%s, %s)", ck.clientID, ck.seqNo, op, key, value)
			var hasKey bool
			idx, hasKey = ck.indexByServer[ck.leader]
			if !hasKey {
				idx = ck.unknownIndex[int(nrand())%len(ck.unknownIndex)]
			}
		}
		ok = ck.SendRPC("RaftKV.PutAppend", idx, &args, &reply)
		if !ok {
			ck.leader = -1
			continue
		}
		ck.RegisterServer(idx, reply.ServerID)
		if reply.WrongLeader {
			ck.leader = reply.LeaderID
			continue
		}
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
