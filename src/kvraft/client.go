package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID int
	seqNo    int
	leader   int
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
	return ck
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
		DPrintf("Client %d : Get(%s)", ck.clientID, key)
		reply := GetReply{}
		var ok bool
		if ck.leader == -1 {
			ok = ck.servers[int(nrand())%len(ck.servers)].Call(
				"RaftKV.Get", &args, &reply)
		} else {
			ok = ck.servers[ck.leader].Call("RaftKV.Get", &args, &reply)
		}
		if !ok {
			continue
		}
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
		if ck.leader == -1 {
			ok = ck.servers[int(nrand())%len(ck.servers)].Call(
				"RaftKV.PutAppend", &args, &reply)
		} else {
			ok = ck.servers[ck.leader].Call("RaftKV.PutAppend", &args, &reply)
		}
		if !ok {
			continue
		}
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
