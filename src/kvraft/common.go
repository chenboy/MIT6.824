package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int
	SeqNo    int
}

type PutAppendReply struct {
	WrongLeader bool
	LeaderID    int
	ServerID    int
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int
	SeqNo    int
}

type GetReply struct {
	WrongLeader bool
	LeaderID    int
	ServerID    int
	Err         Err
	Value       string
}
