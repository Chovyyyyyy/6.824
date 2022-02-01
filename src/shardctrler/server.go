package shardctrler

import "6.824-golabs-2021/raft"
import "6.824-golabs-2021/labrpc"
import "sync"
import "6.824-golabs-2021/labgob"


const (
	CONSENSUS_TIMEOUT = 5000 // ms

	QueryOp = "query"
	JoinOp = "join"
	LeaveOp = "leave"
	MoveOp = "move"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs       []Config // indexed by config num
	waitApplyCh   map[int]chan Op
	lastRequestId map[int64]int

	lastSnapShotRaftLogIndex int
}

type Op struct {
	// Your data here.
	Operation   string
	ClientId    int64
	RequestId   int
	QueryNumber int
	ServersJoin map[int][]string
	GidsLeave   []int
	ShardMove   int
	GidMove     int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitApplyCh = make(map[int]chan Op)
	sc.lastRequestId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		sc.ReadSnapShotToInstall(snapshot)
	}
	go sc.ReadRaftApplyCommandLoop()
	return sc
}
