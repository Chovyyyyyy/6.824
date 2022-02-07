package shardctrler


import (
	"6.824/raft"
	"log"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	Debug = false
	CONSENSUS_TIMEOUT = 5000 // ms

	QueryOp = "query"
	JoinOp = "join"
	LeaveOp = "leave"
	MoveOp = "move"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	maxraftstate int
	applyCh chan raft.ApplyMsg

	// Your data here.
  	configs []Config

	// 和lab3类似
	waitApplyCh map[int]chan Op
	lastRequestId map[int64]int
	lastSnapShotRaftLogIndex int
}


type Op struct {
	// Your data here.
	Operation string
	ClientId int64
	RequestId    int
	NumQuery    int
	ServersJoin map[int][]string
	GidsLeave []int
	ShardMove int
	GidMove   int
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
	sc.maxraftstate = -1


	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.waitApplyCh = make(map[int]chan Op)
	sc.lastRequestId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0{
		sc.ReadSnapShotToInstall(snapshot)
	}
	go sc.ReadRaftApplyCommandLoop()
	return sc
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{Operation: JoinOp, ClientId: args.ClientId, RequestId: args.RequestId, ServersJoin: args.Servers}
	raftIndex,_,_ := sc.rf.Start(op)

	// 创建channel
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	select {
	// 超时
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT):
		if sc.isRequestDuplicate(op.ClientId, op.RequestId){
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	// 接收到消息
	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
	return

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: LeaveOp, ClientId: args.ClientId, RequestId: args.RequestId, GidsLeave: args.GIDs}
	raftIndex,_,_ := sc.rf.Start(op)

	// 创建channel
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()


	select {
	// 超时
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT):
		if sc.isRequestDuplicate(op.ClientId, op.RequestId){
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		}else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: MoveOp, ClientId: args.ClientId, RequestId: args.RequestId, ShardMove: args.Shard, GidMove: args.GID}
	raftIndex,_,_ := sc.rf.Start(op)

	// 创建channel
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	select {
	// 超时
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT):
		if sc.isRequestDuplicate(op.ClientId, op.RequestId){
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{Operation: QueryOp,ClientId: args.ClientId, RequestId: args.RequestId, NumQuery: args.Num}
	raftIndex,_,_ := sc.rf.Start(op)

	// 创建channel
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	select {
	// 超时
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT):
		_,ifLeader := sc.rf.GetState()
		if sc.isRequestDuplicate(op.ClientId, op.RequestId) && ifLeader{
			reply.Config = sc.ExecQueryOnController(op)
			reply.Err = OK
		}else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			// 执行查询操作
			reply.Config = sc.ExecQueryOnController(op)
			reply.Err = OK
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
	return
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


