package shardctrler

import (
	"6.824-golabs-2021/raft"
	"time"
)
import "6.824-golabs-2021/labrpc"
import "sync"
import "6.824-golabs-2021/labgob"


const (
	CONSENSUS_TIMEOUT = 5000

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
	maxRaftState int
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

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.maxRaftState = -1


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
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: JoinOp, ClientId: args.ClientId, RequestId: args.RequestId, ServersJoin: args.Servers}
	raftIndex,_,_ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	//Timeout WaitFor
	select {
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

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	//DPrintf("[Leave]Server %d, From Client %d, Request %d, leaveGidNum %d", sc.me,args.ClientId,args.Requestid,args.GIDs)
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: LeaveOp, ClientId: args.ClientId, RequestId: args.RequestId, GidsLeave: args.GIDs}
	raftIndex,_,_ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	//Timeout WaitFor
	select {
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
	//DPrintf("[Move]Server %d, From Client %d, Request %d, Shard %d to Gid %d", sc.me,args.ClientId,args.Requestid,args.Shard, args.GID)
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: MoveOp, ClientId: args.ClientId, RequestId: args.RequestId, ShardMove: args.Shard,GidMove: args.GID}
	raftIndex,_,_ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	//Timeout WaitFor
	select {
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
	//DPrintf("[Query]Server %d,From Client %d, Request %d, Num %d", sc.me,args.ClientId,args.Requestid,args.Num)
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("[Leader]Server %d is the leader",sc.me)
	op := Op{Operation: QueryOp,ClientId: args.ClientId, RequestId: args.RequestId, QueryNumber: args.Num}
	raftIndex,_,_ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	//Timeout WaitFor
	select {
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
			// Exec Query
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

