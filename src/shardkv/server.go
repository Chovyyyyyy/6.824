package shardkv


import (
	"6.824/labrpc"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"

const (
	Debug = false
	CONSENSUS_TIMEOUT = 500 // ms
	CONFIGCHECK_TIMEOUT = 90
	SENDSHARDS_TIMEOUT = 150
	NShards = shardctrler.NShards

	GETOp = "get"
	PUTOp = "put"
	APPENDOp = "append"
	MIGRATESHARDOp = "migrate"
	NEWCONFIGOp = "newconfig"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // "get" "put" "append"
	Key string
	Value string
	ClientId int64
	RequestId int
	Config_NEWCONFIG shardctrler.Config
	MigrateData_MIGRATE []ShardComponent
	ConfigNum_MIGRATE int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	dead 		 int32
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int // 服务器包含哪些分片
	ctrlers      []*labrpc.ClientEnd
	mck       	 *shardctrler.Clerk
	maxraftstate int // 如果日志达到极限压缩为快照

	// Your definitions here.
	kvDB []ShardComponent //每个 Shard 都有独立的数据 MAP 和 ClientSeq MAP
	waitApplyCh map[int]chan Op

	lastSnapShotRaftLogIndex int

	config shardctrler.Config
	migratingShard [NShards]bool // 保证迁移中拒绝服务，避免脏读和旧数据


}

// CheckShardState 查看当前shard的状态是否可用
func (kv *ShardKV) CheckShardState(clientNum int,shardIndex int)(bool,bool){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num == clientNum && kv.config.Shards[shardIndex] == kv.gid, !kv.migratingShard[shardIndex]
}

// CheckMigrateState 查看是否有正在迁移的shard
func (kv *ShardKV) CheckMigrateState(shardComponets []ShardComponent) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _,shdata := range shardComponets {
		if kv.migratingShard[shdata.ShardIndex] {
			return false
		}
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	shardIndex := key2shard(args.Key)
	ifRes, ifAva := kv.CheckShardState(args.ConfigNum,shardIndex)
	if !ifRes {
		reply.Err = ErrWrongGroup
		return
	}
	if !ifAva {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: GETOp, Key: args.Key, Value: "", ClientId: args.ClientId, RequestId: args.RequestId}

	raftIndex, _, _ := kv.rf.Start(op)

	// create waitForCh
	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()
	// timeout
	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):

		_, ifLeader := kv.rf.GetState()
		if kv.isRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) && ifLeader {
			value, exists := kv.ExecuteGetOpOnKVDB(op)
			if exists {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shardIndex := key2shard(args.Key)
	ifRes, ifAva := kv.CheckShardState(args.ConfigNum,shardIndex)
	if !ifRes {
		reply.Err = ErrWrongGroup
		return
	}
	if !ifAva {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: args.Opreation, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}

	raftIndex, _, _ := kv.rf.Start(op)
	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT) :
		if kv.isRequestDuplicate(op.ClientId,op.RequestId,key2shard(op.Key)){
			reply.Err = OK
		} else{
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <- chForRaftIndex :
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId  {
			reply.Err = OK
		}else{
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh,raftIndex)
	kv.mu.Unlock()
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead,1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.kvDB = make([]ShardComponent, NShards)
	// 初始化每个分片的kvDataBase
	for shard:=0;shard<NShards;shard++ {
		kv.kvDB[shard] = ShardComponent{ShardIndex: shard, KVDBOfShard: make(map[string]string), ClientRequestId: make(map[int64]int)}
	}

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.waitApplyCh = make(map[int]chan Op)

	// 读取块找
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0{
		kv.ReadSnapShotToInstall(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ReadRaftApplyCommandLoop()

	go kv.PullNewConfigLoop()

	go kv.SendShardToOtherGroupLoop()

	return kv
}
