package shardkv

import (
	"6.824-golabs-2021/labrpc"
	"6.824-golabs-2021/shardctrler"
	"sync/atomic"
	"time"
)
import "6.824-golabs-2021/raft"
import "sync"
import "6.824-golabs-2021/labgob"

const (
	NShards = shardctrler.NShards
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation          string
	Key                string
	Value              string
	ClientId           int64
	RequestId          int
	ConfigNewConfig    shardctrler.Config
	MigrateDataMigrate []ShardComponent
	ConfigNumMigrate   int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead         int32
	mck          *shardctrler.Clerk
	// Your definitions here.
	KvDataBase []ShardComponent

	waitApplyCh map[int]chan Op

	lastSnapShotRaftLogIndex int

	config shardctrler.Config

	migratingShard [NShards]bool
}
func (kv *ShardKV) CheckShardState(clientNum int,shardIndex int)(bool,bool){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num == clientNum && kv.config.Shards[shardIndex] == kv.gid, !kv.migratingShard[shardIndex]
}

func (kv *ShardKV) CheckMigrateState(shardComponets []ShardComponent) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _,data := range shardComponets {
		if kv.migratingShard[data.ShardIdx] {
			return false
		}
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	shardIdx := key2shard(args.Key)
	isResponse, isAvaliable := kv.CheckShardState(args.ConfigNum, shardIdx)
	if !isResponse {
		reply.Err = ErrWrongGroup
		return
	}
	if !isAvaliable {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: "Get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	raftIdx, _, _ := kv.rf.Start(op)
	kv.mu.Lock()
	chForRaftIdx, exist := kv.waitApplyCh[raftIdx]
	if !exist {
		kv.waitApplyCh[raftIdx] = make(chan Op,1)
		chForRaftIdx = kv.waitApplyCh[raftIdx]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * 500):
		_, isLeader := kv.rf.GetState()
		if kv.isRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) && isLeader {
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

	case raftCommitOp := <-chForRaftIdx:
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
	delete(kv.waitApplyCh, raftIdx)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shardIndex := key2shard(args.Key)
	isResponse, isAvaliable := kv.CheckShardState(args.ConfigNum,shardIndex)
	if !isResponse {
		reply.Err = ErrWrongGroup
		return
	}
	if !isAvaliable {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}

	raftIndex, _, _ := kv.rf.Start(op)

	// create waitForCh
	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <- time.After(time.Millisecond*500) :
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
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
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

	// Your initialization code here.

	kv.KvDataBase = make([]ShardComponent, NShards)
	for shard := 0; shard < NShards; shard++ {
		kv.KvDataBase[shard] = ShardComponent{
			ShardIdx:        shard,
			KVDataBaseShard: make(map[string]string),
			ClientRequestId: make(map[int64]int),
		}
	}
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.waitApplyCh = make(map[int]chan Op)

	snapShot := persister.ReadSnapshot()
	if len(snapShot) > 0 {
		kv.ReadSnapShotToInstall(snapShot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ReadRaftApplyCommandLoop()
	go kv.PullNewConfigLoop()
	go kv.SendShardToOtherGroupLoop()
	return kv
}
