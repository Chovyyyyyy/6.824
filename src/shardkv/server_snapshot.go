package shardkv

import (
	"6.824-golabs-2021/labgob"
	"6.824-golabs-2021/raft"
	"6.824-golabs-2021/shardctrler"
	"bytes"
)

func (kv *ShardKV) IsNeedToSendSnapShotCommand(raftIndex int, proportion int){
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate*proportion/10){
		// Send SnapShot Command
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

func (kv *ShardKV) ReadSnapShotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistKVDataBase []ShardComponent
	var persistConfig shardctrler.Config
	var persistMigratingShard [NShards]bool

	if d.Decode(&persistKVDataBase) == nil && d.Decode(&persistConfig) == nil && d.Decode(&persistMigratingShard) == nil{
		kv.KvDataBase = persistKVDataBase
		kv.config = persistConfig
		kv.migratingShard = persistMigratingShard
	}
}

func (kv *ShardKV) MakeSnapShot() []byte{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.KvDataBase)
	e.Encode(kv.config)
	e.Encode(kv.migratingShard)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) GetSnapShotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapShotToInstall(message.Snapshot)
		kv.lastSnapShotRaftLogIndex = message.SnapshotIndex
	}
}
