package shardkv

import "bytes"
import "6.824/labgob"
import "6.824/raft"
import "6.824/shardctrler"

func (kv *ShardKV) IfNeedToSendSnapShotCommand(raftIndex int, proportion int){
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate*proportion/10){
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}


func (kv *ShardKV) GetSnapShotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapShotToInstall(message.Snapshot)
		kv.lastSnapShotRaftLogIndex = message.SnapshotIndex
	}
}



func (kv *ShardKV) MakeSnapShot() []byte{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.config)
	e.Encode(kv.migratingShard)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) ReadSnapShotToInstall(snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_kvdb []ShardComponent
	var persist_config shardctrler.Config
	var persist_migratingShard [NShards]bool

	if d.Decode(&persist_kvdb) == nil && d.Decode(&persist_config) == nil && d.Decode(&persist_migratingShard) == nil{
		kv.kvDB = persist_kvdb
		kv.config = persist_config
		kv.migratingShard = persist_migratingShard
	}
}