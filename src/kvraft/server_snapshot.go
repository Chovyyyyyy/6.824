package kvraft

import "bytes"
import "6.824/labgob"
import "6.824/raft"
/**
TODO 测试人员将maxraftstate传递给您的 StartKVServer()。
TODO maxraftstate表示持久 Raft 状态的最大允许大小（以字节为单位）（包括日志，但不包括快照）。
TODO 您应该将maxraftstate与persister.RaftStateSize()进行比较。
TODO 每当您的键/值服务器检测到 Raft 状态大小接近此阈值时，它应该使用Snapshot保存快照
TODO ，而后者又使用persister.SaveRaftState()。如果maxraftstate为 -1，则不必进行快照。
TODO maxraftstate适用于 Raft 传递给persister.SaveRaftState()的 GOB 编码字节。
 */
// IsNeedToSendSnapShotCommand 根据RaftStateSize 和阈值maxraftestate判断是否需要命令Raft进行Snapshot
func (kv *KVServer) IsNeedToSendSnapShotCommand(raftIndex int, proportion int){
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate*proportion/10){
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}


// GetSnapShotFromRaft 安装快照
func (kv *KVServer) GetSnapShotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapShotToInstall(message.Snapshot)
		kv.lastSnapShotRaftLogIndex = message.SnapshotIndex
	}
}




func (kv *KVServer) MakeSnapShot() []byte{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastRequestId)
	data := w.Bytes()
	return data
}

func (kv *KVServer) ReadSnapShotToInstall(snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_kvdb map[string]string
	var persist_lastRequestId map[int64]int

	if d.Decode(&persist_kvdb) == nil && d.Decode(&persist_lastRequestId) == nil {
		kv.kvDB = persist_kvdb
		kv.lastRequestId = persist_lastRequestId
	}
}