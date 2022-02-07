package shardctrler


import "bytes"
import "6.824/labgob"
import "6.824/raft"

// IsNeedToSendSnapShotCommand 使用Snapshot保存快照
func (sc *ShardCtrler) IsNeedToSendSnapShotCommand(raftIndex int, proportion int){
	if sc.rf.GetRaftStateSize() > (sc.maxraftstate*proportion/10){
		// 发送snapshot命令
		snapshot := sc.MakeSnapShot()
		sc.rf.Snapshot(raftIndex, snapshot)
	}
}


// GetSnapShotFromRaft 安装快照
func (sc *ShardCtrler) GetSnapShotFromRaft(message raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		sc.ReadSnapShotToInstall(message.Snapshot)
		sc.lastSnapShotRaftLogIndex = message.SnapshotIndex
	}
}


func (sc *ShardCtrler) MakeSnapShot() []byte{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.lastRequestId)
	data := w.Bytes()
	return data
}

func (sc *ShardCtrler) ReadSnapShotToInstall(snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_config []Config
	var persist_lastRequestId map[int64]int

	if d.Decode(&persist_config) == nil && d.Decode(&persist_lastRequestId) == nil {
		sc.configs = persist_config
		sc.lastRequestId = persist_lastRequestId
	}
}
