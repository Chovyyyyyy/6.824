package shardctrler

import (
	"6.824-golabs-2021/labgob"
	"6.824-golabs-2021/raft"
	"bytes"
)

func (sc *ShardCtrler) IsNeedToSendSnapShotCommand(raftIdx int, proportion int) {
	if sc.rf.GetRaftStateSize() > sc.maxRaftState*proportion/10 {
		snapshot := sc.MakeSnapShot()
		sc.rf.Snapshot(raftIdx,snapshot)
	}
}

func (sc *ShardCtrler) GetSnapShotFromRaft(message raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.rf.CondInstallSnapshot(message.SnapshotTerm,message.SnapshotIndex,message.Snapshot) {
		sc.ReadSnapShotToInstall(message.Snapshot)
		sc.lastSnapShotRaftLogIndex = message.SnapshotIndex
	}
}

func (sc *ShardCtrler) MakeSnapShot() []byte {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.lastRequestId)
	data := w.Bytes()
	return data
}

func (sc *ShardCtrler) ReadSnapShotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) <1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistConfig []Config
	var persistLastRequest map[int64]int

	if d.Decode(&persistConfig) == nil && d.Decode(&persistLastRequest) == nil {
		sc.configs = persistConfig
		sc.lastRequestId = persistLastRequest
	}
}
