package shardctrler

import "6.824-golabs-2021/raft"

func (sc *ShardCtrler) ReadRaftApplyCommandLoop() {
	for message := range sc.applyCh {
		if message.CommandValid {
			sc.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			sc.GetSnapShotFromRaft(message)
		}
	}
}

func (sc *ShardCtrler) GetCommandFromRaft(message raft.ApplyMsg )  {
	op := message.Command.(Op)
	if message.CommandIndex <= sc.lastSnapShotRaftLogIndex {
		return
	}
	if !sc.isRequestDuplicate(op.ClientId,op.RequestId) {
		if op.Operation == JoinOp
	}
}

func (sc *ShardCtrler) isRequestDuplicate(newClientId int64, newRequestId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastRequestId, isClientRecord := sc.lastRequestId[newClientId]
	if !isClientRecord {
		return false
	}
	return newRequestId <= lastRequestId
}