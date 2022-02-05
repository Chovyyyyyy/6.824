package kvraft

import "6.824/raft"

func (kv *KVServer) ReadRaftApplyCommandLoop() {
	for message := range kv.applyCh {
		// 监听raft的每个命令，传递给相关的RPCHandler
		if message.CommandValid {
			kv.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			kv.GetSnapShotFromRaft(message)
		}

	}
}

func (kv *KVServer) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)

	if message.CommandIndex <= kv.lastSnapShotRaftLogIndex {
		return
	}

	// 重复的命令不会执行
	if !kv.isRequestDuplicate(op.ClientId, op.RequestId) {
		if op.Operation == "put" {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == "append" {
			kv.ExecuteAppendOpOnKVDB(op)
		}
	}

	if kv.maxraftstate != -1 {
		kv.IsNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}

	kv.SendMessageToWaitChan(op, message.CommandIndex)
}

func (kv *KVServer) SendMessageToWaitChan(op Op, raftIndex int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		ch <- op
	}
	return exist
}

func (kv *KVServer) ExecuteGetOpOnKVDB(op Op) (string, bool) {
	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	return value, exist
}

func (kv *KVServer) ExecutePutOpOnKVDB(op Op) {
	kv.mu.Lock()
	kv.kvDB[op.Key] = op.Value
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

func (kv *KVServer) ExecuteAppendOpOnKVDB(op Op) {
	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	if exist {
		kv.kvDB[op.Key] = value + op.Value
	} else {
		kv.kvDB[op.Key] = op.Value
	}
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

func (kv *KVServer) isRequestDuplicate(newClientId int64, newRequestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// return true if message is duplicate
	lastRequestId, isClientInRecord := kv.lastRequestId[newClientId]
	if !isClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}
