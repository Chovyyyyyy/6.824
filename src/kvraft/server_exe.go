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

	// 可以根据出现在那个索引处的操作是否实际上是你放在那里的操作
	if message.CommandIndex <= kv.lastSnapShotRaftLogIndex {
		return
	}

	// 重复的命令不会执行
	if !kv.isRequestDuplicate(op.ClientId, op.RequestId) {
		// put命令
		if op.Operation == "put" {
			kv.ExecutePutOpOnKVDB(op)
		}
		// append命令
		if op.Operation == "append" {
			kv.ExecuteAppendOpOnKVDB(op)
		}
	}

	// 如果maxraftstate不为-1，查看是否需要发送请求使用Snapshot保存快照
	if kv.maxraftstate != -1 {
		kv.IsNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}
	// 把raft的命令添加到channel当中
	kv.SendMessageToWaitChan(op, message.CommandIndex)
}
// SendMessageToWaitChan 等待server进行处理
func (kv *KVServer) SendMessageToWaitChan(op Op, raftIndex int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		ch <- op
	}
	return exist
}

// ExecuteGetOpOnKVDB 在kvDB执行Get操作
func (kv *KVServer) ExecuteGetOpOnKVDB(op Op) (string, bool) {
	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	return value, exist
}

// ExecutePutOpOnKVDB 在kvDB执行Put操作
func (kv *KVServer) ExecutePutOpOnKVDB(op Op) {
	kv.mu.Lock()
	kv.kvDB[op.Key] = op.Value
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}
// ExecuteAppendOpOnKVDB 在kvDB执行Append操作
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

// 根据requestId判断请求是否重复
func (kv *KVServer) isRequestDuplicate(newClientId int64, newRequestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 如果message存在，返回true
	lastRequestId, isClientInRecord := kv.lastRequestId[newClientId]
	if !isClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}
