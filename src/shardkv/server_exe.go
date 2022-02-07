package shardkv

import "6.824/raft"
// ReadRaftApplyCommandLoop 从raft获取命令
func (kv *ShardKV) ReadRaftApplyCommandLoop() {
	for message := range kv.applyCh{
		if message.CommandValid {
			kv.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			kv.GetSnapShotFromRaft(message)
		}

	}
}

func (kv *ShardKV) isRequestDuplicate(newClientId int64, newRequestId int, shardNum int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestId, ifClientInRecord := kv.kvDB[shardNum].ClientRequestId[newClientId]
	if !ifClientInRecord {
		// kv.lastRequestId[newClientId] = newRequestId
		return false
	}
	return newRequestId <= lastRequestId
}

// GetCommandFromRaft 处理raft的Command
func (kv *ShardKV) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)

	if message.CommandIndex <= kv.lastSnapShotRaftLogIndex {
		return
	}

	// 获取新Config
	if op.Operation == NEWCONFIGOp {
		kv.ExecuteNewConfigOpOnServer(op)
		// 如果maxraftstate不为-1，查看是否需要发送请求使用Snapshot保存快照
		if kv.maxraftstate != -1{
			kv.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
		}
		return
	}
	// 执行迁移操作
	if op.Operation == MIGRATESHARDOp {
		kv.ExecuteMigrateShardsOnServer(op)
		// 如果maxraftstate不为-1，查看是否需要发送请求使用Snapshot保存快照
		if kv.maxraftstate != -1{
			kv.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
		}
		kv.SendMessageToWaitChan(op,message.CommandIndex)
		return
	}

	// 查看命令是否重复，然后执行Put，Append，Get
	if !kv.isRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) {
		if op.Operation == PUTOp {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == APPENDOp {
			kv.ExecuteAppendOpOnKVDB(op)
		}
	}

	// 如果maxraftstate不为-1，查看是否需要发送请求使用Snapshot保存快照
	if kv.maxraftstate != -1{
		kv.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
	}

	// 添加到waitGroupChannel
	kv.SendMessageToWaitChan(op,message.CommandIndex)
}

// SendMessageToWaitChan 将message添加到channel
func (kv *ShardKV) SendMessageToWaitChan(op Op, raftIndex int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		ch <- op
	}
	return exist
}

// ExecuteGetOpOnKVDB 执行Get操作
func (kv *ShardKV) ExecuteGetOpOnKVDB(op Op) (string, bool){
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value, exist := kv.kvDB[shardNum].KVDBOfShard[op.Key]
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	return value,exist
}

// ExecutePutOpOnKVDB 执行put操作
func (kv *ShardKV) ExecutePutOpOnKVDB(op Op) {
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	kv.kvDB[shardNum].KVDBOfShard[op.Key] = op.Value
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

// ExecuteAppendOpOnKVDB 执行append操作
func (kv *ShardKV) ExecuteAppendOpOnKVDB(op Op){
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value,exist := kv.kvDB[shardNum].KVDBOfShard[op.Key]
	if exist {
		kv.kvDB[shardNum].KVDBOfShard[op.Key] = value + op.Value
	} else {
		kv.kvDB[shardNum].KVDBOfShard[op.Key] = op.Value
	}
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

// lockMigratingShard  ApplyNewConfig时触发，将需要迁移的数据锁住
func (kv *ShardKV) lockMigratingShard(newShards [NShards]int) {

	oldShards := kv.config.Shards
	for shard := 0;shard < NShards;shard++ {

		if oldShards[shard] == kv.gid && newShards[shard] != kv.gid {
			if newShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}

		if oldShards[shard] != kv.gid && newShards[shard] == kv.gid {
			if oldShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
	}
}

func (kv *ShardKV) ExecuteNewConfigOpOnServer(op Op){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newestConfig := op.Config_NEWCONFIG
	if newestConfig.Num != kv.config.Num+1 {
		return
	}
	// 所有的分片迁移此时都应该完成了
	for shard := 0; shard < NShards;shard++ {
		if kv.migratingShard[shard] {
			return
		}
	}
	kv.lockMigratingShard(newestConfig.Shards)
	kv.config = newestConfig
}

// ExecuteMigrateShardsOnServer 执行迁移操作
func (kv *ShardKV) ExecuteMigrateShardsOnServer(op Op){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	myConfig := kv.config
	if op.ConfigNum_MIGRATE != myConfig.Num {
		return
	}
	// 处理每一个分片
	for _, shardComponent := range op.MigrateData_MIGRATE {
		if !kv.migratingShard[shardComponent.ShardIndex] {
			continue
		}
		// 将被迁移的shardIndex设置为false
		kv.migratingShard[shardComponent.ShardIndex] = false
		kv.kvDB[shardComponent.ShardIndex] = ShardComponent{ShardIndex: shardComponent.ShardIndex,KVDBOfShard: make(map[string]string),ClientRequestId: make(map[int64]int)}

		// 如果配置的shardIdx == gid，将第二个组件克隆到第一个组件中
		if myConfig.Shards[shardComponent.ShardIndex] == kv.gid {
			CloneSecondComponentIntoFirstExceptShardIndex(&kv.kvDB[shardComponent.ShardIndex],shardComponent)
		}
	}


}
// CloneSecondComponentIntoFirstExceptShardIndex 将第二个组件克隆到第一个组件中
func CloneSecondComponentIntoFirstExceptShardIndex (component *ShardComponent, receive ShardComponent) {
	for key,value := range receive.KVDBOfShard {
		component.KVDBOfShard[key] = value
	}
	for client,request := range receive.ClientRequestId {
		component.ClientRequestId[client] = request
	}
}