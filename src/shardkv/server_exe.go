package shardkv

import "6.824/raft"
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

func (kv *ShardKV) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)

	if message.CommandIndex <= kv.lastSnapShotRaftLogIndex {
		return
	}

	if op.Operation == NEWCONFIGOp {
		kv.ExecuteNewConfigOpOnServer(op)
		if kv.maxraftstate != -1{
			kv.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
		}
		return
	}

	if op.Operation == MIGRATESHARDOp {
		kv.ExecuteMigrateShardsOnServer(op)
		if kv.maxraftstate != -1{
			kv.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
		}
		kv.SendMessageToWaitChan(op,message.CommandIndex)
		return
	}


	if !kv.isRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) {
		if op.Operation == PUTOp {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == APPENDOp {
			kv.ExecuteAppendOpOnKVDB(op)
		}
	}

	if kv.maxraftstate != -1{
		kv.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
	}

	// Send message to the chan of op.ClientId
	kv.SendMessageToWaitChan(op,message.CommandIndex)
}

func (kv *ShardKV) SendMessageToWaitChan(op Op, raftIndex int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		ch <- op
	}
	return exist
}

func (kv *ShardKV) ExecuteGetOpOnKVDB(op Op) (string, bool){
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value, exist := kv.kvDB[shardNum].KVDBOfShard[op.Key]
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	return value,exist
}

func (kv *ShardKV) ExecutePutOpOnKVDB(op Op) {

	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	kv.kvDB[shardNum].KVDBOfShard[op.Key] = op.Value
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

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
	// all migrate shard should be finished
	for shard := 0; shard < NShards;shard++ {
		if kv.migratingShard[shard] {
			return
		}
	}
	kv.lockMigratingShard(newestConfig.Shards)
	kv.config = newestConfig
}


func (kv *ShardKV) ExecuteMigrateShardsOnServer(op Op){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	myConfig := kv.config
	if op.ConfigNum_MIGRATE != myConfig.Num {
		return
	}
	for _, shardComponent := range op.MigrateData_MIGRATE {
		if !kv.migratingShard[shardComponent.ShardIndex] {
			continue
		}
		kv.migratingShard[shardComponent.ShardIndex] = false
		kv.kvDB[shardComponent.ShardIndex] = ShardComponent{ShardIndex: shardComponent.ShardIndex,KVDBOfShard: make(map[string]string),ClientRequestId: make(map[int64]int)}

		if myConfig.Shards[shardComponent.ShardIndex] == kv.gid {
			CloneSecondComponentIntoFirstExceptShardIndex(&kv.kvDB[shardComponent.ShardIndex],shardComponent)
		}
	}


}

func CloneSecondComponentIntoFirstExceptShardIndex (component *ShardComponent, recive ShardComponent) {
	for key,value := range recive.KVDBOfShard {
		component.KVDBOfShard[key] = value
	}
	for client,request := range recive.ClientRequestId {
		component.ClientRequestId[client] = request
	}
}