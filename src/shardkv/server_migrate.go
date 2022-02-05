package shardkv

import "time"



func (kv *ShardKV) PullNewConfigLoop() {
	for !kv.killed(){
		kv.mu.Lock()
		lastConfigNum := kv.config.Num
		_,ifLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !ifLeader{
			time.Sleep(CONFIGCHECK_TIMEOUT*time.Millisecond)
			continue
		}

		newestConfig := kv.mck.Query(lastConfigNum+1)
		if newestConfig.Num == lastConfigNum+1 {
			// Got a new Config
			op := Op{Operation: NEWCONFIGOp, Config_NEWCONFIG: newestConfig}
			kv.mu.Lock()
			if _,ifLeader := kv.rf.GetState(); ifLeader{
				kv.rf.Start(op)
				}
			kv.mu.Unlock()
		}

		time.Sleep(CONFIGCHECK_TIMEOUT*time.Millisecond)
	}
}


func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.Lock()
	myConfigNum := kv.config.Num
	kv.mu.Unlock()
	if args.ConfigNum > myConfigNum {
		reply.Err = ErrConfigNum
		reply.ConfigNum = myConfigNum
		return
	}

	if args.ConfigNum < myConfigNum {
		reply.Err = OK
		return
	}

	if kv.CheckMigrateState(args.MigrateData) {
		reply.Err = OK
		return
	}

	op := Op{Operation: MIGRATESHARDOp, MigrateData_MIGRATE: args.MigrateData, ConfigNum_MIGRATE: args.ConfigNum}

	raftIndex, _, _ := kv.rf.Start(op)

	// create waitForCh
	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()
	// timeout
	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		kv.mu.Lock()
		_, ifLeader := kv.rf.GetState()
		tempConfig := kv.config.Num
		kv.mu.Unlock()

		if args.ConfigNum <= tempConfig && kv.CheckMigrateState(args.MigrateData) && ifLeader {
			reply.ConfigNum = tempConfig
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:
		kv.mu.Lock()
		tempConfig := kv.config.Num
		kv.mu.Unlock()
		if raftCommitOp.ConfigNum_MIGRATE == args.ConfigNum && args.ConfigNum <= tempConfig && kv.CheckMigrateState(args.MigrateData) {
			reply.ConfigNum = tempConfig
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}


func (kv *ShardKV) SendShardToOtherGroupLoop() {
	for !kv.killed(){
		kv.mu.Lock()
		_,ifLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !ifLeader{
			time.Sleep(SENDSHARDS_TIMEOUT*time.Millisecond)
			continue
		}

		noMigrate := true
		kv.mu.Lock()
		for shard := 0;shard < NShards;shard++ {
			if kv.migratingShard[shard]{
				noMigrate = false
			}
		}
		kv.mu.Unlock()
		if noMigrate{
			time.Sleep(SENDSHARDS_TIMEOUT*time.Millisecond)
			continue
		}

		ifNeedSend, sendData := kv.ifHaveSendData()
		if !ifNeedSend{
			time.Sleep(SENDSHARDS_TIMEOUT*time.Millisecond)
			continue
		}
		kv.sendShardComponent(sendData)
		time.Sleep(SENDSHARDS_TIMEOUT*time.Millisecond)
	}
}

func (kv *ShardKV) ifHaveSendData() (bool, map[int][]ShardComponent) {
	sendData := kv.MakeSendShardComponent()
	if len(sendData) == 0 {
		return false,make(map[int][]ShardComponent)
	}
	return true,sendData
}

func (kv *ShardKV) MakeSendShardComponent()(map[int][]ShardComponent){
	// kv.config already be update
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sendData := make(map[int][]ShardComponent)
	for shard :=0;shard<NShards;shard++ {
		nowOwner := kv.config.Shards[shard]
		if kv.migratingShard[shard] && kv.gid != nowOwner{
			tempComponent := ShardComponent{ShardIndex: shard,KVDBOfShard: make(map[string]string),ClientRequestId: make(map[int64]int)}
			CloneSecondComponentIntoFirstExceptShardIndex(&tempComponent,kv.kvDB[shard])
			sendData[nowOwner] = append(sendData[nowOwner],tempComponent)
		}
	}
	return sendData
}

func (kv *ShardKV) sendShardComponent(sendData map[int][]ShardComponent) {
	for aimGid, ShardComponents := range sendData {
		kv.mu.Lock()
		args := &MigrateShardArgs{ConfigNum: kv.config.Num, MigrateData: make([]ShardComponent,0)}
		groupServers := kv.config.Groups[aimGid]
		kv.mu.Unlock()
		for _,components := range ShardComponents {
			tempComponent := ShardComponent{ShardIndex: components.ShardIndex,KVDBOfShard: make(map[string]string),ClientRequestId: make(map[int64]int)}
			CloneSecondComponentIntoFirstExceptShardIndex(&tempComponent,components)
			args.MigrateData = append(args.MigrateData,tempComponent)
		}

		go kv.callMigrateRPC(groupServers,args)
	}
}

func (kv *ShardKV) callMigrateRPC(groupServers []string, args *MigrateShardArgs){
	for _, groupMember := range groupServers {
		callEnd := kv.make_end(groupMember)
		migrateReply := MigrateShardReply{}
		ok := callEnd.Call("ShardKV.MigrateShard", args, &migrateReply)
		kv.mu.Lock()
		myConfigNum := kv.config.Num
		kv.mu.Unlock()
		if ok && migrateReply.Err == OK {
			if myConfigNum != args.ConfigNum || kv.CheckMigrateState(args.MigrateData){
				return
			} else {
				kv.rf.Start(Op{Operation: MIGRATESHARDOp,MigrateData_MIGRATE: args.MigrateData,ConfigNum_MIGRATE: args.ConfigNum})
				return
			}
		}
	}
}