package shardctrler

import (
	"6.824/raft"
)
//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//



func (sc *ShardCtrler) ReadRaftApplyCommandLoop(){
	for message := range sc.applyCh {
		if message.CommandValid {
			sc.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			sc.GetSnapShotFromRaft(message)
		}
	}
}

func (sc *ShardCtrler) GetCommandFromRaft(message raft.ApplyMsg){
	op := message.Command.(Op)

	if message.CommandIndex <= sc.lastSnapShotRaftLogIndex {
		return
	}
	if !sc.isRequestDuplicate(op.ClientId, op.RequestId){
		if op.Operation == JoinOp {
			sc.ExecJoinOnController(op)
		}
		if op.Operation == LeaveOp {
			sc.ExecLeaveOnController(op)
		}
		if op.Operation == MoveOp {
			sc.ExecMoveOnController(op)
		}
	}

	if sc.maxraftstate != -1{
		sc.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
	}

	sc.SendMessageToWaitChan(op, message.CommandIndex)
}

func (sc *ShardCtrler) SendMessageToWaitChan(op Op, raftIndex int) bool{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitApplyCh[raftIndex]
	if exist {
		ch <- op
	}
	return exist
}

func (sc *ShardCtrler) InitNewConfig()Config{
	var tempConfig Config
	if len(sc.configs) == 1 {
		tempConfig = Config{Num:1, Shards: [10]int{}, Groups: map[int][]string{}}
	} else {
		newestConfig := sc.configs[len(sc.configs)-1]
		tempConfig = Config{Num:newestConfig.Num+1, Shards: [10]int{}, Groups: map[int][]string{}}
		for index, gid := range newestConfig.Shards {
			tempConfig.Shards[index] = gid
		}
		for gid, groups := range newestConfig.Groups {
			tempConfig.Groups[gid] = groups
		}
	}
	return tempConfig
}


func ShowConfig(config Config, op string){
	DPrintf("=========== Config For op %v", op)
	DPrintf("[ConfigNum]%d",config.Num)
	for index, value := range config.Shards {
		DPrintf("[shards]Shard %d --> gid %d", index,value)
	}
	for gid,servers := range config.Groups {
		DPrintf("[Groups]Gid %d --> servers %v", gid, servers)
	}
}

func (sc *ShardCtrler) BalanceShardToGid(config *Config){
	length := len(config.Groups)
	average := NShards/length
	subNum := NShards - average*length
	avgNum := length - subNum

	zeroAimGid := 0
	for gid,_ := range config.Groups {
		if gid != 0 {
			zeroAimGid = gid
		}
	}

	// countArray should not be have Gid 0
	for shards, gid := range config.Shards {
		if gid == 0 {
			config.Shards[shards] = zeroAimGid
		}
	}

	countArray := make(map[int][]int)
	for shardIndex,gid := range config.Shards {
		if _, exist := countArray[gid];exist {
			countArray[gid] = append(countArray[gid],shardIndex)
		} else {
			countArray[gid] = make([]int,0)
			countArray[gid] = append(countArray[gid], shardIndex)
		}
	}
	for gid,_ := range config.Groups {
		if _,exist := countArray[gid];!exist {
			countArray[gid] = make([]int,0)
		}
	}

	for {
		if isBalance(average,avgNum,subNum,countArray){
			break
		}
		// make Max Gid One Shard to Min Gid
		maxShardsNum := -1
		maxGid := -1
		minShardsNum := NShards*10
		minGid := -1
		for gid, shardsArray := range countArray {
			if len(shardsArray) >= maxShardsNum {
				maxShardsNum = len(shardsArray)
				maxGid = gid
			}
			if len(shardsArray) <= minShardsNum {
				minShardsNum = len(shardsArray)
				minGid = gid
			}
		}

		fromGid := maxGid
		movedShard := countArray[maxGid][maxShardsNum-1]
		toGid := minGid

		countArray[fromGid] = countArray[fromGid][:maxShardsNum-1]
		countArray[toGid] = append(countArray[toGid], movedShard)
		config.Shards[movedShard] = toGid
	}

}

func isBalance(average int, avgNum int, subNum int, countArray map[int][]int) bool{
	shouldAvg := 0
	shouldAvgPlus := 0
	for gid, shards := range countArray {
		if len(shards) == average && gid != 0{
			shouldAvg++
		}
		if len(shards) == average+1 && gid != 0{
			shouldAvgPlus++
		}
	}

	return shouldAvg == avgNum && shouldAvgPlus == subNum
}

func (sc *ShardCtrler) ExecQueryOnController(op Op) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	if op.NumQuery == -1 || op.NumQuery >= len(sc.configs){
		return sc.configs[len(sc.configs) - 1]
	} else {
		return sc.configs[op.NumQuery]
	}

}

func (sc *ShardCtrler) ExecJoinOnController(op Op) {
	sc.mu.Lock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeJoinConfig(op.ServersJoin))
	sc.mu.Unlock()
}

func (sc *ShardCtrler) ExecLeaveOnController(op Op) {
	sc.mu.Lock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeLeaveConfig(op.GidsLeave))
	sc.mu.Unlock()
}

func (sc *ShardCtrler) ExecMoveOnController(op Op) {
	sc.mu.Lock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeMoveConfig(op.ShardMove,op.GidMove))
	sc.mu.Unlock()
}


func (sc *ShardCtrler) isRequestDuplicate(newClientId int64, newRequestId int) bool{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// return true if message is duplicate
	lastRequestId, ifClientInRecord := sc.lastRequestId[newClientId]
	if !ifClientInRecord {
		// kv.lastRequestId[newClientId] = newRequestId
		return false
	}
	return newRequestId <= lastRequestId
}


