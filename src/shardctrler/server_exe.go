package shardctrler

import (
	"6.824-golabs-2021/raft"
)

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

	if sc.maxRaftState != -1 {
		sc.IsNeedToSendSnapShotCommand(message.CommandIndex,9)
	}

	sc.SendMessageToWaitChan(op,message.CommandIndex)
}

func (sc *ShardCtrler) SendMessageToWaitChan(op Op,raftIdx int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitApplyCh[raftIdx]
	if exist {
		ch <- op
	}
	return exist
}

func (sc *ShardCtrler) InitNewConfig() Config {
	var tempConfig Config
	if len(sc.configs) == 1 {
		tempConfig = Config{Num: 1,Shards: [10]int{},Groups: map[int][]string{}}
	} else {
		newestConfig := sc.configs[len(sc.configs)-1]
		tempConfig = Config{
			Num: newestConfig.Num+1,
			Shards: [10]int{},
			Groups: map[int][]string{},
		}
		for idx, gid := range newestConfig.Shards {
			tempConfig.Shards[idx] = gid
		}
		for gid,groups := range newestConfig.Groups {
			tempConfig.Groups[gid] = groups
		}
	}
	return tempConfig
}

func (sc *ShardCtrler) ExecQueryOnController(op Op) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	if op.QueryNumber == -1 || op.QueryNumber >= len(sc.configs) {
		return sc.configs[len(sc.configs) - 1]
	} else {
		return sc.configs[op.QueryNumber]
	}
}

func (sc *ShardCtrler) ExecJoinOnController(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeJoinConfig(op.ServersJoin))
}

func (sc *ShardCtrler)ExecLeaveOnController(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeLeaveConfig(op.GidsLeave))
}

func (sc *ShardCtrler) ExecMoveOnController(op Op) {
	sc.mu.Lock()
	//DPrintf("[Exec]Server %d, MOVE, ClientId %d, RequestId %d",sc.me, op.ClientId,op.RequestId)
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeMoveConfig(op.ShardMove,op.GidMove))
	sc.mu.Unlock()
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


func (sc *ShardCtrler) BalanceShardToGid(config *Config){
	length := len(config.Groups)
	average := NShards/length
	subNum := NShards - average*length
	avgNum := length - subNum
	// len(config.Groups) - subNum --> average
	// subNum --> average+1

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

	// count Every Gid mangement which Shard ???
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
		if ifBalance(average,avgNum,subNum,countArray){
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

		//DPrintf("[Blance]Shard %d from Gid %d ===> Gid %d",movedShard, fromGid,toGid)
		countArray[fromGid] = countArray[fromGid][:maxShardsNum-1]
		countArray[toGid] = append(countArray[toGid], movedShard)
		config.Shards[movedShard] = toGid
	}

}

func ifBalance(average int, avgNum int, subNum int, countArray map[int][]int) bool{
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