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


// ReadRaftApplyCommandLoop 从raft接收命令
func (sc *ShardCtrler) ReadRaftApplyCommandLoop(){
	for message := range sc.applyCh {
		// 处理命令
		if message.CommandValid {
			sc.GetCommandFromRaft(message)
		}
		// 处理快照
		if message.SnapshotValid {
			sc.GetSnapShotFromRaft(message)
		}
	}
}

// GetCommandFromRaft 处理raft的命令
func (sc *ShardCtrler) GetCommandFromRaft(message raft.ApplyMsg){
	op := message.Command.(Op)

	// 说明command过期
	if message.CommandIndex <= sc.lastSnapShotRaftLogIndex {
		return
	}
	// 判断请求是否重复
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

	// 如果maxraftstate不为-1，查看是否需要发送请求使用Snapshot保存快照
	if sc.maxraftstate != -1{
		sc.IsNeedToSendSnapShotCommand(message.CommandIndex,9)
	}

	// 处理完成之后添加到waitApplyCh中
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

// ExecJoinOnController 执行添加操作
func (sc *ShardCtrler) ExecJoinOnController(op Op) {
	sc.mu.Lock()
	// 更新requestId
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeJoinConfig(op.ServersJoin))
	sc.mu.Unlock()
}

// ExecLeaveOnController 执行移除Group操作
func (sc *ShardCtrler) ExecLeaveOnController(op Op) {
	sc.mu.Lock()
	// 更新requestId
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeLeaveConfig(op.GidsLeave))
	sc.mu.Unlock()
}

// ExecMoveOnController 将Shard分配给GID的Group
func (sc *ShardCtrler) ExecMoveOnController(op Op) {
	sc.mu.Lock()
	// 更新requestId
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeMoveConfig(op.ShardMove,op.GidMove))
	sc.mu.Unlock()
}

// 请求是否重复
func (sc *ShardCtrler) isRequestDuplicate(newClientId int64, newRequestId int) bool{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 如果消息重复返回true
	lastRequestId, ifClientInRecord := sc.lastRequestId[newClientId]
	if !ifClientInRecord {
		// kv.lastRequestId[newClientId] = newRequestId
		return false
	}
	return newRequestId <= lastRequestId
}


