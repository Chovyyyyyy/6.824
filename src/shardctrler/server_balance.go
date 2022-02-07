package shardctrler

// 对shardNum进行排序
func realNumArray(GidToShardNumMap map[int]int) []int {
	length := len(GidToShardNumMap)

	numArray := make([]int, 0, length)
	for gid, _ := range GidToShardNumMap {
		numArray = append(numArray, gid)
	}

	// 进行排序
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GidToShardNumMap[numArray[j]] < GidToShardNumMap[numArray[j-1]] || (GidToShardNumMap[numArray[j]] == GidToShardNumMap[numArray[j-1]] && numArray[j] < numArray[j-1]) {
				numArray[j], numArray[j-1] = numArray[j-1],numArray[j]
			}
		}
	}
	return numArray
}

// 判断是否平均
func ifAvg(length int, subNum int, i int) bool{
	if i < length-subNum{
		return true
	}else {
		return false
	}
}

func (sc *ShardCtrler) reBalanceShards(GidToShardNumMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GidToShardNumMap)
	// 均值
	average := NShards / length
	// 余数可以用来增加
	subNum := NShards % length
	realSortNum := realNumArray(GidToShardNumMap)

	// 多退少补
	for i := length - 1; i >= 0; i-- {
		resultNum := average
		//如果不平均，resultNum+1
		if !ifAvg(length,subNum,i){
			resultNum = average+1
		}
		// 少了就要+1
		if resultNum < GidToShardNumMap[realSortNum[i]] {
			fromGid := realSortNum[i]
			changeNum := GidToShardNumMap[fromGid] - resultNum
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == fromGid {
					lastShards[shard] = 0
					changeNum -= 1
				}
			}
			GidToShardNumMap[fromGid] = resultNum
		}
	}

	for i := 0; i < length; i++ {
		resultNum := average
		if !ifAvg(length,subNum,i){
			resultNum = average+1
		}
		// 多了就-1
		if resultNum > GidToShardNumMap[realSortNum[i]] {
			toGid := realSortNum[i]
			changeNum := resultNum - GidToShardNumMap[toGid]
			for shard, gid := range lastShards{
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					lastShards[shard] = toGid
					changeNum -= 1
				}
			}
			GidToShardNumMap[toGid] = resultNum
		}

	}
	return lastShards
}

func (sc *ShardCtrler) MakeMoveConfig(shard int, gid int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	// shards默认为10
	tempConfig := Config{Num: len(sc.configs),
						Shards: [10]int{},
						Groups: map[int][]string{}}
	//将分片位置信息的gids更新到tempConfig
	for shards, gids := range lastConfig.Shards {
		tempConfig.Shards[shards] = gids
	}

	tempConfig.Shards[shard] = gid

	//将group的集群成员信息更新到tempConfig
	for gidss, servers := range lastConfig.Groups {
		tempConfig.Groups[gidss] = servers
	}

	return &tempConfig
}

func (sc *ShardCtrler) MakeJoinConfig(servers map[int][]string) *Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	//将lastConfig的集群成员信息更新到tempConfig
	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	// //将servers的集群成员信息更新到tempGroup
	for gids, serverLists := range servers {
		tempGroups[gids] = serverLists
	}

	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		GidToShardNumMap[gid] = 0
	}
	// 更新GidToShardNumMap分片位置信息
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GidToShardNumMap[gid]++
		}
	}

	// 如果添加了分片位置信息需要重新进行负载均衡
	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		// 重新进行负载均衡
		Shards: sc.reBalanceShards(GidToShardNumMap,lastConfig.Shards),
		Groups: tempGroups,
	}
}

func (sc *ShardCtrler) MakeLeaveConfig(gids []int) *Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	// 需要Leave的gid
	ifLeaveSet := make(map[int]bool)
	for _, gid := range gids {
		ifLeaveSet[gid] = true
	}

	// 将lastConfig的集群成员信息添加到tempGroups
	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	//删除gids的集群成员信息
	for _, gidLeave := range gids {
		delete(tempGroups,gidLeave)
	}

	// 创建新的分片位置信息
	newShard := lastConfig.Shards
	GidToShardNumMap := make(map[int]int)
	// 将tempGroups的gid初始化为0
	for gid := range tempGroups {
		if !ifLeaveSet[gid] {
			GidToShardNumMap[gid] = 0
		}

	}
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			// 分片位置信息为0
			if ifLeaveSet[gid] {
				newShard[shard] = 0
			} else {
				GidToShardNumMap[gid]++
			}
		}

	}

	// 如果添加了分片位置信息需要重新进行负载均衡
	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		// 重新进行负载均衡
		Shards: sc.reBalanceShards(GidToShardNumMap,newShard),
		Groups: tempGroups,
	}
}



