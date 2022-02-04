package shardctrler

func realNumArray(GidToShardNumMap map[int]int) []int {
	length := len(GidToShardNumMap)
	numArray := make([]int, 0, length)
	for gid, _ := range GidToShardNumMap {
		numArray = append(numArray, gid)
	}

	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GidToShardNumMap[numArray[j]] < GidToShardNumMap[numArray[j-1]] || (GidToShardNumMap[numArray[j]] == GidToShardNumMap[numArray[j-1]] && numArray[j] < numArray[j-1]) {
				numArray[j], numArray[j-1] = numArray[j-1], numArray[j]
			}
		}
	}
	return numArray
}

func isAvg(length int, subNum int, i int) bool {
	if i < length-subNum {
		return true
	} else {
		return false
	}
}

func (sc *ShardCtrler) reBalanceShards(GidToShardNumMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GidToShardNumMap)
	average := NShards / length
	subNum := NShards % length
	realSortNum := realNumArray(GidToShardNumMap)

	for i := length - 1; i >= 0; i-- {
		resultNum := average
		if !isAvg(length, subNum, i) {
			resultNum = average + 1
		}
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
		if !isAvg(length, subNum, i) {
			resultNum = average + 1
		}
		if resultNum > GidToShardNumMap[realSortNum[i]] {
			toGid := realSortNum[i]
			changeNum := resultNum - GidToShardNumMap[toGid]
			for shard, gid := range lastShards {
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

func (sc *ShardCtrler) MakeJoinConfig(servers map[int][]string) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	for gids, serverLists := range servers {
		tempGroups[gids] = serverLists
	}
	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		GidToShardNumMap[gid] = 0
	}

	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GidToShardNumMap[gid]++
		}
	}

	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(GidToShardNumMap, lastConfig.Shards),
		Groups: tempGroups,
	}
}

func (sc *ShardCtrler) MakeMoveConfig(shard int, gid int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	tempConfig := Config{
		Num: len(sc.configs),
		Shards: [10]int{},
		Groups: map[int][]string{},
	}
	for shards, gids := range lastConfig.Shards {
		tempConfig.Shards[shards] = gids
	}
	tempConfig.Shards[shard] = gid

	for gids,servers := range lastConfig.Groups {
		tempConfig.Groups[gids] = servers
	}
	return &tempConfig
}

func (sc *ShardCtrler) MakeLeaveConfig(gids []int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	isLeaveSet := make(map[int]bool)

	for _, gid := range gids {
		isLeaveSet[gid] = true
	}

	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	for _,gidLeave := range gids {
		delete(tempGroups,gidLeave)
	}

	newShard := lastConfig.Shards
	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		if !isLeaveSet[gid] {
			GidToShardNumMap[gid] = 0
		}
	}
	for shard,gid := range lastConfig.Shards {
		if gid != 0 {
			if isLeaveSet[gid] {
				newShard[shard] = 0
			} else {
				GidToShardNumMap[gid]++
			}
		}
	}
	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num : len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}
	return &Config{
		Num: len(sc.configs),
		Shards: sc.reBalanceShards(GidToShardNumMap,newShard),
		Groups: tempGroups,
	}

}
