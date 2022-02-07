package shardctrler

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

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config编号，顺序号
	Shards [NShards]int     // 分片位置信息，Shards[3] = 2，说明分片序号为3的分片负责的集群式是Group2
	Groups map[int][]string // 集群成员信息，Groups[3] = ip1,ip2，说明gid=3的集群包含两台ip1和ip2的机器
}

const (
	OK = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut = "ErrTimeout"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientId  int64
	RequestId int
}

type JoinReply struct {
	//WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ClientId  int64
	RequestId int
}

type LeaveReply struct {
	//WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ClientId  int64
	RequestId int
}

type MoveReply struct {
	//WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	ClientId  int64
	RequestId int
}

type QueryReply struct {
	//WrongLeader bool
	Err         Err
	Config      Config
}
