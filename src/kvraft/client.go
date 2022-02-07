package kvraft

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

// TODO 本实验要求你使用lab 2中的Raft库构建一个容错的Key/Value服务。
// TODO 你的Key/Value服务应该是由几个使用Raft来维护复制的key/value服务器组成的一个复制状态机
// TODO 尽管存在一些其他故障或网络分区，但只要大多数服务器还活着并可以通信，
// TODO 你的key/value服务就应该继续处理客户端请求。
type Clerk struct {
	servers []*labrpc.ClientEnd
	clientId int64
	requestId int // requestId为了确保线性一致性
	recentLeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.recentLeaderId = GetRandomServer(len(ck.servers))
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func GetRandomServer(length int) int{
	return mathrand.Intn(length)
}

func (ck *Clerk) SendGetToServer(key string, server int) string {
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: ck.requestId}
	reply := GetReply{}
	ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
	//Clerk有时不知道哪个kvServer是Raft的领导者。
	//如果Clerk将一个RPC发送到错误的kvServer，或者它无法到达kvServer，
	//Clerk应该通过发送到一个不同的kvServer来重试。
	//如果key/value服务器将操作提交到它的Raft日志(并因此将该操作应用到key/value状态机)，
	//则领导者通过响应其RPC将结果报告给Clerk。
	//如果操作未能提交(例如，如果领导者被替换)，服务器报告一个错误，
	//并且Clerk用一个不同的服务器重试。
	if !ok || reply.Err == ErrWrongLeader{
		return ck.SendGetToServer(key,(server+1) % len(ck.servers))
	}

	if reply.Err == OK {
		ck.recentLeaderId = server
		return reply.Value
	}

	return ""
}

func (ck *Clerk) Get(key string) string {
	// 线性一致性
	ck.requestId++
	requestId := ck.requestId
	server := ck.recentLeaderId
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: requestId}

	for {
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		// 判断请求是否成功以及raft节点是否是leader
		if !ok || reply.Err == ErrWrongLeader{
			server = (server+1)%len(ck.servers)
			//time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == ErrNoKey {
			return ""
		}
		// 如果reply是OK则更新recentLeaderId
		if reply.Err == OK {
			ck.recentLeaderId = server
			return reply.Value
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) SendPutAppendToServer(key string,value string, opreation string, server int) {
	args := PutAppendArgs{Key: key, Value: value, Opreation : opreation, ClientId: ck.clientId, RequestId: ck.requestId}
	reply := PutAppendReply{}

	ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)

	if !ok || reply.Err == ErrWrongLeader{
		ck.SendPutAppendToServer(key,value,opreation,(server+1) % len(ck.servers))
	}

	if reply.Err == OK {
		ck.recentLeaderId = server
		return
	}
}

func (ck *Clerk) PutAppend(key string, value string, opreation string) {
	// 线性一致性
	ck.requestId++
	requestId := ck.requestId
	server := ck.recentLeaderId
	for {
		args := PutAppendArgs{Key: key, Value: value, Opreation : opreation, ClientId: ck.clientId, RequestId: requestId}
		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		// 判断请求是否成功以及raft节点是否是leader
		if !ok || reply.Err == ErrWrongLeader{
			server = (server+1)%len(ck.servers)
			//time.Sleep(100 * time.Millisecond)
			continue
		}
		// 如果reply是OK则更新recentLeaderId
		if reply.Err == OK {
			ck.recentLeaderId = server
			return
		}
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "append")
}
