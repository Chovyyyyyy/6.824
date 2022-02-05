package kvraft

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"
import mathrand "math/rand"


type Clerk struct {
	servers []*labrpc.ClientEnd
	clientId int64
	requestId int
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

	ck.requestId++
	requestId := ck.requestId
	server := ck.recentLeaderId
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: requestId}

	for {
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader{
			server = (server+1)%len(ck.servers)
			continue
		}

		if reply.Err == ErrNoKey {
			return ""
		}
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
	ck.requestId++
	requestId := ck.requestId
	server := ck.recentLeaderId
	for {
		args := PutAppendArgs{Key: key, Value: value, Opreation : opreation, ClientId: ck.clientId, RequestId: requestId}
		reply := PutAppendReply{}

		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader{
			server = (server+1)%len(ck.servers)
			continue
		}
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
