package raft

import (
	"sync"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}


type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rule 1 ------------
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.targetState(TO_FOLLOWER,false)
		rf.persist()
	}
	reply.Term = rf.currentTerm
	//rule 2 ------------
	if !rf.UpToDate(args.LastLogIndex,args.LastLogTerm) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.electionTime = time.Now()
		rf.persist()
		return
	}

	return

}


func (rf *Raft) leaderElectionTicker() {
	for !rf.killed() {
		nowTime := time.Now()
		time.Sleep(time.Duration(getRand(int64(rf.me)))*time.Millisecond)
		rf.mu.Lock()
		// 判断选举超时时间是否在当前时间之后
		if rf.electionTime.Before(nowTime) && rf.state != LEADER{
			rf.targetState(TO_CANDIDATE,true)
		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) leaderElection() {
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.persist()
	rf.electionTime = time.Now()
	term := rf.currentTerm
	voteCounter := 1
	DPrintf("[%v]: start leader election, term %d\n", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
	var once sync.Once
	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			go rf.candidateRequestVote(serverId, &args, &voteCounter, &once)
		}
	}
}

// candidateRequestVote 选举请求
func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, voteCounter *int, once *sync.Once) {
	//DPrintf("[%d]: term %v send vote request to %d\n", rf.me, args.Term, serverId)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//论文5.1
	//如果RPC请求或响应包含任期 T > currentTerm：设置currentTerm = T，转换为follower
	if reply.Term > args.Term {
		//DPrintf("[%d]: %d 在新的term，更新term，结束\n", rf.me, serverId)
		if rf.currentTerm < reply.Term{
			rf.currentTerm = reply.Term
		}
		rf.targetState(TO_FOLLOWER,false)
		return
	}
	//如果返回的term小于candidate的term，则返回
	if reply.Term < args.Term {
		//DPrintf("[%d]: %d 的term %d 已经失效，结束\n", rf.me, serverId, reply.Term)
		return
	}
	// 如果没有获得选票，则返回
	if !reply.VoteGranted {
		//DPrintf("[%d]: %d 没有投给me，结束\n", rf.me, serverId)
		return
	}
	// 如果candidate和当前节点的日志一样新，则投票
	//DPrintf("[%d]: from %d term一致，且投给%d\n", rf.me, serverId, rf.me)
	*voteCounter++
	//如果收到超过半数投票，并且candidate的term没有落后于最新（没有回到follower），当选为leader
	if *voteCounter > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == CANDIDATE {
		//DPrintf("[%d]: 获得多数选票，可以提前结束\n", rf.me)
		once.Do(func() {
			rf.targetState(TO_LEADER,true)
		})
	}
}