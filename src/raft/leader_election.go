package raft

import (
	"math/rand"
	"sync"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate的任期
	CandidateId  int // candidate的Id
	LastLogIndex int // candidate最新日志的索引
	LastLogTerm  int // candidate最新日志的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期
	VoteGranted bool // true代表candidate获得选票
}

// GetState 查看当前peer是否leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm
	upToDate := args.LastLogTerm > rf.getLastTerm() ||
		(args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex >= rf.getLastIndex())

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.Term == reply.Term && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
	}else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}


// leaderElection 发起选举
func (rf *Raft) leaderElection() {
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
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
		rf.setNewTerm(reply.Term)
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
			//DPrintf("[%d]: 当前term %d 结束\n", rf.me, rf.currentTerm)
			rf.state = LEADER
			lastLogIndex := rf.getLastIndex()
			//选举后重新初始化
			for i, _ := range rf.peers {
				//对于每个服务器
				//要发送给该服务器的下一个日志条目索引
				//初始化为leader的最后一个日志索引+1
				rf.nextIndex[i] = lastLogIndex + 1
				//对于每个服务器
				//已知在服务器上复制的最高日志条目的索引
				//初始化为0
				rf.matchIndex[i] = 0
			}
			//当选leader之后要立刻发送heartbeat
			rf.changeState(LEADER,true)
		})
	}
}

// ticker 判断当前server是leader还是超时选举状态
func (rf *Raft) ticker() {
	for !rf.killed()  {
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else if time.Now().After(rf.electionTime){
			rf.mu.Unlock()
			rf.leaderElection()
		}else {
			rf.mu.Unlock()
		}

	}
}


// resetElectionTimer 重置选举时间
func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	// 150 到 300 毫秒之间的选举超时
	//electionTimeout := time.Duration(150 + rand.Intn(150)) * time.Millisecond
	electionTimeout := time.Duration(50 + rand.Intn(50)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

func (rf *Raft) setNewTerm(term int) {
	// 如果server的term大于candidate的term或则candidate的term等于0
	if term > rf.currentTerm || rf.currentTerm == 0 {
		//重新回到follower节点的状态
		rf.state = FOLLOWER
		rf.currentTerm = term
		rf.votedFor = -1
		DPrintf("[%d]: set term %v\n", rf.me, rf.currentTerm)
		rf.persist()
	}
}

