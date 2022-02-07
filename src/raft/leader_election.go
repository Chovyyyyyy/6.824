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
	// 可能为正常情况，比如3个Raft实例刚启动，都处于Follower状态，s0的选举超时时间先耗尽，变为Candidate状态，任期为1发起选举。
	// s1此时任期为0，处于Follower状态，收到s0的RequestVote RPC请求。 这时应该继续正常执行RequestVote RPC处理程序，
	// 检查s0的日志是否"up-to-date"，如果是，则投票给s0。
	// rule 1
	// 如果收到请求的任期比自己的要旧时，那么就认为请求是过时的，返回false，然后直接return
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//可以将voteFor重置为-1，因为既然该peer的rf.currentTerm < args.Term，说明该peer此时还没有给哪个candidate投票，
	//因为一旦它投过票，其任期就会更新为args.Term。
	//所以此时重置voteFor为-1是安全的，往下继续执行处理，仍然可以投票。
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.targetState(TO_FOLLOWER,false)
		rf.persist()
	}
	reply.Term = rf.currentTerm
	//rule 2
	if !rf.UpToDate(args.LastLogIndex,args.LastLogTerm) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// 已经投票给别的server
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

// 每隔选举间隔判断选举是否超时和server的状态
func (rf *Raft) leaderElectionTicker() {
	for !rf.killed() {
		nowTime := time.Now()
		// 在重置选举超时定时器时，需要重新随机化选举选举超时时间electionTimout。
		// 如果不这么做，如果出现若干个follower的electionTimeOut相同，
		// 则它们同时选举超时，同时发起投票，如果它们瓜分了选票；然后选举超时再次发生，
		// 再次同时发起选举，再一次出现选票瓜分，无法选出leader。
		// 为了避免这种情况，应该每次重置选举超时计时器时都重新选取随机化的选举超时时间，以尽量避免选举超时相同的情况。
		time.Sleep(time.Duration(getRand(int64(rf.me)))*time.Millisecond)
		rf.mu.Lock()
		// 判断选举超时时间是否在当前时间之后
		if rf.electionTime.Before(nowTime) && rf.state != LEADER{
			rf.targetState(TO_CANDIDATE,true)
		}
		rf.mu.Unlock()

	}
}
//1. currentTerm，votedFor，log这三个部分是需要持久化的
//2. 需要将server的状态定义为candidate
//3. 需要给自己投一票
//4. 在发起选举的时候，我们要重置选举超时
//5. 这里使用了once这个锁，可以确保请求投票只会发起一次
//6. 对每一个server开启一个goroutine，然后发起投票candidateRequestVote
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