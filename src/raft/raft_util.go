package raft

import (

	"math/rand"
	"time"
)

// change the raft server state and do something init
func (rf *Raft) targetState(target int, resetTime bool) {

	//可以将voteFor重置为-1，因为既然该peer的rf.currentTerm < args.Term，说明该peer此时还没有给哪个candidate投票，
	//因为一旦它投过票，其任期就会更新为args.Term。
	//所以此时重置voteFor为-1是安全的，往下继续执行处理，仍然可以投票。
	if target == TO_FOLLOWER {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.getVoteNum = 0
		rf.persist()
		if resetTime {
			rf.electionTime = time.Now()
		}
	}

	if target == TO_CANDIDATE {
		rf.state = CANDIDATE
		rf.votedFor = rf.me
		rf.getVoteNum = 1
		rf.currentTerm ++
		rf.persist()
		rf.leaderElection()
		rf.electionTime = time.Now()
	}

	if target == TO_LEADER {
		rf.state = LEADER
		rf.votedFor = -1
		rf.getVoteNum = 0
		rf.persist()

		rf.nextIndex = make([]int,len(rf.peers))
		for i:=0;i<len(rf.peers);i++ {
			rf.nextIndex[i] = rf.getLastIndex()+1
		}
		rf.matchIndex = make([]int,len(rf.peers))
		rf.matchIndex[rf.me] = rf.getLastIndex()
		rf.electionTime = time.Now()
		// 最后并行向每个follower发送心跳包，宣布自己胜出，并防止重新选举
		//go rf.leaderAppendEntries()
	}
}

func getRand(server int64) int{
	rand.Seed(time.Now().Unix()+server)
	return rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN)+ELECTION_TIMEOUT_MIN
}

func (rf* Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf* Raft) getLogWithIndex(globalIndex int) Entry{

	return rf.log[globalIndex - rf.lastSnapShotIndex]
}

func (rf* Raft) getLogTermWithIndex(globalIndex int) int{
	if globalIndex - rf.lastSnapShotIndex == 0{
		return rf.lastSnapShotTerm
	}
	return rf.log[globalIndex - rf.lastSnapShotIndex].Term
}

func (rf *Raft) getLastIndex() int{
	return len(rf.log) - 1 + rf.lastSnapShotIndex
}

func (rf *Raft) getLastTerm() int{
	if len(rf.log)-1 == 0{
		return rf.lastSnapShotTerm
	}else{
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) getPrevLogInfo(server int) (int,int){
	newEntryBeginIndex := rf.nextIndex[server]-1

	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex ,rf.getLogTermWithIndex(newEntryBeginIndex)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}