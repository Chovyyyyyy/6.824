package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labrpc"
	"sync"
	//	"bytes"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type ApplyMsg struct {
	// 如果是日志就设置为true
	// 在lab3中会有其他类型消息，设置为false
	CommandValid bool
	Command      interface{}
	CommandIndex int

	//为lab2D snapshot添加
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER     = 0
	CANDIDATE    = 1
	LEADER       = 2
	TO_FOLLOWER  = 0
	TO_CANDIDATE = 1
	TO_LEADER    = 2

	ELECTION_TIMEOUT_MAX = 250
	ELECTION_TIMEOUT_MIN = 150
	HEARTBEAT            = 40
	APPLIED_TIMEOUT      = 28
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 持久性状态
	currentTerm int     // 最新任期
	votedFor    int     // 获得选票的候选人Id
	log         []Entry // 日志条目
	getVoteNum  int

	//所有server易失性状态
	commitIndex int // 被提交的最高日志索引
	lastApplied int // 已应用于状态机的最高日志条目索引

	state        int
	electionTime time.Time

	//leader的易失性状态
	nextIndex  []int // 对于每个server，要发送给该server的下一个日志的索引（leader的最后一个日志索引+1）
	matchIndex []int // 对于每个server，在服务器上复制的最新日志的索引

	applyCh chan ApplyMsg
	// SnapShot
	lastSnapShotIndex int
	lastSnapShotTerm  int
}

type Entry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return -1, -1, false
	}
	if rf.state != LEADER {
		return -1, -1, false
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		rf.log = append(rf.log, Entry{Term: term, Command: command})
		DPrintf("[StartCommand] Leader %d get command %v,index %d", rf.me, command, index)
		rf.persist()
		return index, term, true
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.getVoteNum = 0
	rf.votedFor = -1


	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.log = []Entry{}
	rf.log = append(rf.log, Entry{})

	rf.applyCh = applyCh

	rf.lastSnapShotIndex = 0
	rf.lastSnapShotTerm = 0
	rf.mu.Unlock()

	// 从崩溃前的持久化状态进行恢复
	rf.readPersist(persister.ReadRaftState())
	if rf.lastSnapShotIndex > 0 {
		rf.lastApplied = rf.lastSnapShotIndex
	}

	// 使用goroutine进行leader选举和发送心跳包
	go rf.leaderElectionTicker()

	go rf.heartBeatTicker()

	go rf.committedToAppliedTicker()

	return rf
}
