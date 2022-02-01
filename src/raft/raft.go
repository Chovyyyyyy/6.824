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
	"sync"
	"time"
)
import "sync/atomic"
import "6.824-golabs-2021/labrpc"

// import "bytes"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
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
	APPLIED_TIMEOUT = 28
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state        int
	heartBeat    time.Duration
	electionTime time.Time

	// 持久性状态
	currentTerm int     // 最新任期
	votedFor    int     // 获得选票的候选人Id
	log         []Entry // 日志条目

	//所有server易失性状态
	commitIndex int // 被提交的最高日志索引
	lastApplied int // 已应用于状态机的最高日志条目索引

	//leader的易失性状态
	nextIndex  []int // 对于每个server，要发送给该server的下一个日志的索引（leader的最后一个日志索引+1）
	matchIndex []int // 对于每个server，在服务器上复制的最新日志的索引

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// SnapShot
	lastSnapShotIndex int
	lastSnapShotTerm  int
}

type Entry struct {
	Term    int
	Command interface{}
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

	if rf.state != LEADER {
		return -1, rf.currentTerm, false
	}
	index := rf.getLastIndex() + 1
	term := rf.currentTerm
	log := Entry{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, log)
	DPrintf("[%v]: term %v Start %v", rf.me, term, log)
	rf.persist()
	return index, term, true

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
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	//rf.heartBeat = 50 * time.Millisecond
	rf.heartBeat = 27 * time.Millisecond

	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = []Entry{}
	rf.log = append(rf.log, Entry{})

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// snapshot
	rf.lastSnapShotIndex = 0
	rf.lastSnapShotTerm = 0

	rf.readPersist(persister.ReadRaftState())
	if rf.lastSnapShotIndex > 0 {
		rf.lastApplied = rf.lastSnapShotIndex
	}

	DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapShotIndex %d , term %d", rf.me, rf.currentTerm, rf.lastSnapShotIndex, rf.lastSnapShotTerm)

	// 使用goroutine进行leader选举
	go rf.ticker()

	go rf.committedToAppliedTicker()

	return rf
}
