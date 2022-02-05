package raft

import "time"

type InstallSnapshotArgs struct{
	Term int
	LeaderId int
	LastIncludeIndex int
	LastIncludeTerm int
	Data[] byte
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastSnapShotIndex >= index || index > rf.commitIndex{
		return
	}
	tempLog := make([]Entry,0)
	tempLog = append(tempLog,Entry{})

	for i := index+1;i<=rf.getLastIndex();i++ {
		tempLog = append(tempLog,rf.getLogWithIndex(i))
	}

	if index == rf.getLastIndex()+1 {
		rf.lastSnapShotTerm = rf.getLastTerm()
	}else {
		rf.lastSnapShotTerm = rf.getLogTermWithIndex(index)
	}

	rf.lastSnapShotIndex = index

	rf.log = tempLog
	if index > rf.commitIndex{
		rf.commitIndex = index
	}
	if index > rf.lastApplied{
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(),snapshot)
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.mu.Lock()
	if rf.currentTerm > args.Term{
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term
	if rf.state != FOLLOWER {
		rf.targetState(TO_FOLLOWER,true)
	}else{
		rf.electionTime = time.Now()
		rf.persist()
	}

	if rf.lastSnapShotIndex >= args.LastIncludeIndex{
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludeIndex
	tempLog := make([]Entry,0)
	tempLog = append(tempLog,Entry{})

	for i := index+1;i<=rf.getLastIndex();i++ {
		tempLog = append(tempLog,rf.getLogWithIndex(i))
	}

	rf.lastSnapShotTerm = args.LastIncludeTerm
	rf.lastSnapShotIndex = args.LastIncludeIndex
	rf.log = tempLog

	if index > rf.commitIndex{
		rf.commitIndex = index
	}
	if index > rf.lastApplied{
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(),args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotTerm: rf.lastSnapShotTerm,
		SnapshotIndex: rf.lastSnapShotIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg

}


func (rf *Raft) leaderSendSnapShot(server int){
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastSnapShotIndex,
		rf.lastSnapShotTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.sendSnapShot(server,&args,&reply)

	if ok {
		rf.mu.Lock()
		if rf.state!=LEADER || rf.currentTerm!= args.Term{
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm{
			rf.targetState(FOLLOWER,true)
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1
		rf.mu.Unlock()
		return
	}
}

