package raft

// InstallSnapshotArgs 快照请求
type InstallSnapshotArgs struct{
	Term int
	LeaderId int
	LastIncludeIndex int
	LastIncludeTerm int
	Data[] byte
}

// InstallSnapshotReply 快照回复
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
	// snapshot the entrier form 1:index(global)
	tempLog := make([]Entry,0)
	tempLog = append(tempLog,Entry{})

	for i := index+1;i<=rf.getLastIndex();i++ {
		tempLog = append(tempLog,rf.getLogWithIndex(i))
	}

	// TODO fix it in lab 4
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
	DPrintf("[SnapShot]Server %d sanpshot until index %d, term %d, loglen %d",rf.me,index,rf.lastSnapShotTerm,len(rf.log)-1)
	// 持久化保存快照
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
		rf.changeState(FOLLOWER,true)
	}else{
		rf.resetElectionTimer()
		rf.persist()
	}

	if rf.lastSnapShotIndex >= args.LastIncludeIndex{
		DPrintf("[HaveSnapShot] sever %d , lastSnapShotIndex %d, leader's lastIncludeIndex %d",rf.me,rf.lastSnapShotIndex,args.LastIncludeIndex)
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
	DPrintf("[FollowerInstallSnapShot]server %d installsnapshot from leader %d, index %d",rf.me,args.LeaderId,args.LastIncludeIndex)

}


// leaderSendSnapShot leader发送快照
func (rf *Raft) leaderSendSnapShot(server int){
	rf.mu.Lock()
	DPrintf("[LeaderSendSnapShot]Leader %d (term %d) send snapshot to server %d, index %d",rf.me,rf.currentTerm,server,rf.lastSnapShotIndex)
	// 构造args
	ssArgs := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastSnapShotIndex,
		rf.lastSnapShotTerm,
		rf.persister.ReadSnapshot(),
	}
	ssReply := InstallSnapshotReply{}
	rf.mu.Unlock()

	// 通过rpc发送快照
	ok := rf.sendSnapShot(server,&ssArgs,&ssReply)

	// 如果接收不到reply
	if !ok {
		DPrintf("[InstallSnapShot ERROR] Leader %d can't receive from %d",rf.me,server)
	}
	if ok {
		rf.mu.Lock()
		if rf.state!=LEADER || rf.currentTerm!=ssArgs.Term{
			rf.mu.Unlock()
			return
		}
		// 说明leader落后了，可能是因为宕机
		if ssReply.Term > rf.currentTerm{
			rf.changeState(FOLLOWER,true)
			rf.mu.Unlock()
			return
		}

		DPrintf("[InstallSnapShot SUCCESS] Leader %d from sever %d",rf.me,server)
		rf.nextIndex[server] = ssArgs.LastIncludeIndex + 1
		rf.matchIndex[server] = ssArgs.LastIncludeIndex
		rf.mu.Unlock()
		return
	}
}