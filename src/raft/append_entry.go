package raft

// Append Entries RPC structure
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictingIndex int // optimizer func for find the nextIndex
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[GetHeartBeat]Sever %d, from Leader %d(term %d), lastIndex %d, leader.preIndex %d",rf.me,args.LeaderId,args.Term,rf.getLastIndex(),args.PrevLogIndex)

	// rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictingIndex = -1
		return
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ConflictingIndex = -1

	if rf.state!=FOLLOWER{
		rf.changeState(FOLLOWER,true)
	}else{
		rf.resetElectionTimer()
		rf.persist()
	}

	// 出现冲突
	if rf.lastSnapShotIndex > args.PrevLogIndex{
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex() + 1
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex()
		DPrintf("[AppendEntries ERROR1]Sever %d ,prevLogIndex %d,Term %d, rf.getLastIndex %d, rf.getLastTerm %d, Conflicting %d",rf.me,args.PrevLogIndex,args.PrevLogTerm,rf.getLastIndex(),rf.getLastTerm(),reply.ConflictingIndex)
		return
	} else {
		if rf.getLogTermWithIndex(args.PrevLogIndex) != args.PrevLogTerm{
			reply.Success = false
			tempTerm := rf.getLogTermWithIndex(args.PrevLogIndex)
			for index := args.PrevLogIndex;index >= rf.lastSnapShotIndex;index--{
				if rf.getLogTermWithIndex(index) != tempTerm{
					reply.ConflictingIndex = index+1
					DPrintf("[AppendEntries ERROR2]Sever %d ,prevLogIndex %d,Term %d, rf.getLastIndex %d, rf.getLastTerm %d, Conflicting %d",rf.me,args.PrevLogIndex,args.PrevLogTerm,rf.getLastIndex(),rf.getLastTerm(),reply.ConflictingIndex)
					break
				}
			}
			return
		}
	}

	//rule 3 & rule 4
	rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastSnapShotIndex],args.Entries...)
	rf.persist()


	//rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(FOLLOWER, args.LeaderCommit)
	}
	DPrintf("[FinishHeartBeat]Server %d, from leader %d(term %d), me.lastIndex %d",rf.me,args.LeaderId,args.Term,rf.getLastIndex())
	return
}

// leaderAppendEntries
func (rf *Raft) leaderAppendEntries(){
	// 向每个peer发送日志复制
	for idx := range rf.peers{
		if idx == rf.me {
			continue
		}
		// 并行复制
		go func(server int){
			rf.mu.Lock()
			if rf.state!=LEADER{
				rf.mu.Unlock()
				return
			}
			prevLogIndexTemp := rf.nextIndex[server]-1

			// 如果leader给follow发送的日志Idx小于快照，则发送快照
			if prevLogIndexTemp < rf.lastSnapShotIndex{
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			// 普通日志
			Args := AppendEntriesArgs{}
			if rf.getLastIndex() >= rf.nextIndex[server] {
				DPrintf("[LeaderAppendEntries]Leader %d (term %d) to server %d, index %d %d",rf.me,rf.currentTerm,server,rf.nextIndex[server],rf.getLastIndex())
				entries := make([]Entry,0)
				entries = append(entries,rf.log[rf.nextIndex[server]-rf.lastSnapShotIndex:]...)
				prevLogIndex,prevLogTerm := rf.getPrevLogInfo(server)
				Args = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					entries,
					rf.commitIndex,
				}
			}else {
				prevLogIndex,prevLogTerm := rf.getPrevLogInfo(server)
				Args = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					[] Entry{},
					rf.commitIndex,
				}
			}
			Reply := AppendEntriesReply{}
			rf.mu.Unlock()

			// 通过rpc请求调用
			ok := rf.sendAppendEntries(server,&Args,&Reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state!=LEADER{
					return
				}

				if Reply.Term > rf.currentTerm {
					rf.currentTerm = Reply.Term
					rf.changeState(FOLLOWER,true)
					return
				}

				DPrintf("[HeartBeatGetReturn] Leader %d (term %d) ,from Server %d, prevLogIndex %d",rf.me,rf.currentTerm,server,Args.PrevLogIndex)

				if Reply.Success {
					DPrintf("[HeartBeat SUCCESS] Leader %d (term %d) ,from Server %d, prevLogIndex %d",rf.me,rf.currentTerm,server,Args.PrevLogIndex)
					rf.matchIndex[server] = Args.PrevLogIndex + len(Args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.updateCommitIndex(LEADER, 0)
				}

				if !Reply.Success {
					// 如果提交时报并且冲突idx不为0的话，回退到提交的idx
					if Reply.ConflictingIndex!= -1 {
						DPrintf("[HeartBeat CONFLICT] Leader %d (term %d) ,from Server %d, prevLogIndex %d, Confilicting %d",rf.me,rf.currentTerm,server,Args.PrevLogIndex,Reply.ConflictingIndex)
						rf.nextIndex[server] = Reply.ConflictingIndex
					}
				}
			}

		}(idx)
	}
}
