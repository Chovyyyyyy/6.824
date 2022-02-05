package raft

import "time"

// Append Entries RPC structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingIndex int // optimizer func for find the nextIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	if rf.state != FOLLOWER {
		rf.targetState(TO_FOLLOWER, true)
	} else {
		rf.electionTime = time.Now()
		rf.persist()
	}


	if rf.lastSnapShotIndex > args.PrevLogIndex {
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex() + 1
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex()
		return
	} else {
		if rf.getLogTermWithIndex(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			tempTerm := rf.getLogTermWithIndex(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastSnapShotIndex; index-- {
				if rf.getLogTermWithIndex(index) != tempTerm {
					reply.ConflictingIndex = index + 1
					break
				}
			}
			return
		}
	}

	//rule 3 & rule 4
	rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastSnapShotIndex], args.Entries...)
	rf.persist()


	//rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(FOLLOWER, args.LeaderCommit)
	}
	return
}

// 发送给每台服务器进行日志复制
func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		// 使用goroutine进行并行复制
		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			tempPrevLogIndex := rf.nextIndex[server] - 1
			// 使用快照追加
			if tempPrevLogIndex < rf.lastSnapShotIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			args := AppendEntriesArgs{}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				entriesNeeded := make([]Entry, 0)
				entriesNeeded = append(entriesNeeded, rf.log[rf.nextIndex[server]-rf.lastSnapShotIndex:]...)
				prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
				args = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					entriesNeeded,
					rf.commitIndex,
				}
			} else {
				prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
				args = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					[]Entry{},
					rf.commitIndex,
				}
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(server, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != LEADER {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.targetState(TO_FOLLOWER, true)
					return
				}

				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.updateCommitIndex(LEADER, 0)
				}

				if !reply.Success {
					if reply.ConflictingIndex != -1 {
						rf.nextIndex[server] = reply.ConflictingIndex
					}
				}
			}

		}(index)

	}
}

func (rf *Raft) heartBeatTicker() {
	for !rf.killed() {
		time.Sleep(HEARTBEAT * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}
