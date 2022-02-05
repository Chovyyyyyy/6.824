package raft

import (
	"time"
)

func (rf *Raft) committedToAppliedTicker(){
	// put the committed entry to apply on the state machine
	for !rf.killed(){
		time.Sleep(APPLIED_TIMEOUT*time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg,0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied ++
			Messages = append(Messages,ApplyMsg{
				CommandValid: true,
				SnapshotValid: false,
				CommandIndex: rf.lastApplied,
				Command: rf.getLogWithIndex(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _,messages := range Messages{
			rf.applyCh<-messages
		}
	}

}

func (rf *Raft) updateCommitIndex(role int,leaderCommit int){

	if role != LEADER{
		if leaderCommit > rf.commitIndex {
			lastNewIndex := rf.getLastIndex()
			if leaderCommit >= lastNewIndex{
				rf.commitIndex = lastNewIndex
			}else{
				rf.commitIndex = leaderCommit
			}
		}
		return
	}

	if role == LEADER{
		rf.commitIndex = rf.lastSnapShotIndex
		for index := rf.getLastIndex();index>=rf.lastSnapShotIndex+1;index--{
			sum := 0
			for i := 0;i<len(rf.peers);i++ {
				if i == rf.me{
					sum += 1
					continue
				}
				if rf.matchIndex[i] >= index {
					sum += 1
				}
			}

			//log.Printf("lastSSP:%d, index: %d, commitIndex: %d, lastIndex: %d",rf.lastSnapShotIndex, index, rf.commitIndex, rf.getLastIndex())
			if sum >= len(rf.peers)/2+1 && rf.getLogTermWithIndex(index)==rf.currentTerm {
				rf.commitIndex = index
				break
			}

		}
		DPrintf("[CommitIndex] Leader %d(term%d) commitIndex %d",rf.me,rf.currentTerm,rf.commitIndex)
		return
	}

}