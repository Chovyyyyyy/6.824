package raft

import (
	"time"
)

// 定期提交
func (rf *Raft) applier(){
	// put the committed entry to apply on the state machine
	for !rf.killed(){
		time.Sleep(APPLIED_TIMEOUT*time.Millisecond)
		rf.mu.Lock()

		// 如果最后的applied比commitIndex更新，则返回
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg,0)
		// 追加需要提交的部分
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

		// 把message添加到channel当中
		for _,messages := range Messages{
			rf.applyCh<-messages
		}
	}

}

// 更新commitIndex
func (rf *Raft) updateCommitIndex(role int,leaderCommit int){

	if role != LEADER{
		// 如果leaderCommit要比follower的commitIndex要新
		if leaderCommit > rf.commitIndex {
			// 判断最后的Index是否比leaderCommit要新
			// 如果leader的更新，我们先要提交lastIndex
			lastNewIndex := rf.getLastIndex()
			if leaderCommit >= lastNewIndex{
				rf.commitIndex = lastNewIndex
			}else{
				rf.commitIndex = leaderCommit
			}
		}
		return
	}

	// 按照Figure2 的 Rules for leader最后一条
	// 如果存在一个N，使得N>commitIndex，大多数的matchIndex[i]≥N，
	// 并且log[N].term == currentTerm：设置commitIndex = N
	if role == LEADER{
		rf.commitIndex = rf.lastSnapShotIndex
		// 从index到快照，找到冲突term
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