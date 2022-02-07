package raft

import (
	"6.824/labgob"
	"bytes"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) GetRaftStateSize() int{
	return rf.persister.RaftStateSize()
}

func (rf *Raft) persistData() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSnapShotIndex)
	e.Encode(rf.lastSnapShotTerm)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

// 恢复持久化状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var persist_currentTrem int
	var persist_voteFor int
	var persist_log []Entry
	var persist_lastSSPointIndex int
	var persist_lastSSPointTerm int

	if d.Decode(&persist_currentTrem) != nil ||
		d.Decode(&persist_voteFor) != nil ||
		d.Decode(&persist_log) != nil ||
		d.Decode(&persist_lastSSPointIndex) != nil ||
		d.Decode(&persist_lastSSPointTerm) != nil {
	} else {
		rf.currentTerm = persist_currentTrem
		rf.votedFor = persist_voteFor
		rf.log = persist_log
		rf.lastSnapShotIndex = persist_lastSSPointIndex
		rf.lastSnapShotTerm = persist_lastSSPointTerm
	}
}