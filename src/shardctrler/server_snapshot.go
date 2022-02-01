package shardctrler

import (
	"6.824-golabs-2021/labgob"
	"bytes"
)

func (sc *ShardCtrler) ReadSnapShotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) <1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistConfig []Config
	var persistLastRequest map[int64]int

	if d.Decode(&persistConfig) == nil && d.Decode(&persistLastRequest) == nil {
		sc.configs = persistConfig
		sc.lastRequestId = persistLastRequest
	}
}
