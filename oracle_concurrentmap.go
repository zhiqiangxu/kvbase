package mdb

import "sync"

const (
	defaultShardCountPoT = 5
)

// concurrentMap is used for oracle commits
type concurrentMap struct {
	shards    []mapShard
	shardMask uint64
}

type mapShard struct {
	sync.RWMutex
	kv map[uint64]uint64
}

func newConcurrentMap() *concurrentMap {
	return newConcurrentMapWithCount(defaultShardCountPoT)
}

func newConcurrentMapWithCount(shardCountPoT uint8) *concurrentMap {
	shardCount := 1 << shardCountPoT
	shardMask := uint64(shardCount - 1)
	shards := make([]mapShard, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		shards = append(shards, mapShard{kv: make(map[uint64]uint64)})
	}
	return &concurrentMap{shards: shards, shardMask: shardMask}
}

func (m *concurrentMap) Get(key uint64) (val uint64, ok bool) {
	idx := key & m.shardMask

	m.shards[idx].RLock()
	defer m.shards[idx].RUnlock()

	val, ok = m.shards[idx].kv[key]
	return
}

func (m *concurrentMap) Set(key, val uint64) {
	idx := key & m.shardMask

	m.shards[idx].Lock()
	defer m.shards[idx].Unlock()

	m.shards[idx].kv[key] = val
}
