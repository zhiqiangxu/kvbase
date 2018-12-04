package kvbase

import (
	"sync"

	farm "github.com/dgryski/go-farm"
	"github.com/zhiqiangxu/qrpc"
)

const (
	defaultShardCountPoT = 5
)

// concurrentMap is concurrent safe
type concurrentMap struct {
	shards    []mapShard
	shardMask uint64
	count     int
}

type mapShard struct {
	sync.RWMutex
	kv map[string]entry
}

func newConcurrentMap() *concurrentMap {
	return newConcurrentMapWithCount(defaultShardCountPoT)
}

func newConcurrentMapWithCount(shardCountPoT uint8) *concurrentMap {
	shardCount := 1 << shardCountPoT
	shardMask := uint64(shardCount - 1)
	shards := make([]mapShard, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		shards = append(shards, mapShard{kv: make(map[string]entry)})
	}
	return &concurrentMap{shards: shards, shardMask: shardMask}
}

func (m *concurrentMap) Get(key string) (val entry, ok bool) {
	finger := farm.Fingerprint64(qrpc.Slice(key))
	idx := finger & m.shardMask

	m.shards[idx].RLock()
	defer m.shards[idx].RUnlock()

	val, ok = m.shards[idx].kv[key]

	return
}

func (m *concurrentMap) Set(key string, val entry) (idx uint64, size int) {
	finger := farm.Fingerprint64(qrpc.Slice(key))
	idx = finger & m.shardMask

	m.shards[idx].Lock()
	defer m.shards[idx].Unlock()

	m.shards[idx].kv[key] = val

	size = len(m.shards[idx].kv)

	return
}

func (m *concurrentMap) ShardCount() int {
	return len(m.shards)
}

func (m *concurrentMap) LockShard(idx int) {
	if idx >= len(m.shards) {
		panic("idx out of shard range")
	}

	m.shards[idx].Lock()
}

func (m *concurrentMap) UnlockShard(idx int) {
	if idx >= len(m.shards) {
		panic("idx out of shard range")
	}

	m.shards[idx].Unlock()
}

func (m *concurrentMap) RangeShardLocked(idx int, f func(key string, val entry) bool) {
	if idx >= len(m.shards) {
		panic("idx out of shard range")
	}

	for k, v := range m.shards[idx].kv {
		if !f(k, v) {
			break
		}
	}
}

func (m *concurrentMap) ClearShardLocked(idx int) (cleared int) {
	if idx >= len(m.shards) {
		panic("idx out of shard range")
	}

	cleared = len(m.shards[idx].kv)
	m.shards[idx].kv = make(map[string]entry)
	return
}
