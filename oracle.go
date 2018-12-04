package kvbase

import (
	"sync"
	"sync/atomic"
)

type oracle struct {
	refCount     int64
	currentTxnTs uint64
	commits      atomic.Value // map[uint64]uint64
	sync.Mutex
}

func newOracle() (o *oracle) {
	o = &oracle{}
	o.commits.Store(make(map[uint64]uint64))
	return
}

func (o *oracle) addRef() {
	atomic.AddInt64(&o.refCount, 1)
}

func (o *oracle) decrRef() {
	currentTxnTs := o.currentTs()
	if atomic.AddInt64(&o.refCount, -1) != 0 {
		return
	}

	o.Lock()
	defer o.Unlock()

	if atomic.LoadInt64(&o.refCount) != 0 {
		return
	}
	//双保险
	if currentTxnTs != o.currentTxnTs {
		return
	}

	commits := make(map[uint64]uint64)
	o.commits.Store(commits)

}

// hasConflict returns true if not serializable.
func (o *oracle) hasConflict(txn *Txn) bool {

	if len(txn.reads) == 0 {
		return false
	}

	commits := o.commits.Load().(map[uint64]uint64)
	for _, ro := range txn.reads {
		// A commit at the read timestamp is expected.
		// But, any commit after the read timestamp should cause a conflict.
		if ts, has := commits[ro]; has && ts > txn.readTs {
			return true
		}
	}
	return false
}

func (o *oracle) currentTs() uint64 {

	return atomic.LoadUint64(&o.currentTxnTs)

}

func (o *oracle) wrapMutate(f func()) {
	// 这里的竞争非常小，仅当最后一个写事务decrRef到一半，此时新来了一个写事务，并且Commit，才会有竞争
	o.Lock()
	defer o.Unlock()
	f()
}

// advanceCurrentTs updates currentTxnTs and commits
func (o *oracle) advanceCurrentTs(txn *Txn) (ts uint64) {

	o.currentTxnTs++
	ts = o.currentTxnTs

	commits := o.commits.Load().(map[uint64]uint64)
	for _, w := range txn.writes {
		commits[w] = ts // Update the commitTs.
	}

	return ts
}
