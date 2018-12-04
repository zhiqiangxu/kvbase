package kvbase

import (
	"errors"
	"qpush/pkg/logger"
	"sync"
	"sync/atomic"
	"time"

	kitmetrics "github.com/go-kit/kit/metrics"
	"github.com/zhiqiangxu/qrpc"

	"github.com/dgraph-io/badger"
)

var (
	// ErrFlushing when flushing
	ErrFlushing = errors.New("flushing, please retry")
	// ErrClosed when op on closed
	ErrClosed = errors.New("closed")
	// ErrConflict when txn not serializable
	ErrConflict = errors.New("conflict, please retry")
)

type state int32

const (
	// Running is default state
	Running state = iota
	// Flushing will STW
	Flushing
	// Closed will fail writes
	Closed
)
const (
	flushSize = 10000
)

// ***关于事务(Txn)、写缓存(WB)、磁盘、读缓存(RB)的综述***
// 事务隔离等级为最严格的可序列化，即：
// 		Txn中读的每个key都假定从Txn开始(currentTxnTs)就没变化，不满足这一点，则Txn提交会失败
// 写写并发
//		引擎会顺序地从Txn的队列里拿出Txn依次执行（也可以通过两阶段锁协议,但是会涉及到等待）
// 读写并发
// 		MVCC，无锁
// 			1.为每一个写（更新）事务分配一个自增的序列号currentTxnTs
//			2.为每一个读请求分配一个已完成的最大写事务序列号readTs
// 			3.读取时对于比readTs大的版本不可见
//
// WB实际是内存数据库，只不过会定时回刷磁盘，以减少内存压力；同步磁盘过程中所有写操作都暂停，直到同步完成
// 以上流程保证了没有RB的情况下的正确性
//
// 读缓存在从磁盘读回时生成，待续

// DB model
type DB struct {
	db    *badger.DB
	lck   sync.RWMutex
	value map[string]entry
	// cache   map[string][]byte // TODO replace with lru
	orc          *oracle
	wg           sync.WaitGroup
	writeCh      chan *request
	closeCh      chan struct{}
	state        int32
	flushCh      chan struct{}
	flushLatency kitmetrics.Histogram
	writeLatency kitmetrics.Histogram
}

type entry struct {
	data    []byte
	version uint64
}

type request struct {
	txn    *Txn
	respCh chan error
}

// New creates DB
func New(db *badger.DB, flushLatency kitmetrics.Histogram, writeLatency kitmetrics.Histogram) *DB {
	mdb := &DB{
		db:    db,
		value: make(map[string]entry), orc: newOracle(),
		writeCh: make(chan *request, 1000), flushLatency: flushLatency,
		closeCh: make(chan struct{}), flushCh: make(chan struct{}),
		writeLatency: writeLatency}
	qrpc.GoFunc(&mdb.wg, mdb.flush)
	qrpc.GoFunc(&mdb.wg, mdb.doWrite)
	return mdb
}

// if s is nil, will return nil
func copySlice(s []byte) []byte {
	if s == nil {
		return nil
	}

	b := make([]byte, len(s))
	copy(b, s)
	return b
}

func (mdb *DB) get(txn *Txn, key []byte, finger uint64) (ret []byte, err error) {
	state := state(atomic.LoadInt32(&mdb.state))
	switch state {
	case Closed:
		return nil, ErrClosed
	}

	defer func() {
		if err == nil && ret == nil {
			ret, err = nil, ErrKeyNotFound
		}
	}()

	mdb.lck.RLock()

	value, ok := mdb.getFromMemLocked(txn, key)
	if ok {
		mdb.lck.RUnlock()
		return copySlice(value), nil
	}

	mdb.lck.RUnlock()

	err = RetryUntilSuccess(maxRetry, retryWait, "mdb.db.Update", func() error {
		return mdb.db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get(key)

			if err != nil {
				if err == badger.ErrKeyNotFound {
					return nil
				}
				return err
			}

			ret, err = item.ValueCopy(nil)
			return err
		})
	})

	if err != nil {
		logger.Error("mdb.db.Update", err)
		return
	}

	return
}

func (mdb *DB) getFromMemLocked(txn *Txn, key []byte) ([]byte, bool) {
	keyStr := qrpc.String(key)
	value, ok := mdb.value[keyStr]
	if ok {
		if txn.readTs < value.version {
			return nil, false
		}
		return value.data, true
	}
	// value, ok = mdb.cache[key]
	// if ok {
	// 	return value, true
	// }

	return nil, false
}

func (mdb *DB) sendToWriteCh(txn *Txn) error {
	state := state(atomic.LoadInt32(&mdb.state))
	switch state {
	case Closed:
		return ErrClosed
	}

	req := &request{txn: txn, respCh: make(chan error)}
	select {
	case mdb.writeCh <- req:
	case <-mdb.closeCh:
		return ErrClosed
	}
	return <-req.respCh
}

func (mdb *DB) doWrite() {
	writeLatency := mdb.writeLatency
	defer func() {
		if err := recover(); err != nil {
			logger.Error("doWrite err", err)
		}
	}()
	for {
		select {
		case req := <-mdb.writeCh:
			if mdb.orc.hasConflict(req.txn) {
				req.respCh <- ErrConflict
				break
			}

			newTs := mdb.orc.currentTxnTs + 1
			var toFlush bool

			mdb.orc.wrapMutate(func() {
				mdb.lck.Lock()
				start := time.Now()
				for k, v := range req.txn.pendingWrites {
					mdb.value[k] = entry{data: copySlice(v), version: newTs}
				}

				if len(mdb.value) > flushSize {
					toFlush = true
				} else {
					toFlush = false
				}
				mdb.lck.Unlock()
				writeLatency.Observe(time.Now().Sub(start).Seconds())

				mdb.orc.advanceCurrentTs(req.txn)
			})

			req.respCh <- nil

			if toFlush {
				select {
				case mdb.flushCh <- struct{}{}:
				case <-mdb.closeCh:
					mdb.cancelPendingWrites()
					return
				default: //flushing
				}
			}
		case <-mdb.closeCh:
			mdb.cancelPendingWrites()
			return
		}
	}
}

func (mdb *DB) cancelPendingWrites() {
	for {
		select {
		case req := <-mdb.writeCh:
			req.respCh <- ErrClosed
		case <-time.After(time.Millisecond * 100):
			//enough for the gap:
			// writeCh <- req
			// req.respCh
			return
		}
	}
}

func (mdb *DB) flush() {
	defer func() {
		if err := recover(); err != nil {
			logger.Error("flush err", err)
		}
	}()
	for {
		select {
		case <-mdb.closeCh:
			// full flush then exit
			mdb.flushOnce(true)
			return
		case <-mdb.flushCh:
			mdb.flushOnce(false)
		case <-time.After(time.Minute):
			mdb.flushOnce(false)
		}
	}
}

// flushOnce will STW
// TODO metric
func (mdb *DB) flushOnce(exit bool) {
	atomic.CompareAndSwapInt32(&mdb.state, int32(Running), int32(Flushing))

	mdb.lck.Lock()
	count := len(mdb.value)
	start := time.Now()
	defer func() {
		mdb.lck.Unlock()
		atomic.CompareAndSwapInt32(&mdb.state, int32(Flushing), int32(Running))
		duration := time.Now().Sub(start)
		logger.Info("flushOnce took", duration.String(), "count", count)
		mdb.flushLatency.Observe(duration.Seconds())
		logger.Info("flushOnce done")
	}()

	batch := mdb.db.NewWriteBatch()
	defer batch.Cancel()
	var err error
	for k, v := range mdb.value {
		if v.data == nil {
			err = batch.Delete([]byte(k))
		} else {
			err = batch.Set([]byte(k), v.data, 0)
		}

		if err != nil {
			logger.Error("batch.Set", err, "v", v)
		}
	}
	err = batch.Flush()
	if err != nil {
		logger.Error("batch.Flush", err)
	}

	mdb.value = make(map[string]entry)
}

// Close Mdb, call twice will panic
func (mdb *DB) Close() error {

	atomic.StoreInt32(&mdb.state, int32(Closed))
	close(mdb.closeCh)
	mdb.wg.Wait()
	return mdb.db.Close()

}
