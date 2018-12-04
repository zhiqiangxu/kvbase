package kvbase

import (
	"errors"

	farm "github.com/dgryski/go-farm"
)

var (
	// ErrDiscardedTxn is returned if a previously discarded transaction is re-used.
	ErrDiscardedTxn = errors.New("This transaction has been discarded. Create a new one")
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")
	// ErrReadOnlyTxn is returned if an update function is called on a read-only transaction.
	ErrReadOnlyTxn = errors.New("No sets or deletes are allowed in a read-only transaction")
	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("Key cannot be empty")
)

// Txn model
type Txn struct {
	readTs        uint64
	mdb           *DB
	update        bool
	discarded     bool
	pendingWrites map[string][]byte // nil for delete
	reads         []uint64
	writes        []uint64
}

// NewTransaction returns a Txn
func (mdb *DB) NewTransaction(update bool) *Txn {
	txn := &Txn{update: update, mdb: mdb, pendingWrites: make(map[string][]byte)}

	if update {
		txn.mdb.orc.addRef()
	}
	txn.readTs = txn.mdb.orc.currentTs()
	return txn
}

// View for ro txn
func (mdb *DB) View(fn func(txn *Txn) error) error {
	var txn *Txn
	txn = mdb.NewTransaction(false)
	defer txn.Discard()

	return fn(txn)
}

// Update for write txn
func (mdb *DB) Update(fn func(txn *Txn) error) error {
	txn := mdb.NewTransaction(true)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit()
}

// Get value by key
func (txn *Txn) Get(key []byte) ([]byte, error) {
	if txn.discarded {
		return nil, ErrDiscardedTxn
	}

	var finger uint64
	if txn.update {
		if v, ok := txn.pendingWrites[string(key)]; ok {
			if v == nil {
				return nil, ErrKeyNotFound
			}

			return copySlice(v), nil
		}
		finger = farm.Fingerprint64(key)
		txn.reads = append(txn.reads, finger)
	} else {
		finger = farm.Fingerprint64(key)
	}

	v, err := txn.mdb.get(txn, key, finger)
	return v, err
}

// Set key value pair, if value is nil, will delete
func (txn *Txn) Set(key, value []byte) error {
	switch {
	case !txn.update:
		return ErrReadOnlyTxn
	case txn.discarded:
		return ErrDiscardedTxn
	case len(key) == 0:
		return ErrEmptyKey
	}

	fp := farm.Fingerprint64(key)
	txn.writes = append(txn.writes, fp)
	txn.pendingWrites[string(key)] = value

	return nil
}

// Delete the key
func (txn *Txn) Delete(key []byte) error {
	return txn.Set(key, nil)
}

// Discard Txn
func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}
	txn.discarded = true
	if txn.update {
		txn.mdb.orc.decrRef()
	}
}

// Commit Txn
func (txn *Txn) Commit() error {
	defer txn.Discard()

	if len(txn.writes) == 0 {
		return nil
	}

	return txn.mdb.sendToWriteCh(txn)
}
