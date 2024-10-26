package raftrocksdb

import (
	"sync"

	"github.com/linxGnu/grocksdb"
)

// MultiStore allows working with multiple Raft instances,
// storing their state into a single RocksDB database.
type MultiStore struct {
	opts     *options
	db       *grocksdb.DB
	readOpts *grocksdb.ReadOptions
	writOpts *grocksdb.WriteOptions
	lock     sync.Mutex
	stores   map[uint32]*Store
}

// NewMultiStore returns a new MultiStore instance.
func NewMultiStore(options ...Option) *MultiStore {
	opts := newOptions()
	for _, fn := range options {
		fn(opts)
	}

	return &MultiStore{
		opts:   opts,
		stores: map[uint32]*Store{},
	}
}

// Open opens the store.
func (s *MultiStore) Open() error {
	db, err := openDB(s.opts)
	if err != nil {
		return err
	}

	s.db = db
	s.readOpts = newReadOptions()
	s.writOpts = newWriteOptions(s.opts.sync)
	return nil
}

// Close closes the store.
func (s *MultiStore) Close() {
	s.db.Close()
	s.readOpts.Destroy()
	s.writOpts.Destroy()

	for _, x := range s.stores {
		x.iterOpts.Destroy()
	}
	clear(s.stores)
}

// Sync syncs the WAL to disk.
func (s *MultiStore) Sync() error {
	return s.db.FlushWAL(true)
}

// New creates a new Store with the provided identifier.
// Successive calls for the same id return the same instance.
func (s *MultiStore) New(id uint32) *Store {
	s.lock.Lock()
	defer s.lock.Unlock()

	store, ok := s.stores[id]

	if !ok {
		store = &Store{
			id:       id,
			opts:     s.opts,
			db:       s.db,
			readOpts: s.readOpts,
			writOpts: s.writOpts,
			iterOpts: newIterReadOptions(id),
			managed:  true,
		}
		s.stores[id] = store
	}

	return store
}

// Drop removes all persisted data for the given Store identifier.
func (s *MultiStore) Drop(id uint32) error {
	s.lock.Lock()

	if x, ok := s.stores[id]; ok {
		x.iterOpts.Destroy()
		delete(s.stores, id)
	}

	s.lock.Unlock()

	startKey, endKey := encodeStoreKeyRange(id)
	cf := s.db.GetDefaultColumnFamily()

	return s.db.DeleteRangeCF(s.writOpts, cf, startKey, endKey)
}
