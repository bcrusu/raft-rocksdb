package raftrocksdb

import (
	"github.com/hashicorp/raft"
	"github.com/linxGnu/grocksdb"
)

var (
	_ raft.LogStore    = (*Store)(nil)
	_ raft.StableStore = (*Store)(nil)
)

// Store implements LogStore and StableStore using RocksDB as backing storage.
type Store struct {
	id       uint32
	opts     *options
	db       *grocksdb.DB
	readOpts *grocksdb.ReadOptions
	writOpts *grocksdb.WriteOptions
	iterOpts *grocksdb.ReadOptions
	managed  bool // managed by MultiStore
}

// NewStore returns a new Store that allows working with a single Raft instance.
func NewStore(options ...Option) *Store {
	opts := newOptions()
	for _, fn := range options {
		fn(opts)
	}

	id := uint32(0)

	return &Store{
		id:      id,
		opts:    opts,
		managed: false,
	}
}

// Open opens the store.
func (s *Store) Open() error {
	if s.managed {
		return nil
	}

	db, err := openDB(s.opts)
	if err != nil {
		return err
	}

	s.db = db
	s.readOpts = newReadOptions()
	s.writOpts = newWriteOptions(s.opts.sync)
	s.iterOpts = newIterReadOptions(s.id)
	return nil
}

// Close closes the store.
func (s *Store) Close() {
	if s.managed {
		return
	}

	s.db.Close()
	s.readOpts.Destroy()
	s.writOpts.Destroy()
	s.iterOpts.Destroy()
}

// Sync syncs the WAL to disk.
func (s *Store) Sync() error {
	return s.db.FlushWAL(true)
}
