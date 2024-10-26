package raftrocksdb

import (
	"github.com/hashicorp/raft"
	"github.com/linxGnu/grocksdb"
)

// FirstIndex returns the first index written. 0 for no entries.
func (s *Store) FirstIndex() (uint64, error) {
	return s.getIndex(func(it *grocksdb.Iterator) { it.SeekToFirst() })
}

// LastIndex returns the last index written. 0 for no entries.
func (s *Store) LastIndex() (uint64, error) {
	return s.getIndex(func(it *grocksdb.Iterator) { it.SeekToLast() })
}

func (s *Store) getIndex(seekFn func(it *grocksdb.Iterator)) (uint64, error) {
	it := s.db.NewIterator(s.iterOpts)
	defer it.Close()

	seekFn(it)
	if !it.Valid() {
		return 0, nil
	}

	key := it.Key()
	if !key.Exists() {
		return 0, nil
	}
	defer key.Free()

	if err := it.Err(); err != nil {
		return 0, err
	}

	_, index := decodeLogKey(key.Data())
	return index, nil
}

// GetLog gets a log entry at a given index.
func (s *Store) GetLog(index uint64, log *raft.Log) error {
	key := encodeLogKey(s.id, index)

	slice, err := s.db.Get(s.readOpts, key)
	if err != nil {
		return err
	}
	defer slice.Free()

	if !slice.Exists() {
		return errNotFound
	}

	return decodeLog(slice.Data(), log)
}

// StoreLog stores a log entry.
func (s *Store) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (s *Store) StoreLogs(logs []*raft.Log) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, log := range logs {
		key := encodeLogKey(s.id, log.Index)
		value, err := encodeLog(log)
		if err != nil {
			return err
		}

		batch.Put(key, value)
	}

	return s.db.Write(s.writOpts, batch)
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (s *Store) DeleteRange(min, max uint64) error {
	startKey, endKey := encodeLogKeyRange(s.id, min, max)
	cf := s.db.GetDefaultColumnFamily()

	return s.db.DeleteRangeCF(s.writOpts, cf, startKey, endKey)
}
