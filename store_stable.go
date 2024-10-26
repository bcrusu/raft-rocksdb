package raftrocksdb

import (
	"slices"
)

// Set is used to set a key/value set outside of the raft log
func (s *Store) Set(key, value []byte) error {
	k := encodeStableKey(s.id, key)
	return s.db.Put(s.writOpts, k, value)
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (s *Store) Get(key []byte) ([]byte, error) {
	k := encodeStableKey(s.id, key)
	slice, err := s.db.Get(s.readOpts, k)
	if err != nil {
		return nil, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return []byte{}, nil
	}

	data := slices.Clone(slice.Data())
	return data, nil
}

// SetUint64 is like Set, but handles uint64 values.
func (s *Store) SetUint64(key []byte, val uint64) error {
	return s.Set(key, encodeUint64(val))
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (s *Store) GetUint64(key []byte) (uint64, error) {
	data, err := s.Get(key)
	if err != nil {
		return 0, err
	} else if len(data) == 0 {
		return 0, nil
	}

	return decodeUint64(data)
}
