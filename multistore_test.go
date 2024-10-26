package raftrocksdb_test

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	raftrocksdb "github.com/bcrusu/raft-rocksdb"
)

func TestMultiStore_Serial(t *testing.T) {
	multi := newMultiStore(t)

	ids := []uint32{1010, 1011, 1012, 1013, 1014}
	stores := make([]*raftrocksdb.Store, len(ids))

	for i, id := range ids {
		stores[i] = multi.New(id)
	}

	for _, store := range stores {
		testAllScenarios(t, store)
	}

	for _, id := range ids {
		if err := multi.Drop(id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiStore_Parallel(t *testing.T) {
	multi := newMultiStore(t)

	ids := []uint32{1010, 1011, 1012, 1013, 1014}

	for i, id := range ids {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			store := multi.New(id)

			testAllScenarios(t, store)

			if err := multi.Drop(id); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMultiStore_Reopen(t *testing.T) {
	multi := newMultiStore(t)
	ids := []uint32{1, 2, 3}
	key := []byte("key1")

	for _, id := range ids {
		store := multi.New(id)
		if err := store.SetUint64(key, 1000+uint64(id)); err != nil {
			t.Fatal(err)
		}
	}

	multi.Close()
	if err := multi.Open(); err != nil {
		t.Fatal(err)
	}

	for _, id := range ids {
		store := multi.New(id)
		if val, err := store.GetUint64(key); err != nil {
			t.Fatal(err)
		} else if val != 1000+uint64(id) {
			t.Fatalf("unexpected value %d", val)
		}
	}
}

func newMultiStore(t testing.TB, options ...raftrocksdb.Option) *raftrocksdb.MultiStore {
	path := fmt.Sprintf("./tests_%d", time.Now().UnixNano())

	opts := append([]raftrocksdb.Option{
		raftrocksdb.WithPath(path),
	}, options...)

	store := raftrocksdb.NewMultiStore(opts...)

	if err := store.Open(); err != nil {
		t.Fatal("NewMultiStore failed")
	}

	t.Cleanup(func() {
		store.Close()
		os.RemoveAll(path)
	})

	return store
}

func testAllScenarios(t *testing.T, store *raftrocksdb.Store) {
	for _, scenario := range allScenarios {
		scenario(t, store)
		store.DeleteRange(0, math.MaxUint64)
	}
}
