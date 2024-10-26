package raftrocksdb_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	raftrocksdb "github.com/bcrusu/raft-rocksdb"
)

func TestStore_Reopen(t *testing.T) {
	store := newStore(t)
	key := []byte("key111")
	val := uint64(1111)

	if err := store.SetUint64(key, val); err != nil {
		t.Fatal(err)
	}

	store.Close()
	if err := store.Open(); err != nil {
		t.Fatal(err)
	}

	if v, err := store.GetUint64(key); err != nil {
		t.Fatal(err)
	} else if v != val {
		t.Fatalf("unexpected value %d", v)
	}
}

func newStore(t testing.TB, options ...raftrocksdb.Option) *raftrocksdb.Store {
	path := fmt.Sprintf("./tests_%d", time.Now().UnixNano())

	opts := append([]raftrocksdb.Option{
		raftrocksdb.WithPath(path),
	}, options...)

	store := raftrocksdb.NewStore(opts...)

	if err := store.Open(); err != nil {
		t.Fatal("NewStore failed")
	}

	t.Cleanup(func() {
		store.Close()
		os.RemoveAll(path)
	})

	return store
}

var (
	allScenarios = []func(*testing.T, *raftrocksdb.Store){
		scenarioStore_FirstIndex,
		scenarioStore_LastIndex,
		scenarioStore_GetLog,
		scenarioStore_SetLog,
		scenarioStore_SetLogs,
		scenarioStore_DeleteRange,
		scenarioStore_Set_Get,
		scenarioStore_SetUint64_GetUint64,
	}
)
