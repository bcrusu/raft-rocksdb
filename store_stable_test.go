package raftrocksdb_test

import (
	"bytes"
	"math"
	"testing"

	raftrocksdb "github.com/bcrusu/raft-rocksdb"
)

func TestStore_Set_Get(t *testing.T) {
	store := newStore(t)
	scenarioStore_Set_Get(t, store)
}

func scenarioStore_Set_Get(t *testing.T, store *raftrocksdb.Store) {
	key1 := []byte("key11")
	key2 := []byte("key21")
	val1 := []byte("value11")
	val2 := []byte("value21")

	if val, err := store.Get(key1); err != nil {
		t.Fatal(err)
	} else if len(val) != 0 {
		t.Fatal("expected empty value")
	}

	if err := store.Set(key1, val1); err != nil {
		t.Fatal(err)
	}

	if err := store.Set(key2, val2); err != nil {
		t.Fatal(err)
	}

	if val, err := store.Get(key1); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(val, val1) {
		t.Fatal("get returned and unexpected value for key1")
	}

	if val, err := store.Get(key2); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(val, val2) {
		t.Fatal("get returned and unexpected value for key2")
	}
}

func TestStore_SetUint64_GetUint64(t *testing.T) {
	store := newStore(t)
	scenarioStore_SetUint64_GetUint64(t, store)
}

func scenarioStore_SetUint64_GetUint64(t *testing.T, store *raftrocksdb.Store) {
	key1 := []byte("key12")
	key2 := []byte("key22")
	val1 := uint64(42)
	val2 := uint64(math.MaxUint64)

	if val, err := store.GetUint64(key1); err != nil {
		t.Fatal(err)
	} else if val != 0 {
		t.Fatal("expected 0 value")
	}

	if err := store.SetUint64(key1, val1); err != nil {
		t.Fatal(err)
	}

	if err := store.SetUint64(key2, val2); err != nil {
		t.Fatal(err)
	}

	if val, err := store.GetUint64(key1); err != nil {
		t.Fatal(err)
	} else if val != val1 {
		t.Fatalf("get returned and unexpected value %d for key1", val)
	}

	if val, err := store.GetUint64(key2); err != nil {
		t.Fatal(err)
	} else if val != val2 {
		t.Fatalf("get returned and unexpected value %d for key2", val)
	}
}
