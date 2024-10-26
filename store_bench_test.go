package raftrocksdb_test

import (
	"testing"

	raftbench "github.com/hashicorp/raft/bench"
)

func BenchmarkStore_FirstIndex(b *testing.B) {
	store := newStore(b)
	raftbench.FirstIndex(b, store)
}

func BenchmarkStore_LastIndex(b *testing.B) {
	store := newStore(b)
	raftbench.LastIndex(b, store)
}

func BenchmarkStore_GetLog(b *testing.B) {
	store := newStore(b)
	raftbench.GetLog(b, store)
}

func BenchmarkStore_StoreLog(b *testing.B) {
	store := newStore(b)
	raftbench.StoreLog(b, store)
}

func BenchmarkStore_StoreLogs(b *testing.B) {
	store := newStore(b)
	raftbench.StoreLogs(b, store)
}

func BenchmarkStore_DeleteRange(b *testing.B) {
	store := newStore(b)
	raftbench.DeleteRange(b, store)
}

func BenchmarkStore_Set(b *testing.B) {
	store := newStore(b)
	raftbench.Set(b, store)
}

func BenchmarkStore_Get(b *testing.B) {
	store := newStore(b)
	raftbench.Get(b, store)
}

func BenchmarkStore_SetUint64(b *testing.B) {
	store := newStore(b)
	raftbench.SetUint64(b, store)
}

func BenchmarkStore_GetUint64(b *testing.B) {
	store := newStore(b)
	raftbench.GetUint64(b, store)
}
