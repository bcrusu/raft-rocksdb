package raftrocksdb_test

import (
	"fmt"
	"testing"

	"github.com/hashicorp/raft"
	raftbench "github.com/hashicorp/raft/bench"
)

const (
	runCount = uint32(5)
)

func BenchmarkMultiStore_FirstIndex(b *testing.B) {
	benchmarkMultiLogStore(b, raftbench.FirstIndex)
}

func BenchmarkMultiStore_LastIndex(b *testing.B) {
	benchmarkMultiLogStore(b, raftbench.LastIndex)
}

func BenchmarkMultiStore_GetLog(b *testing.B) {
	benchmarkMultiLogStore(b, raftbench.GetLog)
}

func BenchmarkMultiStore_StoreLog(b *testing.B) {
	benchmarkMultiLogStore(b, raftbench.StoreLog)
}

func BenchmarkMultiStore_StoreLogs(b *testing.B) {
	benchmarkMultiLogStore(b, raftbench.StoreLogs)
}

func BenchmarkMultiStore_DeleteRange(b *testing.B) {
	benchmarkMultiLogStore(b, raftbench.DeleteRange)
}

func BenchmarkMultiStore_Set(b *testing.B) {
	benchmarkMultiStableStore(b, raftbench.Set)
}

func BenchmarkMultiStore_Get(b *testing.B) {
	benchmarkMultiStableStore(b, raftbench.Get)
}

func BenchmarkMultiStore_SetUint64(b *testing.B) {
	benchmarkMultiStableStore(b, raftbench.SetUint64)
}

func BenchmarkMultiStore_GetUint64(b *testing.B) {
	benchmarkMultiStableStore(b, raftbench.GetUint64)
}

func benchmarkMultiLogStore(b *testing.B, benchFn func(*testing.B, raft.LogStore)) {
	multi := newMultiStore(b)

	b.Run("group", func(b *testing.B) {
		for id := range runCount {
			b.Run(fmt.Sprintf("%d", id), func(b *testing.B) {
				store := multi.New(id)
				benchFn(b, store)
			})
		}
	})
}

func benchmarkMultiStableStore(b *testing.B, benchFn func(*testing.B, raft.StableStore)) {
	multi := newMultiStore(b)

	b.Run("group", func(b *testing.B) {
		for id := range runCount {
			b.Run(fmt.Sprintf("%d", id), func(b *testing.B) {
				store := multi.New(id)
				benchFn(b, store)
			})
		}
	})
}
