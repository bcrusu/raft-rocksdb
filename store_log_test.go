package raftrocksdb_test

import (
	reflect "reflect"
	"testing"
	"time"

	raftrocksdb "github.com/bcrusu/raft-rocksdb"
	"github.com/hashicorp/raft"
)

func TestStore_FirstIndex(t *testing.T) {
	store := newStore(t)
	scenarioStore_FirstIndex(t, store)
}

func scenarioStore_FirstIndex(t *testing.T, store *raftrocksdb.Store) {
	if index, err := store.FirstIndex(); err != nil {
		t.Fatal(err)
	} else if index != 0 {
		t.Fatalf("unexpected index %d", index)
	}

	logs := storeTestLogs(t, store)

	if index, err := store.FirstIndex(); err != nil {
		t.Fatal(err)
	} else if index != logs[0].Index {
		t.Fatalf("unexpected index %d", index)
	}
}

func TestStore_LastIndex(t *testing.T) {
	store := newStore(t)
	scenarioStore_LastIndex(t, store)
}

func scenarioStore_LastIndex(t *testing.T, store *raftrocksdb.Store) {
	if index, err := store.LastIndex(); err != nil {
		t.Fatal(err)
	} else if index != 0 {
		t.Fatalf("unexpected index %d", index)
	}

	logs := storeTestLogs(t, store)

	if index, err := store.LastIndex(); err != nil {
		t.Fatal(err)
	} else if index != logs[len(logs)-1].Index {
		t.Fatalf("unexpected index %d", index)
	}
}

func TestStore_GetLog(t *testing.T) {
	store := newStore(t)
	scenarioStore_GetLog(t, store)
}

func scenarioStore_GetLog(t *testing.T, store *raftrocksdb.Store) {
	if err := store.GetLog(999, &raft.Log{}); err == nil {
		t.Fatal("expected err for not found log")
	}

	logs := storeTestLogs(t, store)
	i := 1

	log := &raft.Log{}
	if err := store.GetLog(logs[i].Index, log); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(log, logs[i]) {
		t.Fatal("returned log does not match")
	}
}

func TestStore_SetLog(t *testing.T) {
	store := newStore(t)
	scenarioStore_SetLog(t, store)
}

func scenarioStore_SetLog(t *testing.T, store *raftrocksdb.Store) {
	log1 := newRaftLog(10, "log10")

	if err := store.StoreLog(log1); err != nil {
		t.Fatal(err)
	}

	log2 := &raft.Log{}
	if err := store.GetLog(log1.Index, log2); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(log1, log2) {
		t.Fatal("returned log does not match")
	}
}

func TestStore_SetLogs(t *testing.T) {
	store := newStore(t)
	scenarioStore_SetLogs(t, store)
}

func scenarioStore_SetLogs(t *testing.T, store *raftrocksdb.Store) {
	logs := []*raft.Log{
		newRaftLog(10, "log10"),
		newRaftLog(11, "log11"),
		newRaftLog(12, "log12"),
	}

	if err := store.StoreLogs(logs); err != nil {
		t.Fatal(err)
	}

	for _, log1 := range logs {
		log2 := &raft.Log{}
		if err := store.GetLog(log1.Index, log2); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(log1, log2) {
			t.Fatal("returned log does not match")
		}
	}
}

func TestStore_DeleteRange(t *testing.T) {
	store := newStore(t)
	scenarioStore_DeleteRange(t, store)
}

func scenarioStore_DeleteRange(t *testing.T, store *raftrocksdb.Store) {
	logs := []*raft.Log{
		newRaftLog(10, "log10"),
		newRaftLog(11, "log11"),
		newRaftLog(12, "log12"),
		newRaftLog(13, "log13"),
		newRaftLog(14, "log14"),
		newRaftLog(15, "log15"),
	}
	deleted := make([]bool, len(logs))

	checkDeleted := func() {
		for i, log1 := range logs {
			log2 := &raft.Log{}
			err := store.GetLog(log1.Index, log2)

			if deleted[i] {
				if err == nil {
					t.Fatalf("expected err for log %d", i)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected err=%s for log %d", err, i)
				} else if !reflect.DeepEqual(log1, log2) {
					t.Fatalf("returned log %d does not match", i)
				}
			}
		}
	}

	if err := store.StoreLogs(logs); err != nil {
		t.Fatal(err)
	}

	// single delete
	if err := store.DeleteRange(10, 10); err != nil {
		t.Fatal(err)
	}
	deleted[0] = true

	checkDeleted()

	// multiple deletes
	if err := store.DeleteRange(12, 14); err != nil {
		t.Fatal(err)
	}
	deleted[2] = true
	deleted[3] = true
	deleted[4] = true

	checkDeleted()
}

func storeTestLogs(t *testing.T, store *raftrocksdb.Store) []*raft.Log {
	logs := []*raft.Log{
		newRaftLog(10, "log10"),
		newRaftLog(11, "log11"),
		newRaftLog(12, "log12"),
	}

	if err := store.StoreLogs(logs); err != nil {
		t.Fatal(err)
	}

	return logs
}

func newRaftLog(index uint64, data string) *raft.Log {
	return &raft.Log{
		Index:      index,
		Term:       index % 13,
		Type:       raft.LogType(index % 6),
		Data:       []byte(data),
		Extensions: []byte(data + "ext"),
		AppendedAt: time.Unix(int64(index), 0).UTC(),
	}
}
