raft-rocksdb
========

This repository provides the `raftrocksdb` Go package that enables RocksDB-backed storage for [Hashicorp Raft](https://github.com/hashicorp/raft) and allows working with multiple Raft instances, all stored in the same RocksDB database.

The package exports:
 - `Store` which implements both the LogStore and StableStore interfaces.
 - `MultiStore` which enables callers to dynamically create and remove Store instances.

### Prerequisites

Download and install [RocksDB](https://github.com/facebook/rocksdb):
 - https://github.com/facebook/rocksdb/releases

Or build from source:
 - https://github.com/facebook/rocksdb/blob/master/INSTALL.md
 - and don't forget to set the $LD_LIBRARY_PATH flag if you are using a custom build path.

Configure CGO flags required by the `grocksdb` library:
 - https://github.com/linxGnu/grocksdb/blob/master/README.md#install

Then add the library to your project:
 - `go get github.com/bcrusu/raft-rocksdb`

### Examples

With multiple Raft instances:
```golang
opts := []raftrocksdb.Option{
    raftrocksdb.WithPath("./path/to/db"),
    raftrocksdb.WithSync(true),
}

multi := raftrocksdb.NewMultiStore(opts...)
multi.Open()

// each store has an unique identifier
ids := []uint32{1, 2, 3}

for _, id := range ids {
    // creates a new Store...
    store := multi.New(id)

    // and pass it to NewRaft call
    raft, err := raft.NewRaft(config, fsm, store, store, snapshots, transport)
}

// when no longer needed, Drop removes all the persisted data for a certain Store:
multi.Drop(ids[0])

// Close closes all active stores
multi.Close()
```

Using a single Raft instance:
```golang
store := raftrocksdb.NewStore(opts...)
store.Open()

raft, err := raft.NewRaft(config, fsm, store, store, snapshots, transport)

store.Close()
```
To note that all error handling above is omitted for brevity.

### License

MIT
