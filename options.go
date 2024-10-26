package raftrocksdb

import (
	"github.com/linxGnu/grocksdb"
)

type Option func(*options)

type options struct {
	path   string
	sync   bool
	dbOpts *grocksdb.Options
}

func newOptions() *options {
	return &options{}
}

// WithPath sets the storage path.
func WithPath(path string) Option {
	return func(o *options) {
		o.path = path
	}
}

// WithSync enables sync after each write.
// If this flag is true, writes will be slower.
// If this flag is false, and the machine crashes, some recent
// writes may be lost. Note that if it is just the process that
// crashes (i.e., the machine does not reboot), no writes will be
// lost even if sync==false.
// Default: false.
func WithSync(sync bool) Option {
	return func(o *options) {
		o.sync = sync
	}
}

// WithDBOptions sets the RocksDB database options.
func WithDBOptions(dbOpts *grocksdb.Options) Option {
	return func(o *options) {
		o.dbOpts = dbOpts
	}
}

func newDBOptions() *grocksdb.Options {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	return opts
}

func newReadOptions() *grocksdb.ReadOptions {
	opts := grocksdb.NewDefaultReadOptions()
	return opts
}

func newWriteOptions(sync bool) *grocksdb.WriteOptions {
	opts := grocksdb.NewDefaultWriteOptions()
	opts.SetSync(sync)
	return opts
}

func newIterReadOptions(id uint32) *grocksdb.ReadOptions {
	lower, upper := encodeLogKeyFullRange(id)

	opts := newReadOptions()
	opts.SetIterateLowerBound(lower)
	opts.SetIterateUpperBound(upper)

	return opts
}
