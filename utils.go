package raftrocksdb

import (
	"errors"
	"fmt"

	"github.com/linxGnu/grocksdb"
)

var (
	errNotFound = errors.New("not found")
)

func wrap(err error, message string) error {
	return fmt.Errorf("%s error=%w", message, err)
}

func openDB(opts *options) (*grocksdb.DB, error) {
	if opts.path == "" {
		return nil, fmt.Errorf("missing path option")
	}

	dbOpts := opts.dbOpts
	if dbOpts == nil {
		dbOpts = newDBOptions()
	}

	db, err := grocksdb.OpenDb(dbOpts, opts.path)
	if err != nil {
		return nil, wrap(err, "failed to open db")
	}

	return db, nil
}
