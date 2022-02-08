// Package badger_wrapper is wrapper at https://godoc.org/github.com/dgraph-io/badger to easy use.
//	If "namespace" or the "key" does not exist in the provided base, an error
//	is returned, otherwise the retrieved value/s.
//	If the key/value pair cannot be saved, an error is returned.
//
package badger_wrapper

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

const (
	badgerDiscardRatio = 0.5
	// Default db GC interval
	badgerGCInterval = 10 * time.Minute
)

type DB interface {
	// Get attempts to get a value for a given key
	Get(namespace, key string) (value []byte, err error)
	// Set attempts to store a value for a given key
	Set(namespace, key string, value []byte) error
	// Has returns a boolean reflecting if the
	// database has a given key for a namespace or not.
	Has(namespace, key string) (bool, error)
	// Close closes the connection to the underlying
	Close() error
}

type Service struct {
	db         *badger.DB
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewDB(ctx context.Context, dir string) (DB, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}

	options := badger.DefaultOptions(dir)
	options.SyncWrites = true

	instance, err := badger.Open(options)
	if err != nil {
		return nil, errors.Wrap(err, "badger.Open")
	}

	base := &Service{
		db: instance,
	}

	base.ctx, base.cancelFunc = context.WithCancel(ctx)

	return base, nil
}

func (s *Service) Get(namespace, key string) (value []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(submitNamespaceKey(namespace, key))
		if err != nil {
			return err
		}


		value, err = item.ValueCopy(value)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (s *Service) Set(namespace, key string, value []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(submitNamespaceKey(namespace, key), value)
	})
	if err != nil {
		return err
	}

	// Save to disk.
	err = s.db.Sync()

	return err
}

func (s *Service) Has(namespace, key string) (ok bool, err error) {
	_, err = s.Get(namespace, key)
	switch err {
	case badger.ErrKeyNotFound:
		ok, err = false, nil
	case nil:
		ok, err = true, nil
	}

	return
}

func (s *Service) Close() error {
	s.cancelFunc()

	return s.db.Close()
}

// RunGC triggers the garbage collection for the db backend database. It
// should be run in a goroutine.
func (s *Service) RunGC() error {
	ticker := time.NewTicker(badgerGCInterval)

	for {
		select {
		case <-ticker.C:
			err := s.db.RunValueLogGC(badgerDiscardRatio)
			if err != nil {
				// don't report error when GC didn't result in any cleanup
				if err == badger.ErrNoRewrite {
					// pass
				} else {
					return errors.Wrap(err, "badger.DB.RunValueLogGC")
				}
			}

		case <-s.ctx.Done():
			return nil
		}
	}
}

// Namespace is a prefix:|)
func submitNamespaceKey(namespace, key string) []byte {
	prefix := []byte(fmt.Sprintf("%s/", namespace))

	return append(prefix, key...)
}
