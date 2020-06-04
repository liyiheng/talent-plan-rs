package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db   *badger.DB
}

// NewStandAloneStorage creates a StandAloneStorage with conf and returns its pointer
// without opening the db
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{conf, nil}
}

// Start opens the db
func (s *StandAloneStorage) Start() error {
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath
	db, err := badger.Open(opts)
	s.db = db
	return err
}

// Stop closes the db
func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

// Reader creates a StorageReader
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &reader{s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()
	for _, op := range batch {
		cf := op.Cf()
		key := op.Key()
		_, isPut := op.Data.(storage.Put)
		var err error
		if isPut {
			err = engine_util.PutCF(s.db, cf, key, op.Value())
		} else {
			err = engine_util.DeleteCF(s.db, cf, key)
		}
		if err != nil {
			return err
		}

	}
	return nil
}

// reader implements storage.StorageReader with *badger.Txn
type reader struct {
	txn *badger.Txn
}

// When the key doesn't exist, return nil for the value
func (r *reader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (r *reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *reader) Close() {
	r.txn.Discard()
}
