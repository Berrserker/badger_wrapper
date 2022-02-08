package badger

import (
	"context"
	"fmt"
	"log"
	"github.com/pkg/errors"
	"os"
	"path"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/suite"
)

type DbTestSuite struct {
	badger *badger.DB
	suite.Suite
	db DB
}

func TestServiceTestSuite(t *testing.T) {
	suite.Run(t, new(DbTestSuite))
}

const (
	namespaceAll = "all"
	namespaceGet = "get"
	testLen      = 10 // nolint:
)

var testCase map[string][]byte

func (suite *DbTestSuite) SetupSuite() {
	wd, _ := os.Getwd()
	dir := path.Join(wd, "db_test")

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Fatalf("cannot create dir at %s", dir)
	}

	options := badger.DefaultOptions(dir)
	options.SyncWrites = true

	instance, err := badger.Open(options)
	if err != nil {
		log.Fatalf(err, "badger.Open")
	}

	base := &Service{
		db: instance,
	}

	base.ctx, base.cancelFunc = context.WithCancel(ctx)

	suite.db = base
	suite.badger = instance

	testCase = make(map[string][]byte)

	for i := 0; i < testLen; i++ {
		test := fmt.Sprintf("testCase %d", i)
		testCase[test] = []byte(fmt.Sprintf("testCase %d", i))
	}

	for k, val := range testCase {
		err := suite.db.Set(namespaceAll, k, val)
		if err != nil {
			log.Fatal("cannot write to base at tests_data_dir namespace all")
		}

		err = suite.db.Set(namespaceGet, k, val)
		if err != nil {
			log.Fatal("cannot write to base at tests_data_dir namespace get")
		}
	}
}

func (suite *DbTestSuite) TearDownSuite() {
	if err := suite.badger.DropAll(); err != nil {
		log.Fatal("badger.DropAll()")
	}

	if err := suite.db.Close(); err != nil {
		log.Fatal("db.Close()")
	}
}

func (suite *DbTestSuite) TestBadgerDB_Get() {
	for k, v := range testCase {
		res, err := suite.db.Get(namespaceGet, k)
		suite.NoError(err)
		suite.Equal(v, res)
	}
}
