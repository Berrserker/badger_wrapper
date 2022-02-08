package badger_wrapper

import (
	"context"
	"fmt"
	"log"
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
	namespaceTest1 = "namespaceTest1"
	namespaceTest2 = "namespaceTest2"
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
		log.Fatalf(err.Error(), "badger.Open")
	}

	base := &Service{
		db: instance,
	}

	base.ctx, base.cancelFunc = context.WithCancel(context.TODO())

	suite.db = base
	suite.badger = instance

	testCase = make(map[string][]byte)

	for i := 0; i < testLen; i++ {
		test := fmt.Sprintf("testCase %d", i)
		testCase[test] = []byte(fmt.Sprintf("testCase %d", i))
	}

	for k, val := range testCase {
		err := suite.db.Set(namespaceTest1, k, val)
		if err != nil {
			log.Fatal("cannot write to base at tests_data_dir namespace all")
		}

		err = suite.db.Set(namespaceTest2, k, val)
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
		res, err := suite.db.Get(namespaceTest2, k)
		suite.NoError(err)
		suite.Equal(v, res)
	}
}
