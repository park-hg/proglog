package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	log_v1 "github.com/park-hg/proglog/api/v1"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			assert.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			if scenario == "make new segment" {
				c.Segment.MaxIndexBytes = 13
			}
			log, err := NewLog(dir, c)
			assert.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	record := &log_v1.Record{Value: []byte("hello world")}
	off, err := log.Append(record)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), off)
	read, err := log.Read(off)
	assert.NoError(t, err)
	assert.Equal(t, record.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	assert.Nil(t, read)
	assert.Error(t, err)
}

func testInitExisting(t *testing.T, log *Log) {
	record := &log_v1.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := log.Append(record)
		assert.NoError(t, err)
	}
	assert.NoError(t, log.Close())

	off, err := log.LowestOffset()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), off)
	off, err = log.HighestOffset()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), off)

	newLog, err := NewLog(log.Dir, log.Config)
	assert.NoError(t, err)

	off, err = newLog.LowestOffset()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), off)
	off, err = newLog.HighestOffset()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	record := &log_v1.Record{Value: []byte("hello world")}
	off, err := log.Append(record)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := io.ReadAll(reader)
	assert.NoError(t, err)

	read := &log_v1.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	assert.NoError(t, err)
	assert.Equal(t, record.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	record := &log_v1.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := log.Append(record)
		assert.NoError(t, err)
	}

	_, err := log.Read(0)
	assert.NoError(t, err)

	err = log.Truncate(1)
	assert.NoError(t, err)

	_, err = log.Read(0)
	assert.Error(t, err)
}
