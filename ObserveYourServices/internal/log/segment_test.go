package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	api "github.com/park-hg/proglog/api/v1"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	assert.NoError(t, err)
	assert.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	assert.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		assert.NoError(t, err)
		assert.Equal(t, 16+i, off)

		got, err := s.Read(off)
		assert.NoError(t, err)
		assert.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	assert.Equal(t, io.EOF, err)

	assert.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	assert.NoError(t, err)

	assert.True(t, s.IsMaxed())

	err = s.Remove()
	assert.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	assert.NoError(t, err)
	assert.False(t, s.IsMaxed())
}
