package logcache

import (
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	// Create a temporary directory for testing
	dir, err := ioutil.TempDir("", "logcache")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	// Testing opening a new LogCache
	lc, err := Open(dir)
	assert.NoError(t, err)
	assert.NotNil(t, lc)
}

func TestPut(t *testing.T) {
	dir, err := ioutil.TempDir("", "logcache")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	lc, err := Open(dir)
	assert.NoError(t, err)

	// Create a sample multihash
	mh, err := multihash.Sum([]byte("hello"), multihash.SHA2_256, -1)
	assert.NoError(t, err)

	// Testing putting a new entry in the cache
	err = lc.Put(mh, []byte("data"))
	assert.NoError(t, err)
}

func TestGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "logcache")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	lc, err := Open(dir)
	assert.NoError(t, err)

	mh, err := multihash.Sum([]byte("hello"), multihash.SHA2_256, -1)
	assert.NoError(t, err)

	err = lc.Put(mh, []byte("data"))
	assert.NoError(t, err)

	// Testing getting an entry from the cache
	err = lc.Get(mh, func(data []byte) error {
		assert.Equal(t, []byte("data"), data)
		return nil
	})
	assert.NoError(t, err)
}

func TestFlush(t *testing.T) {
	dir, err := ioutil.TempDir("", "logcache")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	lc, err := Open(dir)
	assert.NoError(t, err)

	// Testing flushing the cache
	err = lc.Flush()
	assert.NoError(t, err)
}

func TestOpenPutCloseOpenGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "logcache")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	// First Open
	lc, err := Open(dir)
	assert.NoError(t, err)

	// Create a sample multihash
	mh, err := multihash.Sum([]byte("hello"), multihash.SHA2_256, -1)
	assert.NoError(t, err)

	// Put data
	err = lc.Put(mh, []byte("data"))
	assert.NoError(t, err)

	// Close the LogCache
	err = lc.Close()
	assert.NoError(t, err)

	// Second Open
	lc, err = Open(dir)
	assert.NoError(t, err)

	// Get data
	err = lc.Get(mh, func(data []byte) error {
		assert.Equal(t, []byte("data"), data)
		return nil
	})
	assert.NoError(t, err)
}
