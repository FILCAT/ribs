package mlru

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	group := &LRUGroup{counter: atomic.Int64{}}
	cache := NewMLRU[int, int](group, 2)

	err := cache.Put(1, 100)
	assert.NoError(t, err)
	val, err := cache.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, 100, val)
}

func TestEvictLast(t *testing.T) {
	group := &LRUGroup{counter: atomic.Int64{}}
	cache := NewMLRU[int, int](group, 2)

	err := cache.Put(1, 100)
	assert.NoError(t, err)
	err = cache.Put(2, 200)
	assert.NoError(t, err)
	err = cache.Put(3, 300)
	assert.NoError(t, err)

	_, err = cache.Get(1)
	assert.Error(t, err) // The key 1 should have been evicted

	val, err := cache.Get(2)
	assert.NoError(t, err)
	assert.Equal(t, 200, val)

	val, err = cache.Get(3)
	assert.NoError(t, err)
	assert.Equal(t, 300, val)
}

func TestUpdateExistingKey(t *testing.T) {
	group := &LRUGroup{counter: atomic.Int64{}}
	cache := NewMLRU[int, int](group, 2)

	err := cache.Put(1, 100)
	assert.NoError(t, err)
	err = cache.Put(1, 150)
	assert.NoError(t, err)

	val, err := cache.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, 150, val)
}

func TestMergeCaches(t *testing.T) {
	group := &LRUGroup{counter: atomic.Int64{}} // Shared group
	cache1 := NewMLRU[int, int](group, 2)
	err := cache1.Put(1, 100)
	assert.NoError(t, err)
	err = cache1.Put(2, 200)
	assert.NoError(t, err)

	cache2 := NewMLRU[int, int](group, 2) // Sharing the same group
	err = cache2.Put(3, 300)
	assert.NoError(t, err)
	err = cache2.Put(4, 400)
	assert.NoError(t, err)

	err = cache1.Merge(cache2)
	assert.NoError(t, err)

	val, err := cache1.Get(3)
	assert.NoError(t, err)
	assert.Equal(t, 300, val)

	val, err = cache1.Get(4)
	assert.NoError(t, err)
	assert.Equal(t, 400, val)

	_, err = cache2.Get(3)
	assert.Error(t, err) // cache2 should be invalid after the merge
}

func TestOrderedMerge(t *testing.T) {
	group := &LRUGroup{counter: atomic.Int64{}} // Shared group
	cache1 := NewMLRU[int, int](group, 2)
	err := cache1.Put(1, 100) // v1 into cache1
	assert.NoError(t, err)

	cache2 := NewMLRU[int, int](group, 2) // Sharing the same group
	err = cache2.Put(3, 300)              // v3 into cache2
	assert.NoError(t, err)

	err = cache1.Put(2, 200) // v2 into cache1
	assert.NoError(t, err)

	err = cache2.Put(4, 400) // v4 into cache2
	assert.NoError(t, err)

	err = cache1.Merge(cache2)
	assert.NoError(t, err)

	// Expecting v2, v4 in cache1
	val, err := cache1.Get(2)
	assert.NoError(t, err)
	assert.Equal(t, 200, val)

	val, err = cache1.Get(4)
	assert.NoError(t, err)
	assert.Equal(t, 400, val)

	// v1 and v3 should not be in cache1 after the merge
	_, err = cache1.Get(1)
	assert.Error(t, err)

	_, err = cache1.Get(3)
	assert.Error(t, err)

	// cache2 should be invalid after the merge
	_, err = cache2.Get(3)
	assert.Error(t, err)
}
