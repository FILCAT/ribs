package mlru

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.Equal(t, ErrCacheFull, err)

	_, err = cache.Get(3)
	assert.Error(t, err) // The key 3 should not have been put

	val, err := cache.Get(2)
	assert.NoError(t, err)
	assert.Equal(t, 200, val)

	val, err = cache.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, 100, val)
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

func TestEvictLastWithOneElement(t *testing.T) {
	group := &LRUGroup{counter: atomic.Int64{}}
	cache := NewMLRU[int, int](group, 1)

	err := cache.Put(1, 100)
	assert.NoError(t, err)
	err = cache.Put(2, 200) // This should say the cache is full
	assert.EqualError(t, err, ErrCacheFull.Error())
}

func TestInvalidCachePut(t *testing.T) {
	group := &LRUGroup{counter: atomic.Int64{}}
	cache1 := NewMLRU[int, int](group, 2)
	cache2 := NewMLRU[int, int](group, 2)

	err := cache1.Merge(cache2) // This will invalidate cache2
	assert.NoError(t, err)

	err = cache2.Put(1, 100)
	assert.Error(t, err) // Should return error as the cache is invalid
	assert.Equal(t, "invalid cache", err.Error())
}

func TestMergeWithMovingToNext(t *testing.T) {
	group := &LRUGroup{counter: atomic.Int64{}}
	cache1 := NewMLRU[int, int](group, 2)
	err := cache1.Put(2, 200) // most recent entry in cache1
	assert.NoError(t, err)

	cache2 := NewMLRU[int, int](group, 2)
	err = cache2.Put(1, 100) // older entry in cache2
	assert.NoError(t, err)

	err = cache1.Merge(cache2)
	assert.NoError(t, err)

	_, err = cache1.Get(2)
	assert.NoError(t, err) // The key 2 should be in cache1

	_, err = cache1.Get(1)
	assert.NoError(t, err) // The key 1 should be in cache1
}

func TestMergeWithIncreasingCurrentSize(t *testing.T) {
	group := &LRUGroup{counter: atomic.Int64{}}
	cache1 := NewMLRU[int, int](group, 4)
	err := cache1.Put(3, 300) // fresh entry in cache1
	assert.NoError(t, err)
	err = cache1.Put(4, 400) // fresh entry in cache1
	assert.NoError(t, err)

	cache2 := NewMLRU[int, int](group, 2)
	err = cache2.Put(1, 100) // older entry in cache2
	assert.NoError(t, err)
	err = cache2.Put(2, 200) // older entry in cache2
	assert.NoError(t, err)

	err = cache1.Merge(cache2)
	assert.NoError(t, err)

	_, err = cache1.Get(3)
	assert.NoError(t, err) // The key 3 should be in cache1

	_, err = cache1.Get(4)
	assert.NoError(t, err) // The key 4 should be in cache1

	_, err = cache1.Get(1)
	assert.NoError(t, err) // The key 1 should be in cache1

	_, err = cache1.Get(2)
	assert.NoError(t, err) // The key 2 should be in cache1

	assert.Equal(t, int64(4), cache1.currentSize) // The currentSize should be 4
}

func TestMergeWithNilCurrentEntry(t *testing.T) {
	group := &LRUGroup{}
	cache1 := NewMLRU[string, string](group, 2)
	cache2 := NewMLRU[string, string](group, 2)

	// Put some values in cache2
	err := cache2.Put("k1", "v1")
	assert.NoError(t, err)
	err = cache2.Put("k2", "v2")
	assert.NoError(t, err)

	// Merging empty cache1 with cache2
	err = cache1.Merge(cache2)
	assert.NoError(t, err)

	// cache1 should have the values from cache2
	value, err := cache1.Get("k1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", value)

	value, err = cache1.Get("k2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", value)
}

func TestMergeAndUpdatePointers(t *testing.T) {
	group := &LRUGroup{}
	cache1 := NewMLRU[string, string](group, 3)
	cache2 := NewMLRU[string, string](group, 3)

	// Put some values in both caches
	err := cache1.Put("k1", "v1")
	assert.NoError(t, err)
	err = cache1.Put("k2", "v2")
	assert.NoError(t, err)
	err = cache2.Put("k3", "v3")
	assert.NoError(t, err)

	// Merging cache1 with cache2
	err = cache1.Merge(cache2)
	assert.NoError(t, err)

	// cache1 should have the values from both caches
	value, err := cache1.Get("k1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", value)

	value, err = cache1.Get("k2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", value)

	value, err = cache1.Get("k3")
	assert.NoError(t, err)
	assert.Equal(t, "v3", value)
}

func TestMergeAndEvict(t *testing.T) {
	group := &LRUGroup{}
	cache1 := NewMLRU[string, string](group, 2)
	cache2 := NewMLRU[string, string](group, 2)

	// Put some values in both caches
	err := cache1.Put("k1", "v1")
	assert.NoError(t, err)
	err = cache1.Put("k2", "v2")
	assert.NoError(t, err)
	err = cache2.Put("k3", "v3")
	assert.NoError(t, err)
	err = cache2.Put("k4", "v4")
	assert.NoError(t, err)

	// Merging cache1 with cache2
	err = cache1.Merge(cache2)
	assert.NoError(t, err)

	// validate entry chain
	require.Equal(t, "k4", cache1.newest.key)
	require.Equal(t, "k3", cache1.newest.older.key)
	require.Nil(t, cache1.newest.newer)
	require.Equal(t, "k3", cache1.oldest.key)
	require.Equal(t, "k4", cache1.oldest.newer.key)
	require.Nil(t, cache1.oldest.older)

	// cache1 should have the most recent values from both caches
	_, err = cache1.Get("k1")
	assert.Error(t, err) // "k1" should have been evicted

	value, err := cache1.Get("k2")
	assert.Error(t, err) // "k2" should have been evicted

	value, err = cache1.Get("k3")
	assert.NoError(t, err)
	assert.Equal(t, "v3", value)

	value, err = cache1.Get("k4")
	assert.NoError(t, err)
	assert.Equal(t, "v4", value)

	// validate entry chain
	require.Equal(t, "k4", cache1.newest.key)
	require.Equal(t, "k3", cache1.newest.older.key)
	require.Nil(t, cache1.newest.newer)
	require.Equal(t, "k3", cache1.oldest.key)
	require.Equal(t, "k4", cache1.oldest.newer.key)
	require.Nil(t, cache1.oldest.older)

	require.Nil(t, cache1.newest.older.older)
	require.Nil(t, cache1.oldest.newer.newer)
}

func TestMergeWithInvalidCache(t *testing.T) {
	group := &LRUGroup{}
	cache1 := NewMLRU[string, string](group, 2)
	cache2 := NewMLRU[string, string](group, 2)

	// Put some values in cache2
	err := cache2.Put("k1", "v1")
	assert.NoError(t, err)
	err = cache2.Put("k2", "v2")
	assert.NoError(t, err)

	// Invalidate cache2
	cache2.valid = false

	// Merging cache1 with invalid cache2 should result in an error
	err = cache1.Merge(cache2)
	assert.Error(t, err)
	assert.Equal(t, "invalid cache", err.Error())
}

func TestMergeWithNilCache(t *testing.T) {
	group := &LRUGroup{}
	cache1 := NewMLRU[string, string](group, 2)

	// Merging cache1 with nil cache should result in an error
	err := cache1.Merge(nil)
	assert.Error(t, err)
	assert.Equal(t, "invalid cache", err.Error())
}

func TestMergeWithMoreRecentOther(t *testing.T) {
	group := &LRUGroup{}
	cache1 := NewMLRU[string, string](group, 2)
	cache2 := NewMLRU[string, string](group, 2)

	// Put some values in both caches
	err := cache1.Put("k1", "v1")
	assert.NoError(t, err)
	err = cache2.Put("k2", "v2")
	assert.NoError(t, err)
	err = cache2.Put("k3", "v3")
	assert.NoError(t, err)

	// Merging cache1 with cache2
	err = cache1.Merge(cache2)
	assert.NoError(t, err)

	// cache1 should have the most recent values from cache2
	_, err = cache1.Get("k1")
	assert.Error(t, err) // "k1" should have been evicted

	value, err := cache1.Get("k2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", value)

	value, err = cache1.Get("k3")
	assert.NoError(t, err)
	assert.Equal(t, "v3", value)
}

func TestMergeWithEmptyOther(t *testing.T) {
	group := &LRUGroup{}
	cache1 := NewMLRU[string, string](group, 2)
	cache2 := NewMLRU[string, string](group, 2)

	// Put some values in cache1
	err := cache1.Put("k1", "v1")
	assert.NoError(t, err)

	// Merging cache1 with empty cache2
	err = cache1.Merge(cache2)
	assert.NoError(t, err)

	// cache1 should still have the values from before
	value, err := cache1.Get("k1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", value)
}
