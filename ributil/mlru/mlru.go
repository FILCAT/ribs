// Package mlru provides a thread-safe, mergeable least recently used (LRU) cache.
package mlru

import (
	"errors"
	"sync"
	"sync/atomic"
)

// LRUGroup contains a shared counter used for indexing entries.
type LRUGroup struct {
	counter atomic.Int64
}

// MLRU represents a mergeable least recently used (LRU) cache.
type MLRU[K comparable, V any] struct {
	group *LRUGroup
	mu    sync.Mutex // Mutex to ensure thread safety
	valid bool       // Flag to check if the cache is valid

	currentSize, capacity int64
	newest, oldest        *entry[K, V]
	keys                  map[K]*entry[K, V]
}

// entry represents an entry in the LRU cache.
type entry[K comparable, V any] struct {
	key          K
	newer, older *entry[K, V]

	index int64
	value V
}

// NewMLRU creates a new MLRU cache with the specified group and capacity.
func NewMLRU[K comparable, V any](group *LRUGroup, capacity int64) *MLRU[K, V] {
	return &MLRU[K, V]{
		group:    group,
		valid:    true,
		capacity: capacity,
		keys:     make(map[K]*entry[K, V]),
	}
}

// evictLast removes the oldest entry from the cache.
func (l *MLRU[K, V]) evictLast() (evicted *entry[K, V]) {
	if l.oldest != nil {
		evicted = l.oldest

		delete(l.keys, l.oldest.key)
		if l.currentSize == 1 {
			// If size is one, set newest and oldest to nil after eviction.
			l.newest, l.oldest = nil, nil // TODO NOT COVERED
		} else {
			l.oldest = l.oldest.newer
			if l.oldest != nil {
				l.oldest.older = nil
			}
		}
		l.currentSize--
	}

	return
}

// normal lru would just evict the oldest entry. This is part of levelcache,
// where instead of evicting on put, we will merge this layer into the lower layer
// which is where the eviction happens.
var ErrCacheFull = errors.New("cache is full")

// Put adds a new entry to the cache or updates an existing entry.
// It evicts the oldest entry if the cache is at full capacity.
func (l *MLRU[K, V]) Put(key K, value V) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.valid {
		return errors.New("invalid cache") // TODO NOT COVERED
	}

	if existing, ok := l.keys[key]; ok {
		// Allow updating the value and move the entry to the front.
		existing.value = value
		if existing.newer != nil {
			existing.newer.older = existing.older
		}
		if existing.older != nil {
			existing.older.newer = existing.newer // TODO NOT COVERED
		}
		existing.older = l.newest
		l.newest.newer = existing
		l.newest = existing
		return nil
	}

	if l.currentSize >= l.capacity {
		return ErrCacheFull
	}
	index := l.group.counter.Add(1)
	newEntry := &entry[K, V]{key: key, index: index, value: value}
	l.keys[key] = newEntry
	if l.newest == nil {
		l.newest, l.oldest = newEntry, newEntry
	} else {
		newEntry.older = l.newest
		l.newest.newer = newEntry
		l.newest = newEntry
	}
	l.currentSize++
	return nil
}

// Get retrieves an entry from the cache and moves it to the front.
// Returns an error if the key does not exist or the cache is invalid.
func (l *MLRU[K, V]) Get(key K) (V, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.valid {
		var zero V
		return zero, errors.New("invalid cache")
	}

	entry, exists := l.keys[key]
	if !exists {
		var zero V
		return zero, errors.New("key does not exist")
	}
	// Move the accessed entry to the front.
	if entry.newer != nil {
		entry.newer.older = entry.older
	} else {
		// already newest
		return entry.value, nil
	}
	if entry.older != nil {
		entry.older.newer = entry.newer
	} else {
		// If the entry was the oldest, update l.oldest.
		l.oldest = entry.newer
	}
	entry.older = l.newest
	entry.newer = nil
	l.newest.newer = entry
	l.newest = entry
	return entry.value, nil
}

// Merge merges another MLRU cache into the current cache.
// The other cache is invalidated after merging.
// Returns an error if either cache is invalid.
func (l *MLRU[K, V]) Merge(other *MLRU[K, V]) error {
	if other == nil {
		return errors.New("invalid cache")
	}
	if l == other {
		return errors.New("cannot merge cache with itself")
	}

	l.mu.Lock()
	other.mu.Lock()
	defer l.mu.Unlock()
	defer other.mu.Unlock()

	if !l.valid || !other.valid {
		return errors.New("invalid cache")
	}

	if l.capacity == 0 {
		return nil // wat
	}

	// Invalidating the other cache after merging.
	other.valid = false

	entOther := other.newest
	entHere := l.newest

	toIter := l.capacity // tracks how many entries have been processed

	for entOther != nil && toIter > 0 {
		// rewind to entOther.index
		// atIndex will be decreasing as we iterate because we're viewing older-and-older entries
		atIndex := int64(-1)
		if entHere != nil {
			atIndex = entHere.index
		}

		if entOther.index < atIndex {
			// need to rewind entHere more
			if entHere == nil {
				// wut
				return errors.New("somehow other ent counter is lt -1")
			}

			// basically we accept the entry from the cache at this position
			entHere = entHere.older
			toIter--
			continue
		}

		if l.currentSize >= l.capacity {

			// If already at capacity, evict the oldest entry to make room.
			evicted := l.evictLast()

			if evicted == entHere {
				// now we're inserting the oldest element
				entHere = nil
			}
		}

		olderOther := entOther.older

		l.keys[entOther.key] = entOther
		l.currentSize++
		toIter--

		// now the entHere is OLDER than entOther, so we need to insert entOther
		// as the newer element to entHere

		// first make sure that entOther pointers aren't pointing at anything we really
		// don't want them to point at
		entOther.older, entOther.newer = entHere, nil

		if entHere == nil {
			// if entHere is nil, we are inserting the oldest element
			entOther.newer = l.oldest
			l.oldest = entOther
			if entOther.newer != nil {
				// there may not have been any older elements
				// (this probably means that we're also inserting the newest element,
				//  and this probably could be in the if body below)
				entOther.newer.older = entOther
			}
			if l.newest == nil {
				// and apparently this is also the newest element
				l.newest = entOther
			}
		} else {
			if entHere.key == entOther.key {
				return errors.New("can't merge caches with duplicate keys")
			}

			// we are inserting an element newer to entHere
			entOther.newer = entHere.newer
			entHere.newer = entOther
			if l.newest == entHere {
				l.newest = entOther
			}
		}

		// take next entOther
		entOther = olderOther
	}

	return nil
}
