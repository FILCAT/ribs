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
func (l *MLRU[K, V]) evictLast() {
	if l.oldest != nil {
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
}

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
		l.evictLast()
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
	}
	if entry.older != nil {
		entry.older.newer = entry.newer
	} else {
		// If the entry was the oldest, update l.oldest.
		l.oldest = entry.newer
	}
	entry.older = l.newest
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

	newestEntOther := other.newest
	newestEntHere := l.newest

	for newestEntOther != nil {
		if newestEntHere == nil || newestEntHere.index > newestEntOther.index {
			if newestEntHere == nil {
				// if newestEntHere is nil, assume the list is empty and populate it with newestEntOther
				l.newest = newestEntOther
				l.oldest = newestEntOther
				// Update the keys map with the new entry.
				l.keys[newestEntOther.key] = newestEntOther

				newestEntHere = newestEntOther
				newestEntOther = newestEntOther.older
				continue
			}
			if newestEntHere.older == nil {
				// At the end of the list, append the rest of the other list until at capacity.
				for l.currentSize < l.capacity && newestEntOther != nil {
					l.currentSize++
					newestEntHere.older = newestEntOther
					newestEntOther.newer = newestEntHere
					// Update the oldest pointer and the keys map.
					l.oldest = newestEntOther
					l.keys[newestEntOther.key] = newestEntOther
					newestEntHere = newestEntOther
					newestEntOther = newestEntOther.older
				}
				break
			}
			// Move to the older here, which will have a lower index.
			newestEntHere = newestEntHere.older
			continue
		}

		// Here the other entry has a higher index, insert it before the current entry in this list.
		if l.currentSize >= l.capacity {
			// If already at capacity, evict the oldest entry to make room.
			l.evictLast()
		}
		newEntry := newestEntOther
		newestEntOther = newestEntOther.older // move to the older in other list.

		// Insert newEntry before newestEntHere in this list.
		newEntry.older = newestEntHere
		newEntry.newer = newestEntHere.newer
		if newestEntHere.newer != nil {
			newestEntHere.newer.older = newEntry
		} else {
			// Updating the newest entry if needed.
			l.newest = newEntry
		}
		newestEntHere.newer = newEntry
		l.currentSize++
		// Update the keys map with the new entry.
		l.keys[newEntry.key] = newEntry
	}
	return nil
}
