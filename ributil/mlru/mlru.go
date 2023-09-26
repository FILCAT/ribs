package mlru

import (
	"errors"
	"sync"
	"sync/atomic"
)

type LRUGroup struct {
	counter atomic.Int64
}

type MLRU[K comparable, V any] struct {
	group *LRUGroup
	mu    sync.Mutex // Mutex to ensure thread safety
	valid bool       // Flag to check if the cache is valid

	currentSize, capacity int64
	first, last           *entry[K, V]
	keys                  map[K]*entry[K, V]
}

type entry[K comparable, V any] struct {
	key        K
	prev, next *entry[K, V]

	index int64
	value V
}

func NewMLRU[K comparable, V any](group *LRUGroup, capacity int64) *MLRU[K, V] {
	return &MLRU[K, V]{
		group:    group,
		valid:    true,
		capacity: capacity,
		keys:     make(map[K]*entry[K, V]),
	}
}

func (l *MLRU[K, V]) evictLast() {
	if l.last != nil {
		delete(l.keys, l.last.key)
		if l.currentSize == 1 {
			// If size is one, set first and last to nil after eviction
			l.first, l.last = nil, nil
		} else {
			l.last = l.last.prev
			if l.last != nil {
				l.last.next = nil
			}
		}
		l.currentSize--
	}
}

func (l *MLRU[K, V]) Put(key K, value V) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.valid {
		return errors.New("invalid cache")
	}

	if existing, ok := l.keys[key]; ok {
		// Allow updating the value and move the entry to the front
		existing.value = value
		if existing.prev != nil {
			existing.prev.next = existing.next
		}
		if existing.next != nil {
			existing.next.prev = existing.prev
		}
		existing.next = l.first
		l.first.prev = existing
		l.first = existing
		return nil
	}

	if l.currentSize >= l.capacity {
		l.evictLast()
	}
	index := l.group.counter.Add(1)
	newEntry := &entry[K, V]{key: key, index: index, value: value}
	l.keys[key] = newEntry
	if l.first == nil {
		l.first, l.last = newEntry, newEntry
	} else {
		newEntry.next = l.first
		l.first.prev = newEntry
		l.first = newEntry
	}
	l.currentSize++
	return nil
}

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
	if entry.prev != nil {
		entry.prev.next = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		// If the entry was the last, update l.last
		l.last = entry.prev
	}
	entry.next = l.first
	l.first.prev = entry
	l.first = entry
	return entry.value, nil
}

func (l *MLRU[K, V]) Merge(other *MLRU[K, V]) error {
	l.mu.Lock()
	other.mu.Lock()
	defer l.mu.Unlock()
	defer other.mu.Unlock()

	if !l.valid || !other.valid {
		return errors.New("invalid cache")
	}

	// Invalidating the other cache after merging.
	other.valid = false

	curEntOther := other.first
	curEntHere := l.first

	for {
		if curEntHere.index > curEntOther.index {
			if curEntHere.next == nil {
				// At the end of the list, append the rest of the other list until at capacity
				for l.currentSize < l.capacity && curEntOther != nil {
					l.currentSize++
					curEntHere.next = curEntOther
					curEntOther.prev = curEntHere
					curEntHere = curEntOther
					curEntOther = curEntOther.next
				}
				break
			}

			// Move to the next here, which will have a lower index
			curEntHere = curEntHere.next
			continue
		}

		// Here the other entry has a higher index, insert it before the current entry in this list
		if l.currentSize >= l.capacity {
			// If already at capacity, evict the last entry to make room
			l.evictLast()
		}
		newEntry := curEntOther
		curEntOther = curEntOther.next // move to the next in other list

		// Insert newEntry before curEntHere in this list
		newEntry.next = curEntHere
		newEntry.prev = curEntHere.prev
		if curEntHere.prev != nil {
			curEntHere.prev.next = newEntry
		} else {
			// Updating the first entry if needed
			l.first = newEntry
		}
		curEntHere.prev = newEntry
		l.currentSize++
		l.keys[newEntry.key] = newEntry

		if curEntOther == nil {
			break
		}
	}
	return nil
}
