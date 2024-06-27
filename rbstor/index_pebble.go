package rbstor

import (
	"context"
	"encoding/binary"
	"sync"

	iface "github.com/atboosty/ribs"
	"github.com/cockroachdb/pebble"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

// PebbleIndex is the top-level index, thread-safe.
type PebbleIndex struct {
	db *pebble.DB

	/*

		Keys:
		- 's:[mh bytes]' -> [i32BE size]{[i64BE best groupIdx]}
		- 'i:[mh bytes][i64BE groupIdx]' -> {}

	*/

	// todo: why tf is this all big endian?

	// todo limit size somehow
	iterPool sync.Pool

	// todo sharded lock
	dropLk sync.Mutex
}

// NewPebbleIndex creates a new Pebble-backed Index.
func NewPebbleIndex(path string) (*PebbleIndex, error) {

	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	return &PebbleIndex{
		db: db,
		iterPool: sync.Pool{
			New: func() interface{} {
				return db.NewIter(nil)
			},
		},
	}, nil
}

func (i *PebbleIndex) Sync(ctx context.Context) error {
	return i.db.Flush()
}

func (i *PebbleIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func(cidx int, gk iface.GroupKey) (more bool, err error)) error {
	for idx, m := range mh {
		// try to get from sizes
		sizeKey := append([]byte("s:"), m...)
		val, closer, err := i.db.Get(sizeKey)
		if err == pebble.ErrNotFound {
			continue
		}
		if err != nil {
			return xerrors.Errorf("get(s:) get: %w", err)
		}

		if len(val) > 4 {
			//size := binary.BigEndian.Uint32(val[:4])
			groupIdx := binary.BigEndian.Uint64(val[4:])
			groupKey := iface.GroupKey(groupIdx)

			if err := closer.Close(); err != nil {
				return xerrors.Errorf("get(s:) close: %w", err)
			}

			more, err := cb(idx, groupKey)
			if err != nil {
				return err
			}
			if !more {
				continue
			}
		}

		if err := closer.Close(); err != nil {
			return xerrors.Errorf("getsizes close: %w", err)
		}

		// try to get from iterable list

		keyPrefix := append([]byte("i:"), m...)
		upperBound := append(append([]byte("i:"), m...), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
		iter := i.db.NewIter(nil)
		iter.SetBounds(keyPrefix, upperBound)

		for iter.SeekGE(keyPrefix); iter.Valid(); iter.Next() {
			key := iter.Key()
			groupKeyBytes := key[len(key)-8:]
			groupKey := binary.BigEndian.Uint64(groupKeyBytes)

			more, err := cb(idx, iface.GroupKey(groupKey))
			if err != nil {
				if err := iter.Close(); err != nil {
					return xerrors.Errorf("closing iterator: %w", err)
				}
				return err
			}
			if !more {
				break
			}
		}

		if err := iter.Error(); err != nil {
			i.iterPool.Put(iter)
			return xerrors.Errorf("iter error: %w", err)
		}

		if err := iter.Close(); err != nil {
			return xerrors.Errorf("closing iterator: %w", err)
		}
	}

	return nil
}

func (i *PebbleIndex) GetSizes(ctx context.Context, mh []multihash.Multihash, cb func([]int32) error) error {
	sizes := make([]int32, len(mh))

	for id, m := range mh {
		sizeKey := append([]byte("s:"), m...)
		val, closer, err := i.db.Get(sizeKey)
		if err == pebble.ErrNotFound {
			sizes[id] = -1
			continue
		}
		if err != nil {
			return xerrors.Errorf("getsizes get: %w", err)
		}

		sizes[id] = int32(binary.BigEndian.Uint32(val))

		if err := closer.Close(); err != nil {
			return xerrors.Errorf("getsizes close: %w", err)
		}
	}

	return cb(sizes)
}

func (i *PebbleIndex) AddGroup(ctx context.Context, mh []multihash.Multihash, sizes []int32, group iface.GroupKey) error {
	groupBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(groupBytes, uint64(group))

	sizeBytes := make([]byte, 4+len(groupBytes))
	copy(sizeBytes[4:], groupBytes)

	batch := i.db.NewBatch()
	defer batch.Close()

	for i, m := range mh {
		{
			// group key
			key := append(append([]byte("i:"), m...), groupBytes...)
			if err := batch.Set(key, nil, pebble.NoSync); err != nil {
				return xerrors.Errorf("addgroup set (gk): %w", err)
			}
		}
		{
			// size key
			binary.BigEndian.PutUint32(sizeBytes, uint32(sizes[i]))

			key := append([]byte("s:"), m...)
			if err := batch.Set(key, sizeBytes, pebble.NoSync); err != nil {
				return xerrors.Errorf("addgroup set (sk): %w", err)
			}
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return xerrors.Errorf("addgroup commit: %w", err)
	}

	return nil
}

func (i *PebbleIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	i.dropLk.Lock()
	defer i.dropLk.Unlock()

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(group))

	batch := i.db.NewBatch()
	defer batch.Close()

	for _, m := range mh {
		key := append(append([]byte("i:"), m...), b...)
		if err := batch.Delete(key, pebble.NoSync); err != nil {
			return xerrors.Errorf("dropgroup delete: %w", err)
		}

		// if the size key contains entry for this group, remove the group pointer from the size key
		// this way GetGroups is able to return data with a single read from s: keys in the
		// common, optimistic case where the size key contains a group entry, and in the case when
		// the size key does not contain a group entry, it will still be able to return the correct
		// groups by reading from the i: keys
		//
		// note that we could scan the i: keys to find out if another mh->group mapping exists,
		// but that would slow down deletes by a lot. Instead this will be done lazily in a GC pass
		sizeKey := append([]byte("s:"), m...)
		val, closer, err := i.db.Get(sizeKey)
		if err == pebble.ErrNotFound {
			continue
		}
		if err != nil {
			return xerrors.Errorf("get(s:) get: %w", err)
		}

		if len(val) > 4 {
			//size := binary.BigEndian.Uint32(val[:4])
			groupIdx := binary.BigEndian.Uint64(val[4:])
			groupKey := iface.GroupKey(groupIdx)

			if groupKey == group {
				newSizeVal := make([]byte, 4)
				copy(newSizeVal, val[:4])

				if err := batch.Set(sizeKey, newSizeVal, pebble.NoSync); err != nil {
					return xerrors.Errorf("dropgroup set (sk): %w", err)
				}
			}
		}

		if err := closer.Close(); err != nil {
			return xerrors.Errorf("delget(s:) close: %w", err)
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return xerrors.Errorf("dropgroup commit: %w", err)
	}

	// todo: gc size keys

	return nil
}

const averageEntrySize = 35 + 8 // multihash is ~35 bytes, groupkey is 8 bytes

func (i *PebbleIndex) EstimateSize(ctx context.Context) (int64, error) {
	lowerBound := []byte("i:")
	upperBound := []byte("s;")
	estimatedSize, err := i.db.EstimateDiskUsage(lowerBound, upperBound)

	if err != nil {
		return 0, err
	}

	estimatedEntries := int64(estimatedSize) / averageEntrySize
	return estimatedEntries, nil
}

func (i *PebbleIndex) Close() error {
	return i.db.Close()
}

var _ iface.Index = (*PebbleIndex)(nil)
