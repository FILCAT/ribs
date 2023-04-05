package impl

import (
	"context"
	"encoding/binary"
	iface "github.com/lotus-web3/ribs"
	"golang.org/x/xerrors"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/multiformats/go-multihash"
)

// PebbleIndex is the top-level index, thread-safe.
type PebbleIndex struct {
	mu sync.RWMutex
	db *pebble.DB

	// todo limit size somehow
	iterPool sync.Pool
}

// NewIndex creates a new Pebble-backed Index.
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

func (i *PebbleIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func([][]iface.GroupKey) (more bool, err error)) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	groups := make([][]iface.GroupKey, len(mh))

	for idx, m := range mh {
		keyPrefix := append([]byte("i:"), m...)
		iter := i.iterPool.Get().(*pebble.Iterator)
		iter.SetBounds(keyPrefix, append(keyPrefix, 0xff))

		var gkList []iface.GroupKey
		for iter.SeekGE(keyPrefix); iter.Valid(); iter.Next() {
			groupKeyBytes, err := iter.ValueAndErr()
			if err != nil {
				return xerrors.Errorf("iterate: %w", err)
			}

			groupKey := binary.BigEndian.Uint64(groupKeyBytes)

			gkList = append(gkList, iface.GroupKey(groupKey))
		}

		if err := iter.Error(); err != nil {
			i.iterPool.Put(iter)
			return xerrors.Errorf("iter error: %w", err)
		}

		i.iterPool.Put(iter)

		groups[idx] = gkList
	}

	more, err := cb(groups)
	if !more || err != nil {
		return err
	}

	return nil
}

func (i *PebbleIndex) AddGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(group))

	for _, m := range mh {
		key := append(append([]byte("i:"), m...), b...)
		if err := i.db.Set(key, b, pebble.NoSync); err != nil { // todo nosync plus flush
			return xerrors.Errorf("addgroup set: %w", err)
		}
	}

	if err := i.db.Flush(); err != nil {
		return xerrors.Errorf("addgroup flush: %w", err)
	}

	return nil
}

func (i *PebbleIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(group))

	for _, m := range mh {
		key := append(append([]byte("i:"), m...), b...)
		if err := i.db.Delete(key, pebble.NoSync); err != nil {
			return xerrors.Errorf("dropgroup delete: %w", err)
		}
	}

	if err := i.db.Flush(); err != nil {
		return xerrors.Errorf("dropgroup flush: %w", err)
	}

	return nil
}

const averageEntrySize = 56 // multihash is ~36 bytes, groupkey is 8 bytes

func (i *PebbleIndex) EstimateSize(ctx context.Context) (int64, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	lowerBound := []byte("i:")
	upperBound := []byte("i;")
	estimatedSize, err := i.db.EstimateDiskUsage(lowerBound, upperBound)

	if err != nil {
		return 0, err
	}

	estimatedEntries := int64(estimatedSize) / averageEntrySize
	return estimatedEntries, nil
}

var _ iface.Index = (*PebbleIndex)(nil)
