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
	db *pebble.DB

	// todo limit size somehow
	iterPool sync.Pool
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

func (i *PebbleIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func([][]iface.GroupKey) (more bool, err error)) error {
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
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(group))

	batch := i.db.NewBatch()
	defer batch.Close()

	for _, m := range mh {
		key := append(append([]byte("i:"), m...), b...)
		if err := batch.Set(key, b, pebble.NoSync); err != nil {
			return xerrors.Errorf("addgroup set: %w", err)
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return xerrors.Errorf("addgroup commit: %w", err)
	}

	return nil
}

func (i *PebbleIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(group))

	batch := i.db.NewBatch()
	defer batch.Close()

	for _, m := range mh {
		key := append(append([]byte("i:"), m...), b...)
		if err := batch.Delete(key, pebble.NoSync); err != nil {
			return xerrors.Errorf("dropgroup delete: %w", err)
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return xerrors.Errorf("dropgroup commit: %w", err)
	}

	return nil
}

const averageEntrySize = 56 // multihash is ~36 bytes, groupkey is 8 bytes

func (i *PebbleIndex) EstimateSize(ctx context.Context) (int64, error) {
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
