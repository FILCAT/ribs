package ribs

import (
	"context"
	"github.com/multiformats/go-multihash"
)

type GroupKey = int64

const UndefGroupKey = GroupKey(-1)

// Index is the top level index, thread safe
type Index interface {
	// GetGroups gets group ids for the multihashes
	GetGroups(ctx context.Context, mh []multihash.Multihash, cb func([][]GroupKey) (bool, error)) error
	AddGroup(ctx context.Context, mh []multihash.Multihash, group GroupKey) error
	DropGroup(ctx context.Context, mh []multihash.Multihash, group GroupKey) error
}

// Group stores a bunch of blocks, abstracting away the storage backend.
// All underlying storage backends contain all blocks referenced by the group in
// the top level index.
type Group interface {
	// Data interface

	// Put returns the number of blocks written
	Put(ctx context.Context, c []multihash.Multihash, datas [][]byte) (int, error)
	Unlink(ctx context.Context, c []multihash.Multihash) error
	View(ctx context.Context, c []multihash.Multihash, cb func(cidx int, data []byte)) error

	Close() error
	Sync(ctx context.Context) error
}

// user

// Batch groups operations, NOT thread safe
type Batch interface {
	// View is like See Session.View, and all constraints apply, but with batch
	// operations applied
	// todo: is this useful, is this making things too complicated? is this disabling some optimisations?
	//View(ctx context.Context, c []cid.Cid, cb func(cidx int, data []byte)) error

	// Put queues writes to the blockstore
	Put(ctx context.Context, c []multihash.Multihash, datas [][]byte) error

	// Unlink makes a blocks not retrievable from the blockstore
	// NOTE: this method is best-effort. Data may not be removed immediately,
	// and it may be retrievable even after the operation is committed
	// In case of conflicts, Put operation will be preferred over Unlink
	Unlink(ctx context.Context, c []multihash.Multihash) error

	// Flush commits data to the blockstore. The batch can be reused after commit
	Flush(ctx context.Context) error

	// todo? Fork(ctx) (Batch,error) for threaded
}

// Session groups correlated IO operations; thread safa
type Session interface {
	// View attempts to read a list of cids
	// NOTE:
	// * Callback calls can happen out of order
	// * Callback calls can happen in parallel
	// * Callback `data` will be nil when block is not found
	// * Callback `data` must not be referenced after the function returns
	//   If the data is to be used after returning from the callback, it MUST be copied.
	View(ctx context.Context, c []multihash.Multihash, cb func(cidx int, data []byte)) error

	Batch(ctx context.Context) Batch
}

type RIBS interface {
	Session(ctx context.Context) Session
}
