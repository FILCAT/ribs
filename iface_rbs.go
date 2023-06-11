package ribs

import (
	"context"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

type GroupKey = int64

const UndefGroupKey = GroupKey(-1)

// User

type RBS interface {
	Session(ctx context.Context) Session
	Storage() Storage
	StorageDiag() RBSDiag

	// ExternalStorage manages offloaded data
	ExternalStorage() RBSExternalStorage

	// StagingStorage manages staged data (full non-replicated data)
	StagingStorage() RBSStagingStorage

	io.Closer
}

// Batch groups operations, NOT thread safe
type Batch interface {
	// View is like See Session.View, and all constraints apply, but with batch
	// operations applied
	// todo: is this useful, is this making things too complicated? is this disabling some optimisations?
	//View(ctx context.Context, c []cid.Cid, cb func(cidx int, data []byte)) error

	// Put queues writes to the blockstore
	Put(ctx context.Context, b []blocks.Block) error

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
	// * Callback will not be called for indexes where data is not found
	// * Callback `data` must not be referenced after the function returns
	//   If the data is to be used after returning from the callback, it MUST be copied.
	View(ctx context.Context, c []multihash.Multihash, cb func(cidx int, data []byte)) error

	// -1 means not found
	GetSize(ctx context.Context, c []multihash.Multihash, cb func([]int32) error) error

	Batch(ctx context.Context) Batch
}

type Storage interface {
	FindHashes(ctx context.Context, hashes multihash.Multihash) ([]GroupKey, error)

	ReadCar(ctx context.Context, group GroupKey, out io.Writer) error

	// HashSample returns a sample of hashes from the group saved when the group was finalized
	HashSample(ctx context.Context, group GroupKey) ([]multihash.Multihash, error)

	DescibeGroup(ctx context.Context, group GroupKey) (GroupDesc, error)

	Offload(ctx context.Context, group GroupKey) error

	Subscribe(GroupSub)
}

type GroupDesc struct {
	RootCid, PieceCid cid.Cid
}

type OffloadLoader interface {
	View(ctx context.Context, g GroupKey, c []multihash.Multihash, cb func(cidx int, data []byte)) error
}

type GroupSub func(group GroupKey, from, to GroupState)

type RBSDiag interface {
	Groups() ([]GroupKey, error)
	GroupMeta(gk GroupKey) (GroupMeta, error)

	TopIndexStats(context.Context) (TopIndexStats, error)
	GetGroupStats() (*GroupStats, error)
	GroupIOStats() GroupIOStats
}

/* Deal diag */

type GroupMeta struct {
	State GroupState

	MaxBlocks int64
	MaxBytes  int64

	Blocks int64
	Bytes  int64

	ReadBlocks, ReadBytes   int64
	WriteBlocks, WriteBytes int64

	DealCarSize *int64 // todo move to DescribeGroup
}

type GroupStats struct {
	GroupCount           int64
	TotalDataSize        int64
	NonOffloadedDataSize int64
	OffloadedDataSize    int64

	OpenGroups, OpenWritable int
}

type GroupIOStats struct {
	ReadBlocks, ReadBytes   int64
	WriteBlocks, WriteBytes int64
}

type TopIndexStats struct {
	Entries       int64
	Writes, Reads int64
}

/* Storage internal */

// Index is the top level index, thread safe
type Index interface {
	// GetGroups gets group ids for the multihashes
	GetGroups(ctx context.Context, mh []multihash.Multihash, cb func([][]GroupKey) (more bool, err error)) error
	GetSizes(ctx context.Context, mh []multihash.Multihash, cb func([]int32) error) error

	AddGroup(ctx context.Context, mh []multihash.Multihash, sizes []int32, group GroupKey) error

	Sync(ctx context.Context) error
	DropGroup(ctx context.Context, mh []multihash.Multihash, group GroupKey) error
	EstimateSize(ctx context.Context) (int64, error)

	io.Closer
}

type GroupState int

const (
	GroupStateWritable GroupState = iota
	GroupStateFull
	GroupStateVRCARDone

	GroupStateLocalReadyForDeals
	GroupStateOffloaded

	// GroupStateDownloading
)

// Group stores a bunch of blocks, abstracting away the storage backend.
// All underlying storage backends contain all blocks referenced by the group in
// the top level index.
// Group IS thread safe
type Group interface {
	// Put returns the number of blocks written
	Put(ctx context.Context, c []blocks.Block) (int, error)
	Unlink(ctx context.Context, c []multihash.Multihash) error
	View(ctx context.Context, c []multihash.Multihash, cb func(cidx int, data []byte)) error
	Sync(ctx context.Context) error

	// Finalize marks the group as finalized, meaning no more writes will be accepted,
	// a more optimized index will get generated, then the gropu will be queued for
	// replication / offloading.
	Finalize(ctx context.Context) error

	io.Closer
}

type RBSExternalStorage interface {
	InstallProvider(ExternalStorageProvider)
}

type ExternalStorageProvider interface {
	FetchBlocks(ctx context.Context, group GroupKey, mh []multihash.Multihash, cb func(cidx int, data []byte)) error
}

type RBSStagingStorage interface {
	InstallStagingProvider(StagingStorageProvider)
}

type StagingStorageProvider interface {
	Upload(ctx context.Context, group GroupKey, src func() io.Reader) error
	ReadCar(ctx context.Context, group GroupKey, off, size int64) (io.ReadCloser, error)
	Release(ctx context.Context, group GroupKey) error
	URL(ctx context.Context, group GroupKey) (string, error)
}
