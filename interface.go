package ribs

import (
	"context"
	blocks "github.com/ipfs/go-block-format"
	"io"

	"github.com/multiformats/go-multihash"
)

type GroupKey = int64

const UndefGroupKey = GroupKey(-1)

// Index is the top level index, thread safe
type Index interface {
	// GetGroups gets group ids for the multihashes
	GetGroups(ctx context.Context, mh []multihash.Multihash, cb func([][]GroupKey) (more bool, err error)) error
	AddGroup(ctx context.Context, mh []multihash.Multihash, group GroupKey) error
	DropGroup(ctx context.Context, mh []multihash.Multihash, group GroupKey) error
}

type GroupState int

const (
	GroupStateWritable GroupState = iota
	GroupStateFull
	GroupStateBSSTExists
	GroupStateLevelIndexDropped
	GroupStateVRCARDone
	GroupStateHasCommp
	GroupStateDealsInProgress
	GroupStateDealsDone
	GroupStateOffloaded
)

// Group stores a bunch of blocks, abstracting away the storage backend.
// All underlying storage backends contain all blocks referenced by the group in
// the top level index.
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

// user

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

	Has(ctx context.Context, c []multihash.Multihash) ([]bool, error)

	// -1 means not found
	GetSize(ctx context.Context, c []multihash.Multihash) ([]int64, error)

	Batch(ctx context.Context) Batch
}

type RIBS interface {
	Session(ctx context.Context) Session
	Diagnostics() Diag

	io.Closer
}

type GroupMeta struct {
	State GroupState

	MaxBlocks int64
	MaxBytes  int64

	Blocks int64
	Bytes  int64

	Deals []DealMeta
}

type DealMeta struct {
	UUID     string
	Provider int64

	Status     string
	SealStatus string
	Error      string
	DealID     int64

	BytesRecv int64
	TxSize    int64
	PubCid    string
}

type Diag interface {
	Groups() ([]GroupKey, error)
	GroupMeta(gk GroupKey) (GroupMeta, error)

	CarUploadStats() map[GroupKey]*UploadStats

	CrawlState() string
	ReachableProviders() []ProviderMeta
}

type UploadStats struct {
	ActiveRequests       int
	Last250MsUploadBytes int64
}

type ProviderMeta struct {
	ID     int64
	PingOk bool

	BoostDeals     bool
	BoosterHttp    bool
	BoosterBitswap bool

	IndexedSuccess int64
	IndexedFail    int64

	DealAttempts int64
	DealSuccess  int64
	DealFail     int64

	RetrProbeSuccess int64
	RetrProbeFail    int64
	RetrProbeBlocks  int64
	RetrProbeBytes   int64

	// price in fil/gib/epoch
	AskPrice         float64
	AskVerifiedPrice float64

	AskMinPieceSize float64
	AskMaxPieceSize float64
}
