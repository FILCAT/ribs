package ribsbstore

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/atboosty/ribs"
	lotusbstore "github.com/filecoin-project/lotus/blockstore"
	blockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

type Request[P, R any] struct {
	Param P
	Resp  chan R
}

func MakeRequest[P, R any](param P) Request[P, R] {
	return Request[P, R]{
		Param: param,
		Resp:  make(chan R, 1),
	}
}

type Blockstore struct {
	r ribs.RIBS

	sess ribs.Session

	puts chan Request[[]blocks.Block, error]

	flushReq atomic.Int64
	flushPos atomic.Int64

	flush chan struct{}

	stop, stopped chan struct{}
}

var _ blockstore.Blockstore = &Blockstore{}
var _ lotusbstore.Flusher = &Blockstore{}

func New(ctx context.Context, r ribs.RIBS) *Blockstore {
	b := &Blockstore{
		r:    r,
		sess: r.Session(ctx),
		puts: make(chan Request[[]blocks.Block, error], 64), // todo make this configurable

		flush: make(chan struct{}, 1),

		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}

	go b.start(ctx)
	return b
}

var (
	BlockstoreMaxQueuedBlocks    = 64
	BlockstoreMaxUnflushedBlocks = 1024
)

func (b *Blockstore) start(ctx context.Context) {
	var bt ribs.Batch
	var unflushed int

	defer func() {
		if bt != nil {
			err := bt.Flush(ctx)
			if err != nil {
				fmt.Println("failed to flush batch", "error", err) // todo log
			} else {
				fmt.Println("flushed batch on close") // todo log
			}
		}

		close(b.stopped)
	}()

	flushBatch := func() error {
		select { // if any requests are queued, consume them
		case <-b.flush:
		default:
		}

		newPos := b.flushReq.Load()
		defer b.flushPos.Store(newPos)

		if bt == nil {
			return nil
		}

		err := bt.Flush(ctx)
		if err != nil {
			fmt.Println("failed to flush batch", "error", err) // todo log
			// todo PANIK (make all subsequent calls to this blockstore fail)
			return err
		}
		unflushed = 0
		bt = nil

		return nil
	}

	for {
		select {
		case req := <-b.puts:
			var toPut []blocks.Block
			var toRespond []chan<- error

			toPut = append(toPut, req.Param...)
			toRespond = append(toRespond, req.Resp)

		loop:
			for {
				select {
				case req := <-b.puts:
					toPut = append(toPut, req.Param...)
					toRespond = append(toRespond, req.Resp)
				default:
					break loop
				}

				if len(toPut) > BlockstoreMaxQueuedBlocks { // todo make this configurable
					break
				}
			}

			respondAll := func(err error) {
				for _, resp := range toRespond {
					resp <- err
				}
			}

			if bt == nil {
				bt = b.sess.Batch(ctx)
			}

			err := bt.Put(ctx, toPut)
			if err != nil {
				respondAll(err)
				continue
			}

			unflushed += len(toPut)
			if unflushed > BlockstoreMaxUnflushedBlocks { // todo make this configurable
				err = flushBatch()
				if err != nil {
					respondAll(err) // todo: not perfect but better than blocking all writes (?)
					continue
				}
			}

			respondAll(nil)
		case <-b.flush:
			err := flushBatch()
			if err != nil {
				fmt.Println("failed to flush batch", "error", err) // todo log
				// todo PANIK (make all subsequent calls to this blockstore fail)
				return
			}
		case <-b.stop:
			return
		}
	}
}

func cidsToMhs(cids []cid.Cid) []multihash.Multihash {
	mhs := make([]multihash.Multihash, len(cids))
	for i, c := range cids {
		mhs[i] = c.Hash()
	}
	return mhs
}

func (b *Blockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	bt := b.sess.Batch(ctx)
	if err := bt.Unlink(ctx, cidsToMhs([]cid.Cid{c})); err != nil {
		return err
	}
	return bt.Flush(ctx)
}

func (b *Blockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, err := b.GetSize(ctx, c)
	if ipld.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *Blockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	var out blocks.Block

	// todo test not found
	err := b.sess.View(ctx, cidsToMhs([]cid.Cid{c}), func(cidx int, data []byte) {
		dcopy := make([]byte, len(data))
		copy(dcopy, data)

		out, _ = blocks.NewBlockWithCid(dcopy, c)
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, ipld.ErrNotFound{Cid: c}
	}

	return out, err
}

func (b *Blockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	var r int32

	err := b.sess.GetSize(ctx, cidsToMhs([]cid.Cid{c}), func(sz []int32) error {
		if len(sz) != 1 {
			return xerrors.Errorf("expected 1 result")
		}

		r = sz[0]

		return nil
	})
	if err != nil {
		return 0, err
	}

	if r == -1 {
		return 0, ipld.ErrNotFound{Cid: c}
	}

	return int(r), nil
}

func (b *Blockstore) Put(ctx context.Context, block blocks.Block) error {
	req := MakeRequest[[]blocks.Block, error]([]blocks.Block{block})
	select {
	case b.puts <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.Resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Blockstore) PutMany(ctx context.Context, blk []blocks.Block) error {
	req := MakeRequest[[]blocks.Block, error](blk)
	select {
	case b.puts <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.Resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, xerrors.Errorf("too slow")
}

func (b *Blockstore) HashOnRead(bool) {}

func (b *Blockstore) Flush(ctx context.Context) error {
	// note "now"
	now := b.flushReq.Add(1)

	// tell the flusher to flush
	select {
	case b.flush <- struct{}{}:
	default:
		// the channel is buffered, so just merge flushes
	}

	// wait for the flush to complete
	// todo: use a cancellable cond-like thing here
	for {
		if b.flushPos.Load() >= now {
			return nil
		}

		select {
		case <-b.stop:
			return xerrors.Errorf("flush interrupted")
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond * 10):
		}
	}
}

func (b *Blockstore) Close() error {
	close(b.stop)
	<-b.stopped
	return nil
}
