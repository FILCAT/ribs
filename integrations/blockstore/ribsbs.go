package ribsbstore

import (
	"context"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/lotus-web3/ribs"
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
}

func New(ctx context.Context, r ribs.RIBS) *Blockstore {
	b := &Blockstore{
		r:    r,
		sess: r.Session(ctx),
		puts: make(chan Request[[]blocks.Block, error], 64), // todo make this configurable
	}

	go b.start(ctx)
	return b
}

func (b *Blockstore) start(ctx context.Context) {
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

				if len(toPut) > 64 { // todo make this configurable
					break
				}
			}

			respondAll := func(err error) {
				for _, resp := range toRespond {
					resp <- err
				}
			}

			bt := b.sess.Batch(ctx)
			fmt.Println("putting", len(toPut), "blocks")
			err := bt.Put(ctx, toPut)
			if err != nil {
				respondAll(err)
				continue
			}

			err = bt.Flush(ctx)
			if err != nil {
				respondAll(err)
				continue
			}

			respondAll(nil)
		case <-ctx.Done():
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
	/*r, err := b.sess.Has(ctx, cidsToMhs([]cid.Cid{c}))
	if err != nil {
		return false, err
	}
	if len(r) == 0 {
		return false, xerrors.Errorf("no result")
	}
	return r[0], nil*/

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
	/*r, err := b.sess.GetSize(ctx, cidsToMhs([]cid.Cid{c}))
	if err != nil {
		return 0, err
	}

	if len(r) == 0 {
		return 0, xerrors.Errorf("no result")
	}

	if r[0] == -1 {
		return 0, ipld.ErrNotFound{Cid: c}
	}

	return int(r[0]), nil
	*/

	bk, err := b.Get(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(bk.RawData()), nil
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

func (b *Blockstore) HashOnRead(enabled bool) {
	return
}

var _ blockstore.Blockstore = &Blockstore{}
