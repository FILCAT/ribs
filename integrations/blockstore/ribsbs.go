package ribsbstore

import (
	"context"
	ipld "github.com/ipfs/go-ipld-format"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/lotus-web3/ribs"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

type Blockstore struct {
	r ribs.RIBS

	sess ribs.Session
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
	bt := b.sess.Batch(ctx)
	if err := bt.Put(ctx, []blocks.Block{block}); err != nil {
		return err
	}
	return bt.Flush(ctx)
}

func (b *Blockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	bt := b.sess.Batch(ctx)
	if err := bt.Put(ctx, blocks); err != nil {
		return err
	}
	return bt.Flush(ctx)
}

func (b *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, xerrors.Errorf("too slow")
}

func (b *Blockstore) HashOnRead(enabled bool) {
	return
}

var _ blockstore.Blockstore = &Blockstore{}
