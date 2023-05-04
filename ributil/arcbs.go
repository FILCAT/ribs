package ributil

import (
	"context"
	"github.com/filecoin-project/lotus/blockstore"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	block "github.com/ipfs/go-libipfs/blocks"
	"strings"
)

type ArcBlockstore struct {
	write blockstore.Blockstore
	cache *lru.ARCCache[cid.Cid, block.Block]
}

var CacheBstoreSize = (2048 << 20) / 16000 // 2G with average block size of 16KB

func ARCStore(base blockstore.Blockstore) *ArcBlockstore {
	c, _ := lru.NewARC[cid.Cid, block.Block](CacheBstoreSize)

	bs := &ArcBlockstore{
		write: base,

		cache: c,
	}
	return bs
}

var (
	_ blockstore.Blockstore = (*ArcBlockstore)(nil)
	_ blockstore.Viewer     = (*ArcBlockstore)(nil)
)

func (bs *ArcBlockstore) Flush(ctx context.Context) error {
	return bs.write.Flush(ctx)
}

func (bs *ArcBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return bs.write.AllKeysChan(ctx)
}

func (bs *ArcBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	bs.cache.Remove(c)
	return bs.write.DeleteBlock(ctx, c)
}

func (bs *ArcBlockstore) DeleteMany(ctx context.Context, cids []cid.Cid) error {
	for _, c := range cids {
		bs.cache.Remove(c)
	}
	return bs.write.DeleteMany(ctx, cids)
}

func (bs *ArcBlockstore) View(ctx context.Context, c cid.Cid, callback func([]byte) error) error {
	if blk, ok := bs.cache.Get(c); ok {
		return callback(blk.RawData())
	}

	return bs.write.View(ctx, c, func(bytes []byte) error {
		blk, err := block.NewBlockWithCid(bytes, c)
		if err != nil {
			return err
		}
		bs.cache.Add(c, blk)

		return callback(bytes)
	})
}

func (bs *ArcBlockstore) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	if blk, ok := bs.cache.Get(c); ok {
		return blk, nil
	}

	return bs.write.Get(ctx, c)
}

func (bs *ArcBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if blk, ok := bs.cache.Get(c); ok {
		return len(blk.RawData()), nil
	}

	b, err := bs.Get(ctx, c)
	if err != nil {
		if strings.Contains(err.Error(), "ipld: could not find") {
			return 0, &ipld.ErrNotFound{Cid: c}
		}
		return 0, err
	}

	return len(b.RawData()), nil
}

func (bs *ArcBlockstore) Put(ctx context.Context, blk block.Block) error {
	bs.cache.Add(blk.Cid(), blk)

	return nil
}

func (bs *ArcBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if bs.cache.Contains(c) {
		return true, nil
	}

	return bs.write.Has(ctx, c)
}

func (bs *ArcBlockstore) HashOnRead(hor bool) {
	bs.write.HashOnRead(hor)
}

func (bs *ArcBlockstore) PutMany(ctx context.Context, blks []block.Block) error {
	for _, blk := range blks {
		if bs.cache.Contains(blk.Cid()) {
			continue
		}

		bs.cache.Add(blk.Cid(), blk)
	}

	return nil
}
