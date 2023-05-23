package ributil

import (
	"context"
	"fmt"
	"github.com/filecoin-project/lotus/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
	"io"
)

type IpldStoreWrapper struct {
	BS blockstore.Blockstore
}

func (b *IpldStoreWrapper) Get(ctx context.Context, key string) ([]byte, error) {
	keyCid, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("bad CID key: %w", err)
	}

	bk, err := b.BS.Get(ctx, keyCid)
	if err != nil {
		return nil, err
	}

	return bk.RawData(), nil
}

func (b *IpldStoreWrapper) Has(ctx context.Context, key string) (bool, error) {
	keyCid, err := cid.Cast([]byte(key))
	if err != nil {
		return false, fmt.Errorf("bad CID key: %w", err)
	}

	return b.BS.Has(ctx, keyCid)
}

func (b *IpldStoreWrapper) Put(ctx context.Context, key string, content []byte) error {
	keyCid, err := cid.Cast([]byte(key))
	if err != nil {
		return fmt.Errorf("bad CID key: %w", err)
	}

	log.Errorw("putting block", "keyCid", keyCid)

	bk, err := blocks.NewBlockWithCid(content, keyCid)
	if err != nil {
		return fmt.Errorf("bad block: %w", err)
	}

	return b.BS.Put(ctx, bk)
}

// BlockWriteOpener returns a BlockWriteOpener that operates on this storage.
func (b *IpldStoreWrapper) BlockWriteOpener() linking.BlockWriteOpener {
	return func(lctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		wr, wrcommit, err := ipldstorage.PutStream(lctx.Ctx, b)
		return wr, func(lnk ipld.Link) error {
			return wrcommit(lnk.Binary())
		}, err
	}
}

var _ ipldstorage.WritableStorage = (*IpldStoreWrapper)(nil)
var _ ipldstorage.ReadableStorage = (*IpldStoreWrapper)(nil)
