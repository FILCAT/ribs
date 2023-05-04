package ributil

import (
	"context"
	"github.com/filecoin-project/lotus/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"golang.org/x/xerrors"
	"strings"
	"sync"
	"time"
)

type FallbackStore struct {
	blockstore.Blockstore

	lk sync.RWMutex
	// missFn is the function that will be invoked on a local miss to pull the
	// block from elsewhere.
	missFn func(context.Context, cid.Cid) (blocks.Block, error)
}

func (fbs *FallbackStore) SetFallback(missFn func(context.Context, cid.Cid) (blocks.Block, error)) {
	fbs.lk.Lock()
	defer fbs.lk.Unlock()

	fbs.missFn = missFn
}

func (fbs *FallbackStore) getFallback(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	log.Warnf("fallbackstore: block not found locally, fetching from the network; cid: %s", c)
	fbs.lk.RLock()
	defer fbs.lk.RUnlock()

	if fbs.missFn == nil {
		// FallbackStore wasn't configured yet (chainstore/bitswap aren't up yet)
		// Wait for a bit and retry
		fbs.lk.RUnlock()
		time.Sleep(5 * time.Second)
		fbs.lk.RLock()

		if fbs.missFn == nil {
			log.Errorw("fallbackstore: missFn not configured yet")
			return nil, ipld.ErrNotFound{Cid: c}
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	b, err := fbs.missFn(ctx, c)
	if err != nil {
		return nil, err
	}

	// chain bitswap puts blocks in temp blockstore which is cleaned up
	// every few min (to drop any messages we fetched but don't want)
	// in this case we want to keep this block around
	if err := fbs.Put(ctx, b); err != nil {
		return nil, xerrors.Errorf("persisting fallback-fetched block: %w", err)
	}
	return b, nil
}

func (fbs *FallbackStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	b, err := fbs.Blockstore.Get(ctx, c)
	switch {
	case err == nil:
		return b, nil
	case ipld.IsNotFound(err) || strings.Contains(err.Error(), "ipld: could not find"):
		return fbs.getFallback(ctx, c)
	default:
		return b, xerrors.Errorf("fbs get: %w", err)
	}
}

func (fbs *FallbackStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	sz, err := fbs.Blockstore.GetSize(ctx, c)
	switch {
	case err == nil:
		return sz, nil
	case ipld.IsNotFound(err) || strings.Contains(err.Error(), "ipld: could not find"):
		b, err := fbs.getFallback(ctx, c)
		if err != nil {
			return 0, err
		}
		return len(b.RawData()), nil
	default:
		return sz, xerrors.Errorf("fbs getsize: %w", err)
	}
}

func (fbs *FallbackStore) View(ctx context.Context, c cid.Cid, callback func([]byte) error) error {
	err := fbs.Blockstore.View(ctx, c, callback)
	switch {
	case err == nil:
		return nil
	case ipld.IsNotFound(err) || strings.Contains(err.Error(), "ipld: could not find"):
		b, err := fbs.getFallback(ctx, c)
		if err != nil {
			return err
		}
		return callback(b.RawData())
	default:
		return xerrors.Errorf("fbs view: %w", err)
	}
}
