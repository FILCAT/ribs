package impl

import (
	"context"
	"fmt"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipfs/go-libipfs/blocks"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/jbob"
	"github.com/lotus-web3/ribs/ributil"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"os"
	"path/filepath"
	"time"
)

func (m *Group) Finalize(ctx context.Context) error {
	m.jblk.Lock()
	defer m.jblk.Unlock()

	if m.state != iface.GroupStateFull {
		return xerrors.Errorf("group not in state for finalization: %d", m.state)
	}

	if err := m.jb.MarkReadOnly(); err != nil && err != jbob.ErrReadOnly {
		return xerrors.Errorf("mark read-only: %w", err)
	}

	if err := m.jb.Finalize(); err != nil {
		return xerrors.Errorf("finalize jbob: %w", err)
	}

	if err := m.advanceState(ctx, iface.GroupStateBSSTExists); err != nil {
		return xerrors.Errorf("mark bsst exists: %w", err)
	}

	if err := m.jb.DropLevel(); err != nil {
		return xerrors.Errorf("removing leveldb index: %w", err)
	}

	if err := m.advanceState(ctx, iface.GroupStateLevelIndexDropped); err != nil {
		return xerrors.Errorf("mark level index dropped: %w", err)
	}

	return nil
}

func (m *Group) GenTopCar(ctx context.Context) error {
	m.jblk.RLock()
	defer m.jblk.RUnlock()

	if err := os.MkdirAll(filepath.Join(m.path, "vcar"), 0755); err != nil {
		return xerrors.Errorf("make vcar dir: %w", err)
	}

	if m.state != iface.GroupStateLevelIndexDropped {
		return xerrors.Errorf("group not in state for generating top CAR: %d", m.state)
	}

	level := 1
	const arity = 2048
	var links []cid.Cid
	var nextLevelLinks []cid.Cid

	makeLinkBlock := func() (blocks.Block, error) {
		nd, err := cbor.WrapObject(links, mh.SHA2_256, -1)
		if err != nil {
			return nil, xerrors.Errorf("wrap links: %w", err)
		}

		links = links[:0]

		nextLevelLinks = append(nextLevelLinks, nd.Cid())

		return nd, nil
	}

	fname := filepath.Join(m.path, "vcar", fmt.Sprintf("layer%d.cardata", level))
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_APPEND|os.O_TRUNC|os.O_WRONLY, 0644)
	outCdata := &cardata{
		f: f,
	}

	err = m.jb.Iterate(func(c mh.Multihash, data []byte) error {
		link := mhToRawCid(c)
		links = append(links, link)

		if len(links) == arity {
			bk, err := makeLinkBlock()
			if err != nil {
				return xerrors.Errorf("make link block: %w", err)
			}

			if err := outCdata.writeBlock(bk.Cid(), bk.RawData()); err != nil {
				return xerrors.Errorf("writing jbob link block: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return xerrors.Errorf("iterate jbob: %w", err)
	}

	if len(links) > 0 {
		bk, err := makeLinkBlock()
		if err != nil {
			return xerrors.Errorf("make link block: %w", err)
		}

		if err := outCdata.writeBlock(bk.Cid(), bk.RawData()); err != nil {
			return xerrors.Errorf("writing jbob link final block: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return xerrors.Errorf("close level 1: %w", err)
	}

	for {
		level++
		fname := filepath.Join(m.path, "vcar", fmt.Sprintf("layer%d.cardata", level))
		f, err := os.OpenFile(fname, os.O_CREATE|os.O_APPEND|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return xerrors.Errorf("open cardata file: %w", err)
		}

		outCdata = &cardata{
			f: f,
		}

		prevLevelLinks := nextLevelLinks

		// this actually works because nextLevelLinks grow slower than prevLevelLinks
		nextLevelLinks = nextLevelLinks[:0]

		for _, link := range prevLevelLinks {
			links = append(links, link)

			if len(links) == arity {
				bk, err := makeLinkBlock()
				if err != nil {
					return xerrors.Errorf("make link block: %w", err)
				}

				if err := outCdata.writeBlock(bk.Cid(), bk.RawData()); err != nil {
					return xerrors.Errorf("writing link block: %w", err)
				}
			}
		}

		if len(links) > 0 {
			bk, err := makeLinkBlock()
			if err != nil {
				return xerrors.Errorf("make link block: %w", err)
			}

			if err := outCdata.writeBlock(bk.Cid(), bk.RawData()); err != nil {
				return xerrors.Errorf("writing link block: %w", err)
			}
		}
		if err := f.Close(); err != nil {
			return xerrors.Errorf("close level %d: %w", level, err)
		}

		if len(prevLevelLinks) == 1 {
			break
		}
	}

	if err := os.WriteFile(filepath.Join(m.path, "vcar", "layers"), []byte(fmt.Sprintf("%d", level)), 0644); err != nil {
		return xerrors.Errorf("write layers file: %w", err)
	}
	if err := os.WriteFile(filepath.Join(m.path, "vcar", "arity"), []byte(fmt.Sprintf("%d", arity)), 0644); err != nil {
		return xerrors.Errorf("write arity file: %w", err)
	}

	if err := m.advanceState(ctx, iface.GroupStateVRCARDone); err != nil {
		return xerrors.Errorf("mark level index dropped: %w", err)
	}

	return nil
}

func (m *Group) GenCommP() error {
	if m.state != iface.GroupStateVRCARDone {
		return xerrors.Errorf("group not in state for generating top CAR: %d", m.state)
	}

	cc := new(ributil.DataCidWriter)

	start := time.Now()

	carSize, root, err := m.writeCar(cc)
	if err != nil {
		return xerrors.Errorf("write car: %w", err)
	}

	sum, err := cc.Sum()
	if err != nil {
		panic(err)
	}

	log.Infow("generated commP", "duration", time.Since(start), "commP", sum.PieceCID, "pps", sum.PieceSize, "mbps", float64(carSize)/time.Since(start).Seconds()/1024/1024)

	p, _ := commcid.CIDToDataCommitmentV1(sum.PieceCID)

	if err := m.setCommP(context.Background(), iface.GroupStateHasCommp, p, int64(sum.PieceSize), root, carSize); err != nil {
		return xerrors.Errorf("set commP: %w", err)
	}

	return nil
}

func (m *Group) advanceState(ctx context.Context, st iface.GroupState) error {
	m.dblk.Lock()
	defer m.dblk.Unlock()

	m.state = st

	// todo enter failed state on error
	return m.db.SetGroupState(ctx, m.id, st)
}

func (m *Group) setCommP(ctx context.Context, state iface.GroupState, commp []byte, paddedPieceSize int64, root cid.Cid, carSize int64) error {
	m.dblk.Lock()
	defer m.dblk.Unlock()

	m.state = state

	// todo enter failed state on error
	return m.db.SetCommP(ctx, m.id, state, commp, paddedPieceSize, root, carSize)
}
