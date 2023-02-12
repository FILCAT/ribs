package impl

import (
	"bufio"
	"context"
	"fmt"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/jbob"
	mh "github.com/multiformats/go-multihash"
	"io"
	"path/filepath"
	"strconv"
	"sync"

	"golang.org/x/xerrors"
	"os"
)

var (
	// 100MB for now
	// TODO: make this configurable
	maxGroupSize int64 = 6000 << 20

	// todo enforce this
	maxGroupBlocks int64 = 20 << 20
)

type Group struct {
	db    *ribsDB
	index iface.Index

	path string
	id   int64

	state iface.GroupState

	// db lock
	// note: can be taken when jblk is held
	dblk sync.Mutex

	// jbob (with jblk)

	jblk sync.RWMutex

	inflightBlocks int64
	inflightSize   int64

	committedBlocks int64
	committedSize   int64

	jb *jbob.JBOB
}

func OpenGroup(db *ribsDB, index iface.Index, id, committedBlocks, committedSize int64, path string, state iface.GroupState, create bool) (*Group, error) {
	groupPath := filepath.Join(path, "grp", strconv.FormatInt(id, 32))

	if err := os.MkdirAll(groupPath, 0755); err != nil {
		return nil, xerrors.Errorf("create group directory: %w", err)
	}

	// open jbob

	jbOpenFunc := jbob.Open
	if create {
		jbOpenFunc = jbob.Create
	}

	jb, err := jbOpenFunc(filepath.Join(groupPath, "blk.jbmeta"), filepath.Join(groupPath, "blk.jblog"))
	if err != nil {
		return nil, xerrors.Errorf("open jbob: %w", err)
	}

	return &Group{
		db:    db,
		index: index,

		jb: jb,

		committedBlocks: committedBlocks,
		committedSize:   committedSize,

		path:  groupPath,
		id:    id,
		state: state,
	}, nil
}

func (m *Group) Put(ctx context.Context, b []blocks.Block) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	m.jblk.Lock()
	defer m.jblk.Unlock()

	// reserve space
	if m.state != iface.GroupStateWritable {
		return 0, nil
	}

	availSpace := maxGroupSize - m.committedSize // todo async - inflight

	var writeSize int64
	var writeBlocks int

	for _, blk := range b {
		if int64(len(blk.RawData()))+writeSize > availSpace {
			break
		}
		writeSize += int64(len(blk.RawData()))
		writeBlocks++
	}

	if writeBlocks < len(b) {
		// this group is full
		m.state = iface.GroupStateFull
	}

	m.inflightBlocks += int64(writeBlocks)
	m.inflightSize += writeSize

	// backend write

	// 1. (buffer) writes to jbob

	c := make([]mh.Multihash, len(b))
	for i, blk := range b {
		c[i] = blk.Cid().Hash()
	}

	err := m.jb.Put(c[:writeBlocks], b[:writeBlocks])
	if err != nil {
		// todo handle properly (abort, close, check disk space / resources, repopen)
		return 0, xerrors.Errorf("writing to jbob: %w", err)
	}

	// <todo async commit>

	// 2. commit jbob (so puts above are now on disk)

	at, err := m.jb.Commit()
	if err != nil {
		// todo handle properly (abort, close, check disk space / resources, repopen)
		return 0, xerrors.Errorf("committing jbob: %w", err)
	}

	// 3. write top-level index (before we update group head so replay is possible)
	err = m.index.AddGroup(ctx, c[:writeBlocks], m.id)
	if err != nil {
		// todo handle properly (abort, close, check disk space / resources, repopen)
		return 0, xerrors.Errorf("writing index: %w", err)
	}

	// 3.5 mark as read-only if full
	// todo is this the right place to do this?
	if m.state == iface.GroupStateFull {
		if err := m.jb.MarkReadOnly(); err != nil {
			// todo handle properly (abort, close, check disk space / resources, repopen)
			// todo combine with commit
			return 0, xerrors.Errorf("mark jbob read-only: %w", err)
		}
	}

	// 4. update head
	m.inflightBlocks -= int64(writeBlocks)
	m.inflightSize -= writeSize
	m.committedBlocks += int64(writeBlocks)
	m.committedSize += writeSize

	m.dblk.Lock()
	err = m.db.SetGroupHead(ctx, m.id, m.state, m.committedBlocks, m.committedSize, at)
	m.dblk.Unlock()
	if err != nil {
		// todo handle properly (retry, abort, close, check disk space / resources, repopen)
		return 0, xerrors.Errorf("update group head: %w", err)
	}

	// </todo async commit>

	return writeBlocks, nil
}

func (m *Group) Unlink(ctx context.Context, c []mh.Multihash) error {
	// write log

	// write idx

	// update head

	//TODO implement me
	panic("implement me")
}

func (m *Group) View(ctx context.Context, c []mh.Multihash, cb func(cidx int, data []byte)) error {
	// right now we just read from jbob
	return m.jb.View(c, func(cidx int, found bool, data []byte) error {
		// TODO: handle not found better?
		if !found {
			return xerrors.Errorf("group: block not found")
		}

		cb(cidx, data)
		return nil
	})
}

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
	defer m.jblk.RLock()

	if err := os.Mkdir(filepath.Join(m.path, "vcar"), 0755); err != nil {
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

	cc := new(commp.Calc)

	carSize, root, err := m.writeCar(cc)
	if err != nil {
		return xerrors.Errorf("write car: %w", err)
	}

	commP, pps, err := cc.Digest()
	if err != nil {
		panic(err)
	}

	fmt.Println("commP", commP)
	fmt.Println("pps", pps)

	if err := m.setCommP(context.Background(), iface.GroupStateHasCommp, commP, int64(pps), root, carSize); err != nil {
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

func (m *Group) Close() error {
	//TODO implement me
	panic("implement me")
}

func (m *Group) Sync(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

type sizerWriter struct {
	w io.Writer
	s int64
}

func (s *sizerWriter) Write(p []byte) (int, error) {
	w, err := s.w.Write(p)
	s.s += int64(w)
	return w, err
}

// returns car size and root cid
func (m *Group) writeCar(w io.Writer) (int64, cid.Cid, error) {
	// read layers file
	ls, err := os.ReadFile(filepath.Join(m.path, "vcar", "layers"))
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("read layers file: %w", err)
	}

	layerCount, err := strconv.Atoi(string(ls))
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("parse layers file: %w", err)
	}

	// read arity file
	as, err := os.ReadFile(filepath.Join(m.path, "vcar", "arity"))
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("read arity file: %w", err)
	}

	arity, err := strconv.Atoi(string(as))
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("parse arity file: %w", err)
	}

	var layers []*cardata

	for i := 1; i <= layerCount; i++ {
		fname := filepath.Join(m.path, "vcar", fmt.Sprintf("layer%d.cardata", i))
		f, err := os.OpenFile(fname, os.O_RDONLY, 0644)
		if err != nil {
			return 0, cid.Undef, xerrors.Errorf("open cardata file: %w", err)
		}

		layers = append(layers, &cardata{
			f:  f,
			br: bufio.NewReader(f),
		})
	}

	defer func() {
		for n, l := range layers {
			if err := l.f.Close(); err != nil {
				log.Warnf("closing cardata layer %d file: %s", n+1, err)
			}
		}
	}()

	// read root block, which is the only block in the last layer
	rcid, _, err := carutil.ReadNode(layers[len(layers)-1].br)
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("reading root block: %w", err)
	}

	sw := &sizerWriter{w: w}
	w = sw

	if err := car.WriteHeader(&car.CarHeader{
		Roots:   []cid.Cid{rcid},
		Version: 1,
	}, w); err != nil {
		return 0, cid.Undef, xerrors.Errorf("write car header: %w", err)
	}
	_, err = layers[len(layers)-1].f.Seek(0, io.SeekStart)
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("seeking to start of last layer: %w", err)
	}
	layers[len(layers)-1].br.Reset(layers[len(layers)-1].f)

	// write depth first, starting from top layer
	atLayer := layerCount

	layerWrote := make([]int, layerCount+1)

	err = m.jb.Iterate(func(c mh.Multihash, data []byte) error {
		// get down to layer 0 (jbob)
		for atLayer > 0 {
			// read next block from current layer
			c, data, err := carutil.ReadNode(layers[atLayer-1].br)
			if err != nil {
				return xerrors.Errorf("reading node from layer %d (wrote %d at that layer): %w", atLayer, layerWrote[atLayer], err)
			}

			// write block
			if err := carutil.LdWrite(w, c.Bytes(), data); err != nil {
				return xerrors.Errorf("writing node from layer %d: %w", atLayer, err)
			}

			layerWrote[atLayer]++
			atLayer--
		}

		// write block
		if err := carutil.LdWrite(w, mhToRawCid(c).Bytes(), data); err != nil {
			return xerrors.Errorf("writing jbob block: %w", err)
		}

		layerWrote[atLayer]++

		// propagate layers up if at arity
		for layerWrote[atLayer] == arity {
			layerWrote[atLayer] = 0
			atLayer++
		}

		return nil
	})
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("iterate jbob: %w", err)
	}

	return sw.s, rcid, nil
}

type cardata struct {
	f *os.File

	br *bufio.Reader
}

func (c *cardata) writeBlock(ci cid.Cid, data []byte) error {
	return carutil.LdWrite(c.f, ci.Bytes(), data)
}

func mhToRawCid(mh mh.Multihash) cid.Cid {
	return cid.NewCidV1(cid.Raw, mh)
}

var _ iface.Group = &Group{}
