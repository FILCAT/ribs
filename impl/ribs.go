package impl

import (
	"context"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/ributil"
	_ "github.com/mattn/go-sqlite3"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var log = logging.Logger("ribs")

type openOptions struct {
	workerGate chan struct{} // for testing
	hostGetter func(...libp2p.Option) (host.Host, error)
}

type OpenOption func(*openOptions)

func WithWorkerGate(gate chan struct{}) OpenOption {
	return func(o *openOptions) {
		o.workerGate = gate
	}
}

func WithHostGetter(hg func(...libp2p.Option) (host.Host, error)) OpenOption {
	return func(o *openOptions) {
		o.hostGetter = hg
	}
}

func Open(root string, opts ...OpenOption) (iface.RIBS, error) {
	if err := os.Mkdir(root, 0755); err != nil && !os.IsExist(err) {
		return nil, xerrors.Errorf("make root dir: %w", err)
	}

	db, err := openRibsDB(root)
	if err != nil {
		return nil, xerrors.Errorf("open db: %w", err)
	}

	opt := &openOptions{
		workerGate: make(chan struct{}),
		hostGetter: libp2p.New,
	}
	close(opt.workerGate)

	for _, o := range opts {
		o(opt)
	}

	walletPath := "~/.ribswallet"

	wallet, err := ributil.OpenWallet(walletPath)
	if err != nil {
		return nil, xerrors.Errorf("open wallet: %w", err)
	}

	defWallet, err := wallet.GetDefault()
	if err != nil {
		wl, err := wallet.WalletList(context.TODO())
		if err != nil {
			return nil, xerrors.Errorf("get wallet list: %w", err)
		}

		if len(wl) != 1 || len(wl) == 0 {
			return nil, xerrors.Errorf("no default wallet or more than one wallet: %#v", wl)
		}

		if err := wallet.SetDefault(wl[0]); err != nil {
			return nil, xerrors.Errorf("setting default wallet: %w", err)
		}

		defWallet, err = wallet.GetDefault()
		if err != nil {
			return nil, xerrors.Errorf("getting default wallet: %w", err)
		}
	}

	fmt.Println("RIBS Wallet: ", defWallet)

	h, err := opt.hostGetter()
	if err != nil {
		return nil, xerrors.Errorf("creating host: %w", err)
	}

	r := &ribs{
		root:  root,
		db:    db,
		index: NewIndex(db.db),

		host:   h,
		wallet: wallet,

		writableGroups: make(map[iface.GroupKey]*Group),

		// all open groups (including all writable)
		openGroups: make(map[iface.GroupKey]*Group),

		uploadStats:     map[iface.GroupKey]*iface.UploadStats{},
		uploadStatsSnap: map[iface.GroupKey]*iface.UploadStats{},

		tasks: make(chan task, 16),

		workerClose:  make(chan struct{}),
		workerClosed: make(chan struct{}),

		spCrawlClose:  make(chan struct{}),
		spCrawlClosed: make(chan struct{}),
	}

	// todo resume tasks

	go r.groupWorker(opt.workerGate)
	go r.spCrawler()

	if err := r.setupCarServer(context.TODO(), h); err != nil {
		return nil, xerrors.Errorf("setup car server: %w", err)
	}

	return r, nil
}

func (r *ribs) groupWorker(gate <-chan struct{}) {
	for {
		<-gate
		select {
		case task := <-r.tasks:
			r.workerExecTask(task)
		case <-r.workerClose:
			close(r.workerClosed)
			return
		}
	}
}

func (r *ribs) workerExecTask(task task) {
	switch task.tt {
	case taskTypeFinalize:

		r.lk.Lock()
		g, ok := r.openGroups[task.group]
		if !ok {
			r.lk.Unlock()
			log.Errorw("group not open", "group", task.group, "task", task)
			return
		}

		err := g.Finalize(context.TODO())
		r.lk.Unlock()
		if err != nil {
			log.Errorf("finalizing group: %s", err)
		}
		fallthrough
	case taskTypeMakeVCAR:
		r.lk.Lock()
		g, ok := r.openGroups[task.group]
		r.lk.Unlock()
		if !ok {
			log.Errorw("group not open", "group", task.group, "task", task)
			return
		}

		err := g.GenTopCar(context.TODO())
		if err != nil {
			log.Errorf("generating top car: %s", err)
		}
		fallthrough
	case taskTypeGenCommP:
		r.lk.Lock()
		g, ok := r.openGroups[task.group]
		r.lk.Unlock()
		if !ok {
			log.Errorw("group not open", "group", task.group, "task", task)
			return
		}

		err := g.GenCommP()
		if err != nil {
			log.Errorf("generating commP: %s", err)
		}
		fallthrough
	case taskTypeMakeMoreDeals:
		r.lk.Lock()
		g, ok := r.openGroups[task.group]
		r.lk.Unlock()
		if !ok {
			log.Errorw("group not open", "group", task.group, "task", task)
			return
		}

		reqToken, err := r.makeCarRequestToken(context.TODO(), task.group, time.Hour*36)
		if err != nil {
			log.Errorf("making car request token: %s", err)
			return
		}

		err = g.MakeMoreDeals(context.TODO(), r.host, r.wallet, reqToken)
		if err != nil {
			log.Errorf("starting new deals: %s", err)
		}
	}
}

type taskType int

const (
	taskTypeFinalize taskType = iota
	taskTypeMakeVCAR
	taskTypeGenCommP
	taskTypeMakeMoreDeals
)

type task struct {
	tt    taskType
	group iface.GroupKey
}

type ribs struct {
	root string

	// todo hide this db behind an interface
	db    *ribsDB
	index iface.Index

	lk sync.Mutex

	host   host.Host
	wallet *ributil.LocalWallet

	/* storage */

	workerClose  chan struct{}
	workerClosed chan struct{}

	spCrawlClose  chan struct{}
	spCrawlClosed chan struct{}

	tasks chan task

	openGroups     map[int64]*Group
	writableGroups map[int64]*Group

	/* sp tracker */
	crawlState atomic.Pointer[string]

	/* car uploads */
	uploadStats     map[iface.GroupKey]*iface.UploadStats
	uploadStatsSnap map[iface.GroupKey]*iface.UploadStats
	uploadStatsLk   sync.Mutex
}

func (r *ribs) Close() error {
	close(r.workerClose)
	close(r.spCrawlClose)
	<-r.workerClosed
	<-r.spCrawlClosed

	// todo close all open groups

	return nil
}

func (r *ribs) withWritableGroup(prefer iface.GroupKey, cb func(group *Group) error) (selectedGroup iface.GroupKey, err error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	defer func() {
		if err != nil || selectedGroup == iface.UndefGroupKey {
			return
		}
		// if the group was filled, drop it from writableGroups and start finalize
		if r.writableGroups[selectedGroup].state != iface.GroupStateWritable {
			delete(r.writableGroups, selectedGroup)

			r.tasks <- task{
				tt:    taskTypeFinalize,
				group: selectedGroup,
			}
		}
	}()

	// todo prefer
	for g, grp := range r.writableGroups {
		return g, cb(grp)
	}

	// no writable groups, try to open one

	selectedGroup = iface.UndefGroupKey
	{
		var blocks int64
		var bytes int64
		var state iface.GroupState

		selectedGroup, blocks, bytes, state, err = r.db.GetWritableGroup()
		if err != nil {
			return iface.UndefGroupKey, xerrors.Errorf("finding writable groups: %w", err)
		}

		if selectedGroup != iface.UndefGroupKey {
			g, err := OpenGroup(r.db, r.index, selectedGroup, blocks, bytes, r.root, state, false)
			if err != nil {
				return iface.UndefGroupKey, xerrors.Errorf("opening group: %w", err)
			}

			r.writableGroups[selectedGroup] = g
			r.openGroups[selectedGroup] = g
			return selectedGroup, cb(g)
		}
	}

	// no writable groups, create one

	selectedGroup, err = r.db.CreateGroup()
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("creating group: %w", err)
	}

	g, err := OpenGroup(r.db, r.index, selectedGroup, 0, 0, r.root, iface.GroupStateWritable, true)
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("opening group: %w", err)
	}

	r.writableGroups[selectedGroup] = g
	r.openGroups[selectedGroup] = g
	return selectedGroup, cb(g)
}

func (r *ribs) withReadableGroup(group iface.GroupKey, cb func(group *Group) error) (err error) {
	r.lk.Lock()

	// todo prefer
	if r.openGroups[group] != nil {
		r.lk.Unlock()
		return cb(r.openGroups[group])
	}

	// not open, open it

	blocks, bytes, state, err := r.db.OpenGroup(group)
	if err != nil {
		r.lk.Unlock()
		return xerrors.Errorf("getting group metadata: %w", err)
	}

	g, err := OpenGroup(r.db, r.index, group, blocks, bytes, r.root, state, false)
	if err != nil {
		r.lk.Unlock()
		return xerrors.Errorf("opening group: %w", err)
	}

	if state == iface.GroupStateWritable {
		r.writableGroups[group] = g
	}
	r.openGroups[group] = g

	r.lk.Unlock()
	return cb(g)
}

type ribSession struct {
	r *ribs
}

type ribBatch struct {
	r *ribs

	currentWriteTarget iface.GroupKey

	// todo: use lru
	currentReadTarget iface.GroupKey
}

func (r *ribs) Session(ctx context.Context) iface.Session {
	return &ribSession{
		r: r,
	}
}

func (r *ribSession) View(ctx context.Context, c []mh.Multihash, cb func(cidx int, data []byte)) error {
	done := map[int]struct{}{}
	byGroup := map[iface.GroupKey][]int{}

	err := r.r.index.GetGroups(ctx, c, func(i [][]iface.GroupKey) (bool, error) {
		for cidx, groups := range i {
			if _, ok := done[cidx]; ok {
				continue
			}
			done[cidx] = struct{}{}

			for _, g := range groups {
				if g == iface.UndefGroupKey {
					continue
				}

				byGroup[g] = append(byGroup[g], cidx)
			}
		}

		return len(done) != len(c), nil
	})
	if err != nil {
		return err
	}

	for g, cidxs := range byGroup {
		toGet := make([]mh.Multihash, len(cidxs))
		for i, cidx := range cidxs {
			toGet[i] = c[cidx]
		}

		err := r.r.withReadableGroup(g, func(g *Group) error {
			return g.View(ctx, toGet, func(cidx int, data []byte) {
				cb(cidxs[cidx], data)
			})
		})
		if err != nil {
			return xerrors.Errorf("with readable group: %w", err)
		}
	}

	return nil
}

func (r *ribSession) Batch(ctx context.Context) iface.Batch {
	return &ribBatch{
		r:                  r.r,
		currentWriteTarget: iface.UndefGroupKey,
	}
}

func (r *ribBatch) Put(ctx context.Context, b []blocks.Block) error {
	// todo filter blocks that already exist
	var done int
	for done < len(b) {
		gk, err := r.r.withWritableGroup(r.currentWriteTarget, func(g *Group) error {
			wrote, err := g.Put(ctx, b[done:])
			if err != nil {
				return err
			}
			done += wrote
			return nil
		})
		if err != nil {
			return xerrors.Errorf("write to group: %w", err)
		}

		r.currentWriteTarget = gk
	}

	return nil
}

func (r *ribSession) Has(ctx context.Context, c []mh.Multihash) ([]bool, error) {
	//TODO implement me
	panic("implement me")
}

func (r *ribSession) GetSize(ctx context.Context, c []mh.Multihash) ([]int64, error) {
	//TODO implement me
	panic("implement me")
}

func (r *ribBatch) Unlink(ctx context.Context, c []mh.Multihash) error {
	//TODO implement me
	panic("implement me")
}

func (r *ribBatch) Flush(ctx context.Context) error {
	// noop for now, group puts are sync currently
	return nil
}

var _ iface.RIBS = &ribs{}
