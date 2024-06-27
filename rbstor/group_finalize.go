package rbstor

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	iface "github.com/atboosty/ribs"
	"github.com/atboosty/ribs/carlog"
	"github.com/atboosty/ribs/ributil"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

var globalCommpBytes atomic.Int64

func (m *Group) Finalize(ctx context.Context) error {
	m.dataLk.Lock()
	defer m.dataLk.Unlock()

	if m.state != iface.GroupStateFull {
		return xerrors.Errorf("group not in state for finalization: %d", m.state)
	}

	if err := m.jb.MarkReadOnly(); err != nil && err != carlog.ErrReadOnly {
		return xerrors.Errorf("mark read-only: %w", err)
	}

	if err := m.jb.Finalize(ctx); err != nil {
		return xerrors.Errorf("finalize jbob: %w", err)
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

	commStatWr := &rateStatWriter{
		w:  cc,
		st: &globalCommpBytes,
	}
	defer commStatWr.done()

	carSize, root, err := m.writeCar(commStatWr)
	if err != nil {
		return xerrors.Errorf("write car: %w", err)
	}

	sum, err := cc.Sum()
	if err != nil {
		return xerrors.Errorf("sum car (size: %d): %w", carSize, err)
	}

	log.Infow("generated commP", "duration", time.Since(start), "commP", sum.PieceCID, "pps", sum.PieceSize, "mbps", float64(carSize)/time.Since(start).Seconds()/1024/1024)

	p, _ := commcid.CIDToDataCommitmentV1(sum.PieceCID)

	if err := m.setCommP(context.Background(), iface.GroupStateLocalReadyForDeals, p, int64(sum.PieceSize), root, carSize); err != nil {
		return xerrors.Errorf("set commP: %w", err)
	}

	return nil
}

func (m *Group) LoadFilCar(ctx context.Context, f io.Reader, sz int64) error {
	if m.state != iface.GroupStateOffloaded {
		return xerrors.Errorf("can't offload group in state %d", m.state)
	}

	if err := m.jb.LoadData(ctx, f, sz); err != nil {
		return xerrors.Errorf("load carlog data: %w", err)
	}

	if err := m.advanceState(context.Background(), iface.GroupStateReload); err != nil {
		return xerrors.Errorf("marking group as offloaded: %w", err)
	}

	return nil
}

func (m *Group) FinDataReload(ctx context.Context) error {
	log.Errorw("FIN DATA REEELOAD")

	if m.state != iface.GroupStateReload {
		return xerrors.Errorf("group not in state for finishing data reload: %d", m.state)
	}

	gm, err := m.db.GroupMeta(m.id)
	if err != nil {
		return xerrors.Errorf("getting group meta: %w", err)
	}

	if gm.DealCarSize == nil {
		return xerrors.Errorf("deal car size is nil!")
	}

	if err := m.jb.FinDataReload(context.Background(), gm.Blocks, *gm.DealCarSize); err != nil {
		return xerrors.Errorf("carlog finalize data reload: %w", err)
	}

	log.Infow("finished data reload", "group", m.id)

	if err := m.advanceState(ctx, iface.GroupStateLocalReadyForDeals); err != nil {
		return xerrors.Errorf("mark level index dropped: %w", err)
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

// offload completely removes local data
func (m *Group) offload() error {
	m.offloaded.Store(1)
	// m.jb.Offload will wait for any in-progress writes to finish

	m.dataLk.Lock()
	defer m.dataLk.Unlock()

	if m.state != iface.GroupStateLocalReadyForDeals {
		return xerrors.Errorf("can't offload group in state %d", m.state)
	}

	if err := m.advanceState(context.Background(), iface.GroupStateOffloaded); err != nil {
		return xerrors.Errorf("marking group as offloaded: %w", err)
	}

	// TODO Offloading state

	err := m.jb.Offload()
	if err != nil && err != carlog.ErrAlreadyOffloaded {
		return xerrors.Errorf("offloading carlog: %w", err)
	}

	if err := m.db.WriteOffloadEntry(m.id); err != nil {
		return xerrors.Errorf("write offload entry: %w", err)
	}

	return nil
}

// offloadStaging removes local data, reads will be redirected to staging
func (m *Group) offloadStaging() error {
	m.dataLk.Lock()
	defer m.dataLk.Unlock()

	if m.state != iface.GroupStateLocalReadyForDeals {
		return xerrors.Errorf("can't offload group in state %d", m.state)
	}

	err := m.jb.OffloadData()
	if err != nil {
		return xerrors.Errorf("offloading carlog data: %w", err)
	}

	if err := m.db.WriteOffloadEntry(m.id); err != nil {
		return xerrors.Errorf("write offload entry: %w", err)
	}

	return nil
}

type rateStatWriter struct {
	w io.Writer

	st *atomic.Int64
	t  int64
}

func (r *rateStatWriter) Write(p []byte) (n int, err error) {
	n, err = r.w.Write(p)

	r.t += int64(n)
	if r.t > 1<<23 {
		r.st.Add(r.t)
		r.t = 0
	}

	return
}

func (r *rateStatWriter) done() {
	r.st.Add(r.t)
	r.t = 0
}

var _ io.Writer = &rateStatWriter{}
