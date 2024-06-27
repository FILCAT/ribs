package rbstor

import (
	"context"
	"time"

	iface "github.com/atboosty/ribs"
	"golang.org/x/xerrors"
)

const MaxLocalGroupCount = 64 // todo user config

func (r *rbs) createGroup(ctx context.Context) (iface.GroupKey, *Group, error) {
	if err := r.ensureSpaceForGroup(ctx); err != nil {
		return 0, nil, xerrors.Errorf("ensure space for group: %w", err)
	}

	selectedGroup, err := r.db.CreateGroup()
	if err != nil {
		return iface.UndefGroupKey, nil, xerrors.Errorf("creating group: %w", err)
	}

	g, err := r.openGroup(ctx, selectedGroup, 0, 0, 0, iface.GroupStateWritable, true)
	if err != nil {
		return iface.UndefGroupKey, nil, xerrors.Errorf("opening group: %w", err)
	}

	return selectedGroup, g, nil
}

func (r *rbs) openGroup(ctx context.Context, group iface.GroupKey, blocks, bytes, jbhead int64, state iface.GroupState, create bool) (*Group, error) {
	g, err := OpenGroup(ctx, r.db, r.index, &r.staging, group, blocks, bytes, jbhead, r.root, state, create)
	if err != nil {
		return nil, xerrors.Errorf("opening group: %w", err)
	}

	if state == iface.GroupStateWritable {
		r.writableGroups[group] = g
	}
	r.openGroups[group] = g

	return g, nil
}

func (r *rbs) withWritableGroup(ctx context.Context, prefer iface.GroupKey, cb func(group *Group) error) (selectedGroup iface.GroupKey, err error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	r.writeLk.Lock()
	defer r.writeLk.Unlock()

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
		var blocks, bytes, jbhead int64
		var state iface.GroupState

		selectedGroup, blocks, bytes, jbhead, state, err = r.db.GetWritableGroup()
		if err != nil {
			return iface.UndefGroupKey, xerrors.Errorf("finding writable groups: %w", err)
		}

		if selectedGroup != iface.UndefGroupKey {
			g, err := r.openGroup(ctx, selectedGroup, blocks, bytes, jbhead, state, false)
			if err != nil {
				return iface.UndefGroupKey, xerrors.Errorf("opening group: %w", err)
			}

			return selectedGroup, cb(g)
		}
	}

	// no writable groups, create one

	selectedGroup, g, err := r.createGroup(ctx)
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("creating group: %w", err)
	}

	return selectedGroup, cb(g)
}

func (r *rbs) withReadableGroup(ctx context.Context, group iface.GroupKey, cb func(group *Group) error) (err error) {
	r.lk.Lock()

	// todo prefer
	if r.openGroups[group] != nil {
		r.lk.Unlock()
		return cb(r.openGroups[group])
	}

	// not open, open it

	blocks, bytes, jbhead, state, err := r.db.OpenGroup(group)
	if err != nil {
		r.lk.Unlock()
		return xerrors.Errorf("getting group metadata: %w", err)
	}

	g, err := r.openGroup(ctx, group, blocks, bytes, jbhead, state, false)
	if err != nil {
		r.lk.Unlock()
		return xerrors.Errorf("opening group: %w", err)
	}

	r.resumeGroup(group)

	r.lk.Unlock()
	return cb(g)
}

func (r *rbs) ensureSpaceForGroup(ctx context.Context) error {
	localCount, err := r.db.CountNonOffloadedGroups()
	if err != nil {
		return xerrors.Errorf("counting non-offloaded groups: %w", err)
	}

	if localCount < MaxLocalGroupCount {
		return nil
	}

	var offloadCandidate iface.GroupKey
	for {
		offloadCandidate, err = r.db.GetOffloadCandidate()
		if err != nil {
			return xerrors.Errorf("getting offload candidate: %w", err)
		}

		if offloadCandidate != iface.UndefGroupKey {
			break
		}

		log.Errorw("no offload candidate, waiting for space", "localCount", localCount)

		// wait 1 min, then try again
		r.lk.Unlock()

		select {
		case <-ctx.Done():
			r.lk.Lock()
			return ctx.Err()
		case <-time.After(time.Minute):
		}

		r.lk.Lock()
	}

	log.Errorw("local space full, offloading group", "group", offloadCandidate)

	// release read side
	r.lk.Unlock()
	defer r.lk.Lock()

	return r.withReadableGroup(ctx, offloadCandidate, func(g *Group) error {
		err := g.offloadStaging()
		return err
	})
}
