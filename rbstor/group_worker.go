package rbstor

import (
	"context"

	iface "github.com/atboosty/ribs"
)

func (r *rbs) groupWorker(i int) {
	r.workersAvail.Add(1)
	defer r.workersAvail.Add(-1)

	for {
		select {
		case task := <-r.tasks:
			r.workerExecTask(task)
		case <-r.close:
			close(r.workerClosed[i])
			return
		}
	}
}

func (r *rbs) workerExecTask(toExec task) {
	switch toExec.tt {
	case taskTypeFinalize:
		r.workersFinalizing.Add(1)
		defer r.workersFinalizing.Add(-1)

		r.lk.Lock()
		g, ok := r.openGroups[toExec.group]
		if !ok {
			r.lk.Unlock()
			log.Errorw("group not open", "group", toExec.group, "toExec", toExec)
			return
		}

		r.lk.Unlock()
		err := g.Finalize(context.TODO())
		if err != nil {
			log.Errorw("finalizing group", "error", err, "group", toExec.group)
		}

		r.sendSub(toExec.group, iface.GroupStateFull, iface.GroupStateVRCARDone)

		log.Errorw("finalize fallthrough to genCommP", "group", toExec.group)
		fallthrough

	case taskTypeGenCommP:
		r.workersCommP.Add(1)
		defer r.workersCommP.Add(-1)

		r.lk.Lock()
		g, ok := r.openGroups[toExec.group]
		r.lk.Unlock()
		if !ok {
			log.Errorw("group not open", "group", toExec.group, "toExec", toExec)
			return
		}

		err := g.GenCommP() // todo do in finalize...
		if err != nil {
			log.Errorw("generating commP", "group", toExec.group, "err", err)
		}

		r.sendSub(toExec.group, iface.GroupStateVRCARDone, iface.GroupStateLocalReadyForDeals)
	case taskTypeFinDataReload:
		r.workersFinDataReload.Add(1)
		defer r.workersFinDataReload.Add(-1)

		r.lk.Lock()
		g, ok := r.openGroups[toExec.group]
		r.lk.Unlock()
		if !ok {
			log.Errorw("group not open", "group", toExec.group, "toExec", toExec)
			return
		}

		err := g.FinDataReload(context.TODO())
		if err != nil {
			log.Errorw("finishing data reload", "group", toExec.group, "err", err)
		}

		r.sendSub(toExec.group, iface.GroupStateReload, iface.GroupStateLocalReadyForDeals)
	}
}

func (r *rbs) Subscribe(sub iface.GroupSub) {
	r.subLk.Lock()
	defer r.subLk.Unlock()

	r.subs = append(r.subs, sub)
}

func (r *rbs) resumeGroups(ctx context.Context) {
	gs, err := r.db.GroupStates()
	if err != nil {
		panic(err)
	}

	for g, st := range gs {
		switch st {
		case iface.GroupStateFull, iface.GroupStateVRCARDone, iface.GroupStateLocalReadyForDeals:
			if err := r.withReadableGroup(ctx, g, func(g *Group) error {
				return nil
			}); err != nil {
				log.Errorw("failed to resume group", "group", g, "err", err)
				return
			}
		}
	}
}

func (r *rbs) resumeGroup(group iface.GroupKey) {
	sendTask := func(tt taskType) {
		go func() {
			r.tasks <- task{
				tt:    tt,
				group: group,
			}
		}()
	}

	r.sendSub(group, r.openGroups[group].state, r.openGroups[group].state)

	switch r.openGroups[group].state {
	case iface.GroupStateWritable: // nothing to do
	case iface.GroupStateFull:
		sendTask(taskTypeFinalize)
	case iface.GroupStateVRCARDone:
		sendTask(taskTypeGenCommP)
	case iface.GroupStateLocalReadyForDeals:
	case iface.GroupStateOffloaded:
	case iface.GroupStateReload:
		sendTask(taskTypeFinDataReload)
	}
}

func (r *rbs) sendSub(group iface.GroupKey, old, new iface.GroupState) {
	r.subLk.Lock()
	defer r.subLk.Unlock()

	for _, sub := range r.subs {
		sub(group, old, new)
	}
}
