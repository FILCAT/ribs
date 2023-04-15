package rbstor

import (
	"context"

	iface "github.com/lotus-web3/ribs"
)

func (r *rbs) groupWorker(gate <-chan struct{}) {
	for {
		<-gate
		select {
		case task := <-r.tasks:
			r.workerExecTask(task)
		case <-r.close:
			close(r.workerClosed)
			return
		}
	}
}

func (r *rbs) workerExecTask(toExec task) {
	switch toExec.tt {
	case taskTypeFinalize:

		r.lk.Lock()
		g, ok := r.openGroups[toExec.group]
		if !ok {
			r.lk.Unlock()
			log.Errorw("group not open", "group", toExec.group, "toExec", toExec)
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
		g, ok := r.openGroups[toExec.group]
		r.lk.Unlock()
		if !ok {
			log.Errorw("group not open", "group", toExec.group, "toExec", toExec)
			return
		}

		err := g.GenTopCar(context.TODO())
		if err != nil {
			log.Errorf("generating top car: %s", err)
		}
		fallthrough
	case taskTypeGenCommP:
		r.lk.Lock()
		g, ok := r.openGroups[toExec.group]
		r.lk.Unlock()
		if !ok {
			log.Errorw("group not open", "group", toExec.group, "toExec", toExec)
			return
		}

		err := g.GenCommP()
		if err != nil {
			log.Errorf("generating commP: %s", err)
		}
	}
}

func (r *rbs) Subscribe(sub iface.GroupSub) {
	//TODO implement me
	panic("implement me")
}

func (r *rbs) resumeGroups(ctx context.Context) {
	gs, err := r.db.GroupStates()
	if err != nil {
		panic(err)
	}

	for g, st := range gs {
		switch st {
		case iface.GroupStateFull, iface.GroupStateBSSTExists, iface.GroupStateLevelIndexDropped, iface.GroupStateVRCARDone, iface.GroupStateLocalReadyForDeals:
			if err := r.withReadableGroup(ctx, g, func(g *Group) error {
				return nil
			}); err != nil {
				log.Errorw("failed to resume group", "group", g, "err", err)
				return
			}
		}
	}
}
