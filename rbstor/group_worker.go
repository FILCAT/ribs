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
		//fallthrough
		/*case taskTypeMakeMoreDeals:
			if !r.doDeals {
				return
			}

			r.lk.Lock()
			g, ok := r.openGroups[toExec.group]
			r.lk.Unlock()
			if !ok {
				log.Errorw("group not open", "group", toExec.group, "toExec", toExec)
				return
			}

			dealInfo, err := r.db.GetDealParams(context.TODO(), toExec.group)
			if err != nil {
				log.Errorf("getting deal params: %s", err)
				return
			}

			reqToken, err := r.makeCarRequestToken(context.TODO(), toExec.group, time.Hour*36, dealInfo.CarSize)
			if err != nil {
				log.Errorf("making car request token: %s", err)
				return
			}

			err = g.MakeMoreDeals(context.TODO(), r.host, r.wallet, reqToken)
			if err != nil {
				log.Errorf("starting new deals: %s", err)
			}
			fallthrough
		case taskMonitorDeals:
			if !r.doDeals {
				return
			}

			c, err := r.db.GetNonFailedDealCount(toExec.group)
			if err != nil {
				log.Errorf("getting non-failed deal count: %s", err)
				return
			}

			if c < minimumReplicaCount {
				go func() {
					r.tasks <- task{
						tt:    taskTypeMakeMoreDeals,
						group: toExec.group,
					}
				}()
			}
		*/
		// todo add a check-in task to some timed queue
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
		case iface.GroupStateFull, iface.GroupStateBSSTExists, iface.GroupStateLevelIndexDropped, iface.GroupStateVRCARDone, iface.GroupStateHasCommp:
			if err := r.withReadableGroup(ctx, g, func(g *Group) error {
				return nil
			}); err != nil {
				log.Errorw("failed to resume group", "group", g, "err", err)
				return
			}
		}
	}
}
