package rbdeal

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/xerrors"
)

func (r *ribs) repairWatcher(ctx context.Context) {
	for {
		if err := r.repairLoop(ctx); err != nil {
			log.Errorw("repair loop error", "error", err)
		}

		select {
		case <-time.After(10 * time.Minute):
		case <-ctx.Done():
			return
		}
	}
}

func (r *ribs) repairLoop(ctx context.Context) error {
	toRepair, err := r.db.GroupsToRepair()
	if err != nil {
		return xerrors.Errorf("finding groups to repair: %w", err)
	}

	fmt.Println("repair count ", len(toRepair))

	for _, g := range toRepair {
		fmt.Println("torepair ", g.GroupID, " ", g.Retrievable)
	}

	return nil
}
