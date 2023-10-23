package rbdeal

import (
	"context"
	"fmt"
	ribs2 "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/ributil"
	"golang.org/x/xerrors"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"
)

/*

REPAIR WORKERS:
* check if they have active work
* if not, manage repair queue
* go to top

If they have active work:
* Fetch sector
	* Http if possible
	* lassie if not..
* Verify and send to storage, move group to deals in progress

Tables:
* repair_queue: group, retrievable_deals, assigned_worker, last_attempt

*/

var RepairCheckInterval = time.Minute

func (r *ribs) repairWorker(ctx context.Context) { // root, id?
	workerID := 0

	for {
		select {
		case <-r.close:
			return
		default:
		}

		err := r.repairStep(ctx, workerID)
		if err != nil {
			log.Errorw("repair step failed", "error", err, "worker", workerID)
		}
	}
}

func (r *ribs) repairStep(ctx context.Context, workerID int) error {
	assignedGroups, err := r.db.GetAssignedWorkByWorkerID(workerID)
	if err != nil {
		return xerrors.Errorf("get assigned work: %w", err)
	}

	if len(assignedGroups) > 1 {
		log.Warnw("repair worker has more than one assigned group", "worker", workerID, "groups", len(assignedGroups))
	}

	var assigned *ribs2.GroupKey

	if len(assignedGroups) == 0 {
		if err := r.db.AddRepairsForLowRetrievableDeals(); err != nil {
			return xerrors.Errorf("AddRepairsForLowRetrievableDeals: %w", err)
		}

		assigned, err = r.db.AssignRepairToWorker(workerID)
		if err != nil {
			return xerrors.Errorf("assign repair to worker: %w", err)
		}
	} else {
		assigned = &assignedGroups[0]
	}

	if assigned == nil {
		select {
		case <-r.close:
		case <-time.After(RepairCheckInterval):
		}

		return nil
	}

	// fetch sector if not fetched
	err = r.fetchGroup(ctx, workerID, *assigned)
	if err != nil {
		return xerrors.Errorf("fetch sector: %w", err)
	}

	// here we have the sector fetched

	select {}

	// send to storage

	// move group to deals in progress

	return nil
}

func (r *ribs) fetchGroup(ctx context.Context, workerID int, group ribs2.GroupKey) error {
	rstat := ribs2.RepairJob{
		GroupKey:      group,
		State:         ribs2.RepairJobStateFetching,
		FetchProgress: 0,
		FetchSize:     0,
	}

	r.repairStatsLk.Lock()
	r.repairStats[workerID] = &rstat
	r.repairStatsLk.Unlock()

	workerDir := filepath.Join(r.repairDir, fmt.Sprintf("w%d", workerID))
	// todo check if anything else is in the worker dir, cleanup if needed

	if err := os.MkdirAll(workerDir, 0755); err != nil {
		return xerrors.Errorf("mkdir repair worker dir: %w", err)
	}

	groupFile := filepath.Join(workerDir, fmt.Sprintf("group-%d.car", group))

	if err := r.fetchGroupHttp(ctx, workerID, group, groupFile); err != nil {
		return xerrors.Errorf("fetch group http: %w", err)
	}

	// todo: lassie

	return nil
}

func (r *ribs) updateRepairStats(worker int, cb func(*ribs2.RepairJob)) {
	r.repairStatsLk.Lock()
	defer r.repairStatsLk.Unlock()

	cb(r.repairStats[worker])
}

func (r *ribs) fetchGroupHttp(ctx context.Context, workerID int, group ribs2.GroupKey, groupFile string) error {
	cc, err := r.retrProv.retrievalCandidatesForGroupCached(group)
	if err != nil {
		return xerrors.Errorf("failed to get retrieval candidates: %w", err)
	}
	candidates := cc.candidates

	gm, err := r.Storage().DescibeGroup(ctx, group)
	if err != nil {
		return xerrors.Errorf("failed to get group metadata: %w", err)
	}

	r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
		r.FetchSize = gm.CarSize
	})

	for _, candidate := range candidates {
		addrInfo, err := r.retrProv.getAddrInfoCached(candidate.Provider)
		if err != nil {
			log.Errorw("failed to get addrinfo", "provider", candidate.Provider, "err", err)
			continue
		}

		if len(addrInfo.HttpMaddrs) == 0 {
			continue
		}

		u, err := ributil.MaddrsToUrl(addrInfo.HttpMaddrs)
		if err != nil {
			log.Errorw("failed to parse addrinfo", "provider", candidate.Provider, "err", err)
			continue
		}

		// start fetch into the file
		reqUrl := *u
		reqUrl.Path = path.Join(reqUrl.Path, "piece", gm.PieceCid.String())

		log.Errorw("attempting http repair retrieval", "url", reqUrl.String(), "group", group, "provider", candidate.Provider)

		req, err := http.NewRequestWithContext(ctx, "GET", reqUrl.String(), nil)
		if err != nil {
			return xerrors.Errorf("new request: %w", err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return xerrors.Errorf("do request: %w", err)
		}

		if resp.StatusCode != 200 {
			return xerrors.Errorf("http status: %d", resp.StatusCode)
		}

		r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
			r.FetchProgress = 0
			r.FetchUrl = reqUrl.String()
		})

		// copy response body to file
		f, err := os.OpenFile(groupFile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
		if err != nil {
			return xerrors.Errorf("open group file: %w", err)
		}

		ctx, done := context.WithCancel(ctx)
		go func() {
			// watch fetch progress with file stat (that allows io.Copy to be smart)

			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}

				fi, err := f.Stat()
				if err != nil {
					log.Errorw("failed to stat group file", "err", err)
					continue
				}

				r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
					r.FetchProgress = fi.Size()
				})
			}
		}()

		_, err = io.Copy(f, resp.Body)
		done()
		if err != nil {
			_ = f.Close()
			_ = os.Remove(groupFile)
			_ = resp.Body.Close()
			return xerrors.Errorf("copy response body: %w", err)
		}

		if err := f.Close(); err != nil {
			_ = resp.Body.Close()
			return xerrors.Errorf("close group file: %w", err)
		}

		if err := resp.Body.Close(); err != nil {
			return xerrors.Errorf("close response body: %w", err)
		}

		r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
			r.FetchProgress = r.FetchSize
			r.State = ribs2.RepairJobStateVerifying
		})

		// todo: verify group file

		return nil
	}

	return xerrors.Errorf("no retrieval candidates")
}

func (r *ribs) fetchGroupLassie() {

}
