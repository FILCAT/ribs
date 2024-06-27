package rbdeal

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"

	ribs2 "github.com/atboosty/ribs"
	"github.com/atboosty/ribs/ributil"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
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

func (r *ribs) repairWorker(ctx context.Context, workerID int) { // root, id?
	for {
		select {
		case <-r.close:
			return
		default:
		}

		err := r.repairStep(ctx, workerID)
		if err != nil {
			log.Errorw("repair step failed", "error", err, "worker", workerID)

			if err := r.db.UpdateRepairOnStepNotDone(workerID); err != nil {
				log.Errorw("unassigning worker from failed repair", "worker", workerID)
			}
		}
	}
}

func (r *ribs) repairStep(ctx context.Context, workerID int) error {
	assignedGroups, err := r.db.GetAssignedRepairWorkByWorkerID(workerID)
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
	groupFile, err := r.fetchGroup(ctx, workerID, *assigned)
	if err != nil {
		return xerrors.Errorf("fetch sector (group %d): %w", *assigned, err)
	}

	groupReader, err := os.OpenFile(groupFile, os.O_RDONLY, 0644)
	if err != nil {
		return xerrors.Errorf("opening repair .car file: %w", err)
	}
	defer groupReader.Close()

	st, err := groupReader.Stat()
	if err != nil {
		return xerrors.Errorf("stat repair file: %w", err)
	}

	// here we have the sector fetched and verified

	err = r.RBS.Storage().LoadFilCar(ctx, *assigned, groupReader, int64(st.Size()))
	if err != nil {
		return xerrors.Errorf("reload data file (group %d): %w", *assigned, err)
	}

	if err := r.db.DelRepair(*assigned); err != nil {
		return xerrors.Errorf("marking group %d as repaired: %w", *assigned, err)
	}

	// remove repair file
	if err := os.Remove(groupFile); err != nil {
		return xerrors.Errorf("removing repair file: %w", err)
	}

	return nil
}

func (r *ribs) fetchGroup(ctx context.Context, workerID int, group ribs2.GroupKey) (string, error) {
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
		return "", xerrors.Errorf("mkdir repair worker dir: %w", err)
	}

	groupFile := filepath.Join(workerDir, fmt.Sprintf("group-%d.car", group))

	if err := r.fetchGroupHttp(ctx, workerID, group, groupFile); err != nil {
		log.Errorw("failed to fetch group with http, will attempt lassie", "err", err, "group", group, "worker", workerID)

		if err := r.fetchGroupLassie(ctx, workerID, group, groupFile); err != nil {
			log.Errorw("failed to fetch group with lassie", "err", err, "group", group, "worker", workerID)
			return "", xerrors.Errorf("fetch group lassie: %w", err)
		}
	}

	return groupFile, nil
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

	type retrievalSource struct {
		provider string
		reqUrl   url.URL
	}

	var sources []retrievalSource

	{
		// TODO: HACK: make this use the db
		// local data import
		envName := fmt.Sprintf("RIBS_IMPORT_%d", group)
		if importUrl, ok := os.LookupEnv(envName); ok {
			u, err := url.Parse(importUrl)
			if err != nil {
				return xerrors.Errorf("failed to parse import url: %w", err)
			}

			sources = append(sources, retrievalSource{
				provider: "local",
				reqUrl:   *u,
			})
		}
	}

	for _, candidate := range candidates {
		// booster-http providers
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

		sources = append(sources, retrievalSource{
			provider: fmt.Sprint(candidate.Provider),
			reqUrl:   reqUrl,
		})
	}

	for _, candidate := range sources {
		r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
			r.State = ribs2.RepairJobStateFetching
		})

		reqUrl := candidate.reqUrl

		log.Errorw("attempting http repair retrieval", "url", reqUrl.String(), "group", group, "provider", candidate.provider)

		// make the request!!

		robustReqReader := ributil.RobustGet(reqUrl.String(), gm.CarSize, func() *ributil.RateCounter {
			return r.repairFetchCounters.Get(group)
		})

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
			// watch fetch progress with file stat

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

		repairReader, err := ributil.NewCarRepairReader(robustReqReader, gm.RootCid, func(b cid.Cid, badData []byte) ([]byte, error) {
			var outData []byte
			err := r.retrProv.FetchBlocks(ctx, group, []multihash.Multihash{b.Hash()}, func(cidx int, data []byte) {
				outData = make([]byte, len(data))
				copy(outData, data)
			})
			if err == nil {
				return outData, nil
			}

			log.Errorw("failed to fetch repair block", "err", err, "group", group, "provider", candidate.provider, "url", reqUrl.String())
			/*
				// try bitflip repair
				NOTE: this bit flip repair is not really useful, apparently bitflips tend to come in groups,
					and we're not fixing more that one bitfilp

				if len(badData) == 0 {
					return nil, xerrors.Errorf("can't attempt bitflip repair and repair retrieval failed: %w", err)
				}

				log.Errorw("attempting bitflip repair", "group", group, "provider", candidate.Provider, "url", reqUrl.String(), "dataSize", len(badData))
				for i := 0; i < len(badData)*8; i++ {
					if i > 0 {
						// unflip previous bit
						prevBit := i - 1
						badData[prevBit/8] ^= 1 << (prevBit % 8)
					}

					// flip bit
					badData[i/8] ^= 1 << (i % 8)

					hash, err := b.Prefix().Sum(badData)
					if err != nil {
						return nil, xerrors.Errorf("hash data: %w", err)
					}

					if hash.Equals(b) {
						log.Errorw("bitflip repair successful", "group", group, "provider", candidate.Provider, "url", reqUrl.String(), "flippedBit", i)
						return badData, nil
					}
				}

				// unflip last bit
				badData[len(badData)-1] ^= 1 << 7
				log.Errorw("bitflip repair failed", "group", group, "provider", candidate.Provider, "url", reqUrl.String())
			*/
			return nil, xerrors.Errorf("repair retrieval failed: %w", err)
		})
		if err != nil {
			_ = f.Close()
			_ = os.Remove(groupFile)
			_ = robustReqReader.Close()
			done()
			log.Errorw("failed to create repair reader", "err", err, "group", group, "provider", candidate.provider, "url", reqUrl.String())
			continue
		}

		cc := new(ributil.DataCidWriter)
		commdReader := io.TeeReader(repairReader, cc)

		_, err = io.Copy(f, commdReader)
		done()
		if err != nil {
			_ = f.Close()
			_ = os.Remove(groupFile)
			_ = robustReqReader.Close()
			log.Errorw("failed to copy response body", "err", err, "group", group, "provider", candidate.provider, "url", reqUrl.String())
			continue
		}

		if err := f.Close(); err != nil {
			_ = robustReqReader.Close()
			return xerrors.Errorf("close group file: %w", err)
		}

		if err := robustReqReader.Close(); err != nil {
			return xerrors.Errorf("close response body: %w", err)
		}

		r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
			r.FetchProgress = r.FetchSize
			r.State = ribs2.RepairJobStateVerifying
		})

		dc, err := cc.Sum()
		if err != nil {
			return xerrors.Errorf("sum car: %w", err)
		}

		if dc.PieceCID != gm.PieceCid {
			//return xerrors.Errorf("piece cid mismatch: %s != %s", dc.PieceCID, gm.PieceCid)
			// todo record
			log.Errorw("piece cid mismatch", "cid", dc.PieceCID, "expected", gm.PieceCid, "provider", candidate.provider, "group", group, "file", groupFile)

			// remove the file
			_ = os.Remove(groupFile)

			continue
		}

		r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
			r.FetchProgress = r.FetchSize
			r.State = ribs2.RepairJobStateImporting
		})

		return nil
	}

	return xerrors.Errorf("no retrieval candidates")
}

func (r *ribs) fetchGroupLassie(ctx context.Context, workerID int, group ribs2.GroupKey, groupFile string) error {
	gm, err := r.Storage().DescibeGroup(ctx, group)
	if err != nil {
		return xerrors.Errorf("failed to get group metadata: %w", err)
	}

	log.Errorw("attempting lassie repair retrieval", "group", group, "root", gm.RootCid, "piece", gm.PieceCid, "file", groupFile, "worker", workerID)

	tempDir := fmt.Sprintf("%s.temp", groupFile)
	err = os.MkdirAll(tempDir, 0755)
	if err != nil {
		return xerrors.Errorf("mkdir temp dir: %w", err)
	}

	defer func() {
		err := os.RemoveAll(tempDir)
		if err != nil {
			log.Errorw("failed to remove lassie temp dir", "err", err, "dir", tempDir, "group", group, "worker", workerID)
		}
	}()

	r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
		r.FetchProgress = 0
		r.State = ribs2.RepairJobStateFetching
		r.FetchUrl = "lassie+[bitswap,graphsync]"
	})

	ctx, done := context.WithCancel(ctx)
	go func() {
		// watch fetch progress with file stat

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}

			fi, err := os.Stat(groupFile)
			if err != nil {
				log.Errorw("failed to stat group file", "err", err)
				continue
			}

			r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
				r.FetchProgress = fi.Size()
			})
		}
	}()

	err = r.retrProv.FetchDeal(ctx, group, gm.RootCid, tempDir, groupFile)
	done()

	if err != nil {
		_ = os.Remove(groupFile)
		log.Errorw("failed to fetch deal with lassie", "err", err, "group", group, "worker", workerID)
		return xerrors.Errorf("failed to fetch deal: %w", err)
	}

	r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
		r.FetchProgress = r.FetchSize
		r.State = ribs2.RepairJobStateVerifying
	})

	f, err := os.OpenFile(groupFile, os.O_RDONLY, 0644)
	if err != nil {
		return xerrors.Errorf("open group file: %w", err)
	}

	cc := new(ributil.DataCidWriter)

	_, err = io.Copy(cc, f)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(groupFile)
		return xerrors.Errorf("copy group file: %w", err)
	}

	if err := f.Close(); err != nil {
		return xerrors.Errorf("close group file: %w", err)
	}

	dc, err := cc.Sum()
	if err != nil {
		_ = os.Remove(groupFile)
		return xerrors.Errorf("sum car: %w", err)
	}

	if dc.PieceCID != gm.PieceCid {
		_ = os.Remove(groupFile)
		log.Errorw("piece cid mismatch in lassie fetch", "cid", dc.PieceCID, "expected", gm.PieceCid, "group", group, "file", groupFile)
		return xerrors.Errorf("piece cid mismatch: %s != %s", dc.PieceCID, gm.PieceCid)
	}

	r.updateRepairStats(workerID, func(r *ribs2.RepairJob) {
		r.FetchProgress = r.FetchSize
		r.State = ribs2.RepairJobStateImporting
	})

	return nil
}
