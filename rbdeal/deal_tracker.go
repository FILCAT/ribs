package rbdeal

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/lib/must"
	cbor "github.com/ipfs/go-ipld-cbor"

	ribs2 "github.com/atboosty/ribs"
	"github.com/atboosty/ribs/ributil"
	types "github.com/atboosty/ribs/ributil/boosttypes"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	types2 "github.com/filecoin-project/lotus/chain/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"golang.org/x/xerrors"
)

const DealStatusV12ProtocolID = "/fil/storage/status/1.2.0"

func (r *ribs) dealTracker(ctx context.Context) {
	for {
		checkStart := time.Now()
		select {
		case <-r.close:
			return
		default:
		}

		err := r.runDealCheckLoop(ctx)
		if err != nil {
			log.Errorw("deal check loop failed", "error", err)
		}

		checkDuration := time.Since(checkStart)

		log.Errorw("deal check loop finished", "duration", checkDuration)

		if checkDuration < DealCheckInterval {
			select {
			case <-r.close:
				return
			case <-time.After(DealCheckInterval - checkDuration):
			}
		}
	}
}

func (r *ribs) runDealCheckLoop(ctx context.Context) error {
	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		return xerrors.Errorf("creating gateway rpc client: %w", err)
	}
	defer closer()

	/* PUBLISHED DEAL CHECKS */
	/* Wait for published deals to become active (or expire) */

	{
		toCheck, err := r.db.PublishedDeals()
		if err != nil {
			return xerrors.Errorf("get inactive published deals: %w", err)
		}

		for _, deal := range toCheck {
			dealInfo, err := gw.StateMarketStorageDeal(ctx, deal.DealID, types2.EmptyTSK)
			if err != nil {
				log.Warnw("get deal info", "error", err, "dealid", deal.DealID, "deal", deal.DealUUID)

				startBy, err := r.db.GetDealStartEpoch(deal.DealUUID)
				if err != nil {
					return xerrors.Errorf("get deal start epoch: %w", err)
				}

				head, err := gw.ChainHead(ctx)
				if err != nil {
					return xerrors.Errorf("get chain head: %w", err)
				}

				if head.Height() >= startBy {
					if err := r.db.UpdateExpiredDeal(deal.DealUUID); err != nil {
						return xerrors.Errorf("marking deal as expired: %w", err)
					}
				}
				continue
			}

			if dealInfo.State.SectorStartEpoch > 0 {
				if err := r.db.UpdateActivatedDeal(deal.DealUUID, dealInfo.State.SectorStartEpoch); err != nil {
					return xerrors.Errorf("marking deal as active: %w", err)
				}
				log.Infow("deal active", "deal", deal.DealUUID, "dealid", deal.DealUUID, "startepoch", dealInfo.State.SectorStartEpoch)
			}
		}
	}

	/* PUBLISHING DEAL CHECKS */
	/* Wait for publish at "good-enough" finality */

	{
		cdm := ributil.CurrentDealInfoManager{CDAPI: gw}

		toCheck, err := r.db.PublishingDeals()
		if err != nil {
			return xerrors.Errorf("get inactive publishing deals: %w", err)
		}

		head, err := gw.ChainHead(ctx) // todo lookback
		if err != nil {
			return xerrors.Errorf("get chain head: %w", err)
		}

		for _, deal := range toCheck {
			var dprop market.ClientDealProposal
			if err := dprop.UnmarshalCBOR(bytes.NewReader(deal.Proposal)); err != nil {
				return xerrors.Errorf("unmarshaling proposal: %w", err)
			}

			pcid, err := cid.Decode(deal.PublishCid)
			if err != nil {
				return xerrors.Errorf("decode publish cid: %w", err)
			}

			// todo somewhere here we'll need to handle published, failed deals

			cdi, err := cdm.GetCurrentDealInfo(ctx, head.Key(), &dprop.Proposal, pcid)
			if err != nil {
				log.Infow("get current deal info", "error", err)
				continue
			}

			pubH, err := gw.ChainGetTipSet(ctx, cdi.PublishMsgTipSet)
			if err != nil {
				return xerrors.Errorf("get publish tipset: %w", err)
			}

			if head.Height()-pubH.Height() > dealPublishFinality {
				if err := r.db.UpdatePublishedDeal(deal.DealUUID, cdi.DealID, cdi.PublishMsgTipSet); err != nil {
					return xerrors.Errorf("marking deal as published: %w", err)
				}
				log.Infow("deal published", "deal", deal.DealUUID, "dealid", cdi.DealID, "publishcid", pcid, "publishheight", pubH.Height(), "headheight", head.Height(), "finality", head.Height()-pubH.Height())
			}
		}
	}

	/* PROPOSED DEAL CHECKS */
	/* Mainly waiting to get publish deal cid */

	walletAddr, err := r.wallet.GetDefault()
	if err != nil {
		return xerrors.Errorf("get wallet address: %w", err)
	}

	{
		toCheck, err := r.db.InactiveDealsToCheck()
		if err != nil {
			return xerrors.Errorf("get inactive deals: %w", err)
		}

		sort.SliceStable(toCheck, func(i, j int) bool {
			return toCheck[i].ProviderAddr < toCheck[j].ProviderAddr
		})

		// todo also check "failed" not expired deals at some lower interval

		sem := make(chan struct{}, ParallelDealChecks)

		for _, deal := range toCheck {
			sem <- struct{}{}
			go func(deal inactiveDealMeta) {
				defer func() {
					<-sem
				}()

				ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
				err := r.runDealCheckQuery(ctx, gw, walletAddr, deal)
				cancel()
				if err != nil {
					log.Warnw("deal check failed", "deal", deal.DealUUID, "provider", fmt.Sprintf("f0%d", deal.ProviderAddr), "error", err)
				}
			}(deal)
		}

		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}
	}

	/* Active deal checks */

	// get deals with deal id, active, not checked in last 24 hours

	// check market deal state

	// Inactive, expired deal cleanup
	{
		head, err := gw.ChainHead(ctx) // todo lookback
		if err != nil {
			return xerrors.Errorf("get chain head: %w", err)
		}

		// todo make configurable, 60 is 30min
		if err := r.db.MarkExpiredDeals(int64(head.Height()) - 60); err != nil {
			return xerrors.Errorf("marking expired deals: %w", err)
		}
	}

	/* fill deal sector numbers */
	{
		toFill, err := r.db.GetSealedDealsWithNoSectorNums()
		if err != nil {
			return xerrors.Errorf("getting sealed deals with no sector nums: %w", err)
		}

		var curProviderDeals map[abi.DealID]abi.SectorNumber
		curProvider := abi.ActorID(0)

		store := adt.WrapStore(ctx, cbor.NewCborStore(blockstore.NewAPIBlockstore(gw)))

		for _, deal := range toFill {
			if abi.ActorID(deal.Provider) != curProvider {
				// TODO post v13 actors sector numbers are in market deal state

				act, err := gw.StateGetActor(ctx, must.One(address.NewIDAddress(uint64(deal.Provider))), types2.EmptyTSK)
				if err != nil {
					return xerrors.Errorf("getting actor: %w", err)
				}

				mas, err := miner.Load(store, act)
				if err != nil {
					return xerrors.Errorf("loading miner actor state: %w", err)
				}

				ms, err := mas.LoadSectors(nil)
				if err != nil {
					return xerrors.Errorf("loading miner sectors: %w", err)
				}

				curProviderDeals = make(map[abi.DealID]abi.SectorNumber)
				for _, s := range ms {
					for _, d := range s.DealIDs {
						curProviderDeals[d] = s.SectorNumber
					}
				}

				curProvider = abi.ActorID(deal.Provider)
			}

			sector, ok := curProviderDeals[abi.DealID(deal.DealID)]
			if !ok {
				log.Warnw("deal sector not found", "deal", deal.DealID, "provider", deal.Provider)
				continue
			}

			if err := r.db.FillDealSectorNumber(deal.UUID, sector); err != nil {
				return xerrors.Errorf("filling deal sector number: %w", err)
			}
		}
	}

	/* deal count checks */

	gs, err := r.db.GetGroupDealStats() // todo swap for GetNonFailedDealCount?
	if err != nil {
		return xerrors.Errorf("getting storage groups: %w", err)
	}

	for gid, gs := range gs {
		if gs.State != ribs2.GroupStateLocalReadyForDeals {
			continue
		}

		if gs.TotalDeals-gs.FailedDeals-gs.Unretrievable < int64(targetReplicaCount) {
			go func(gid ribs2.GroupKey) {
				err := r.makeMoreDeals(context.TODO(), gid, r.host, r.wallet)
				if err != nil {
					log.Errorf("starting new deals: %s", err)
				}
			}(gid)
		} else if gs.Retrievable >= int64(minimumReplicaCount) {
			upStat := r.CarUploadStats().ByGroup
			if upStat[gid] == nil {
				log.Errorw("OFFLOAD GROUP", "group", gid)

				if err := r.Storage().Offload(ctx, gid); err != nil {
					log.Errorw("offloading group", "group", gid, "error", err)
					return xerrors.Errorf("offloading group %d: %w", gid, err)
				}

				if err := r.cleanupS3Offload(gid); err != nil {
					return xerrors.Errorf("cleaning up S3 offload: %w", err)
				}
			} else {
				log.Errorw("NOT OFFLOADING GROUP yet", "group", gid, "retrievable", gs.Retrievable, "uploads", upStat[gid].ActiveRequests)
			}
		}
	}

	return nil
}

func (r *ribs) runDealCheckQuery(ctx context.Context, gw api.Gateway, walletAddr address.Address, deal inactiveDealMeta) error {
	maddr, err := address.NewIDAddress(uint64(deal.ProviderAddr))
	if err != nil {
		return xerrors.Errorf("new id address: %w", err)
	}

	dealUUID, err := uuid.Parse(deal.DealUUID)
	if err != nil {
		return xerrors.Errorf("parse deal uuid: %w", err)
	}

	addrInfo, err := GetAddrInfo(ctx, gw, maddr)
	if err != nil {
		return xerrors.Errorf("get addr info: %w", err)
	}

	if err := r.host.Connect(ctx, *addrInfo); err != nil {
		if err := r.db.UpdateSPDealState(dealUUID, nil, xerrors.Errorf("connect to miner: %w", err)); err != nil {
			return xerrors.Errorf("storing deal state response: %w", err)
		}

		return xerrors.Errorf("connect to miner: %w", err)
	}

	resp, err := r.sendDealStatusRequest(ctx, addrInfo.ID, dealUUID, walletAddr)
	if err := r.db.UpdateSPDealState(dealUUID, resp, err); err != nil {
		return xerrors.Errorf("storing deal state response: %w", err)
	}

	return nil
}

func (r *ribs) sendDealStatusRequest(ctx context.Context, id peer.ID, dealUUID uuid.UUID, caddr address.Address) (*types.DealStatusResponse, error) {
	log.Debugw("send deal status req", "deal-uuid", dealUUID, "id", id)

	uuidBytes, err := dealUUID.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("getting uuid bytes: %w", err)
	}

	sig, err := r.wallet.WalletSign(ctx, caddr, uuidBytes, api.MsgMeta{Type: api.MTDealProposal})
	if err != nil {
		return nil, fmt.Errorf("signing uuid bytes: %w", err)
	}

	// Create a libp2p stream to the provider
	s, err := shared.NewRetryStream(r.host).OpenStream(ctx, id, []protocol.ID{DealStatusV12ProtocolID})
	if err != nil {
		return nil, err
	}

	defer s.Close() // nolint

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(clientWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// Write the deal status request to the stream
	req := types.DealStatusRequest{DealUUID: dealUUID, Signature: *sig}
	if err = cborutil.WriteCborRPC(s, &req); err != nil {
		return nil, fmt.Errorf("sending deal status req: %w", err)
	}

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(clientReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the response from the stream
	var resp types.DealStatusResponse
	if err := resp.UnmarshalCBOR(s); err != nil {
		return nil, fmt.Errorf("reading deal status response: %w", err)
	}

	log.Debugw("received deal status response", "id", resp.DealUUID, "status", resp.DealStatus)

	return &resp, nil
}
