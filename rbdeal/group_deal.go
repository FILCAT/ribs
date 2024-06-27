package rbdeal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	gobig "math/big"
	"time"

	iface "github.com/atboosty/ribs"
	"github.com/atboosty/ribs/ributil"
	types "github.com/atboosty/ribs/ributil/boosttypes"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"golang.org/x/xerrors"
)

const DealProtocolv120 = "/fil/storage/mk/1.2.0"

type ErrRejected struct {
	Reason string
}

func (e ErrRejected) Error() string {
	return fmt.Sprintf("deal proposal rejected: %s", e.Reason)
}

func (r *ribs) makeMoreDeals(ctx context.Context, id iface.GroupKey, h host.Host, w *ributil.LocalWallet) error {
	r.dealsLk.Lock()
	if _, ok := r.moreDealsLocks[id]; ok {
		r.dealsLk.Unlock()

		// another goroutine is already making deals for this group
		return nil
	}
	r.moreDealsLocks[id] = struct{}{}
	r.dealsLk.Unlock()

	defer func() {
		r.dealsLk.Lock()
		delete(r.moreDealsLocks, id)
		r.dealsLk.Unlock()
	}()

	if err := r.maybeEnsureS3Offload(id); err != nil {
		return xerrors.Errorf("attempting s3 offload: %w", err)
	}

	dealInfo, err := r.db.GetDealParams(ctx, id)
	if err != nil {
		return xerrors.Errorf("get deal params: %w", err)
	}

	notFailed, err := r.db.GetNonFailedDealCount(id)
	if err != nil {
		log.Errorf("getting non-failed deal count: %s", err)
		return xerrors.Errorf("getting non-failed deal count: %w", err)
	}

	if notFailed >= targetReplicaCount {
		// occasionally in some racy cases we can end up here
		return nil
	}

	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		return xerrors.Errorf("creating gateway rpc client: %w", err)
	}
	defer closer()

	walletAddr, err := w.GetDefault()
	if err != nil {
		return xerrors.Errorf("get wallet address: %w", err)
	}

	vc, err := gw.StateVerifiedClientStatus(ctx, walletAddr, ctypes.EmptyTSK)
	if err != nil {
		return xerrors.Errorf("getting verified client status: %w", err)
	}

	verified := false
	maxToPay := maxPrice

	if vc != nil {
		if vc.LessThan(minDatacap) {
			return xerrors.Errorf("not starting additional verified deals: datacap too low (%s, min %s)", ctypes.SizeStr(*vc), ctypes.SizeStr(minDatacap))
		}

		maxToPay = maxVerifPrice
		verified = true
	}

	provs, err := r.db.SelectDealProviders(id, dealInfo.PieceSize, verified, maxToPay)
	if err != nil {
		return xerrors.Errorf("select deal providers: %w", err)
	}

	pieceCid, err := commcid.PieceCommitmentV1ToCID(dealInfo.CommP)
	if err != nil {
		return fmt.Errorf("failed to convert commP to cid: %w", err)
	}

	makeDealWith := func(prov dealProvider) error {
		// check proposal params
		maddr, err := address.NewIDAddress(uint64(prov.id))
		if err != nil {
			return xerrors.Errorf("new id address: %w", err)
		}

		addrInfo, err := GetAddrInfo(ctx, gw, maddr)
		if err != nil {
			return xerrors.Errorf("get addr info: %w", err)
		}

		var providerCollateral abi.TokenAmount

		bounds, err := gw.StateDealProviderCollateralBounds(ctx, abi.PaddedPieceSize(dealInfo.PieceSize), verified, ctypes.EmptyTSK)
		if err != nil {
			return fmt.Errorf("node error getting collateral bounds: %w", err)
		}
		providerCollateral = big.Div(big.Mul(bounds.Min, big.NewInt(6)), big.NewInt(5)) // add 20%

		head, err := gw.ChainHead(ctx)
		if err != nil {
			return fmt.Errorf("getting chain head: %w", err)
		}

		startEpoch := head.Height() + dealStartTime

		// generate proposal
		dealUuid := uuid.New()

		duration := 530 * builtin.EpochsInDay

		pricef := gobig.NewFloat(prov.ask_price)
		if verified {
			pricef = gobig.NewFloat(prov.ask_verif_price)
		}

		price := big.Zero()
		pricef.Int(price.Int)

		dealProposal, err := dealProposal(ctx, w, walletAddr, dealInfo.Root, abi.PaddedPieceSize(dealInfo.PieceSize), pieceCid, maddr, startEpoch, duration, verified, providerCollateral, price)
		if err != nil {
			return fmt.Errorf("failed to create a deal proposal: %w", err)
		}

		var proposalBuf bytes.Buffer
		if err := dealProposal.MarshalCBOR(&proposalBuf); err != nil {
			return fmt.Errorf("failed to marshal deal proposal: %w", err)
		}

		// generate transfer token
		reqToken, err := r.makeCarRequestToken(context.TODO(), id, time.Hour*36, dealInfo.CarSize, dealUuid)
		if err != nil {
			return xerrors.Errorf("make car request token: %w", err)
		}

		transferParams := &types.HttpRequest{URL: "libp2p://" + h.Addrs()[0].String() + "/p2p/" + h.ID().String()} // todo get from autonat / config
		transferParams.Headers = map[string]string{
			"Authorization": string(reqToken),
		}

		paramsBytes, err := json.Marshal(transferParams)
		if err != nil {
			return fmt.Errorf("marshalling request parameters: %w", err)
		}

		transfer := types.Transfer{
			Type:   "libp2p",
			Params: paramsBytes,
			Size:   uint64(dealInfo.CarSize),
		}

		dealParams := types.DealParams{
			DealUUID:           dealUuid,
			ClientDealProposal: *dealProposal,
			DealDataRoot:       dealInfo.Root,
			IsOffline:          false,
			Transfer:           transfer,
		}

		di := dbDealInfo{
			DealUUID:            dealUuid.String(),
			GroupID:             id,
			ClientAddr:          walletAddr.String(),
			ProviderAddr:        prov.id,
			PricePerEpoch:       price.Int64(),
			Verified:            verified,
			KeepUnsealed:        true,
			StartEpoch:          startEpoch,
			EndEpoch:            startEpoch + abi.ChainEpoch(duration),
			SignedProposalBytes: proposalBuf.Bytes(),
		}

		err = r.db.StoreDealProposal(di)
		if err != nil {
			return fmt.Errorf("saving deal info: %w", err)
		}

		if price.GreaterThan(big.NewInt(int64(maxToPay))) {
			err = r.db.StoreRejectedDeal(dealUuid.String(), fmt.Sprintf("price %d is greater than max price %f", price, maxToPay), 0)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			// this check is probably redundant, buuut..
			return fmt.Errorf("price %d is greater than max price %f", price, maxToPay)
		}

		if err := h.Connect(ctx, *addrInfo); err != nil {
			err = r.db.StoreRejectedDeal(di.DealUUID, fmt.Sprintf("failed to connect to miner: %s", err), 0)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return xerrors.Errorf("connect to miner: %w", err)
		}

		x, err := h.Peerstore().FirstSupportedProtocol(addrInfo.ID, DealProtocolv120)
		if err != nil {
			err = r.db.StoreRejectedDeal(di.DealUUID, fmt.Sprintf("failed to connect to miner: %s", err), 0)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return fmt.Errorf("getting protocols for peer %s: %w", addrInfo.ID, err)
		}

		if len(x) == 0 {
			err := fmt.Errorf("boost client cannot make a deal with storage provider %s because it does not support protocol version 1.2.0", maddr)

			if err := r.db.StoreRejectedDeal(di.DealUUID, err.Error(), 0); err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return err
		}

		// MAKE THE DEAL

		s, err := h.NewStream(ctx, addrInfo.ID, DealProtocolv120)
		if err != nil {
			err = r.db.StoreRejectedDeal(di.DealUUID, xerrors.Errorf("opening deal proposal stream: %w", err).Error(), 0)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return fmt.Errorf("failed to open stream to peer %s: %w", addrInfo.ID, err)
		}
		defer s.Close()

		var resp types.DealResponse
		if err := doRpc(ctx, s, &dealParams, &resp); err != nil {
			err = r.db.StoreRejectedDeal(di.DealUUID, xerrors.Errorf("sending deal proposal rpc: %w", err).Error(), 0)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return fmt.Errorf("send proposal rpc: %w", err)
		}

		if !resp.Accepted {
			err = r.db.StoreRejectedDeal(di.DealUUID, resp.Message, 1)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return ErrRejected{Reason: resp.Message}
		}

		if err := r.db.StoreSuccessfullyProposedDeal(di); err != nil {
			return xerrors.Errorf("marking deal as successfully proposed: %w", err)
		}

		log.Warnf("Deal %s with %s accepted for group %d!!!", dealUuid, maddr, id)

		return nil
	}

	// make deals with candidates
	for _, prov := range provs {
		err := makeDealWith(prov)
		if err == nil {
			notFailed++

			if notFailed >= targetReplicaCount {
				// enough
				break
			}

			// deal made
			continue
		}
		/*if re, ok := err.(ErrRejected); ok {
			// deal rejected
			continue
		}*/

		log.Errorw("failed to make deal with provider", "provider", fmt.Sprintf("f0%d", prov.id), "error", err)
	}

	return nil
}

func dealProposal(ctx context.Context, w *ributil.LocalWallet, clientAddr address.Address, rootCid cid.Cid, pieceSize abi.PaddedPieceSize, pieceCid cid.Cid, minerAddr address.Address, startEpoch abi.ChainEpoch, duration int, verified bool, providerCollateral abi.TokenAmount, storagePrice abi.TokenAmount) (*market.ClientDealProposal, error) {
	endEpoch := startEpoch + abi.ChainEpoch(duration)
	// deal proposal expects total storage price for deal per epoch, therefore we
	// multiply pieceSize * storagePrice (which is set per epoch per GiB) and divide by 2^30
	storagePricePerEpochForDeal := big.Div(big.Mul(big.NewInt(int64(pieceSize)), storagePrice), big.NewInt(int64(1<<30)))
	l, err := market.NewLabelFromString(rootCid.String())
	if err != nil {
		return nil, err
	}
	proposal := market.DealProposal{
		PieceCID:             pieceCid,
		PieceSize:            pieceSize,
		VerifiedDeal:         verified,
		Client:               clientAddr,
		Provider:             minerAddr,
		Label:                l,
		StartEpoch:           startEpoch,
		EndEpoch:             endEpoch,
		StoragePricePerEpoch: storagePricePerEpochForDeal,
		ProviderCollateral:   providerCollateral,
	}

	buf, err := cborutil.Dump(&proposal)
	if err != nil {
		return nil, err
	}

	sig, err := w.WalletSign(ctx, clientAddr, buf, api.MsgMeta{Type: api.MTDealProposal})
	if err != nil {
		return nil, fmt.Errorf("wallet sign failed: %w", err)
	}

	return &market.ClientDealProposal{
		Proposal:        proposal,
		ClientSignature: *sig,
	}, nil
}
