package ributil

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"github.com/filecoin-project/lotus/lib/addrutil"
	"github.com/filecoin-project/lotus/lib/result"
	"github.com/ipfs/kubo/core/node"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v11/util/adt"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/must"
	blockadt "github.com/filecoin-project/specs-actors/actors/util/adt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipfs/go-libipfs/bitswap"
	bsnetwork "github.com/ipfs/go-libipfs/bitswap/network"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

//go:embed chainbootstrap
var bootstrapfs embed.FS

func builtinBootstrap() ([]peer.AddrInfo, error) {
	spi, err := bootstrapfs.ReadFile(path.Join("chainbootstrap", "mainnet.pi"))
	if err != nil {
		return nil, err
	}
	if len(spi) == 0 {
		return nil, nil
	}

	return addrutil.ParseAddresses(context.TODO(), strings.Split(strings.TrimSpace(string(spi)), "\n"))
}

type FullCDU struct {
	gw api.Gateway

	bstore blockstore.Blockstore

	cindex *store.ChainIndex

	mmCache *lru.Cache[cid.Cid, mmCids]
}

// NewFullCDU wraps gateway, which usually has limited lookback, and provides full-node like impl for CDU.
func NewFullCDU(ctx context.Context, gw api.Gateway) (*FullCDU, error) {
	chainBitswapHost, err := libp2p.New(
		libp2p.DefaultTransports, libp2p.DefaultMuxers,
		libp2p.Muxer(mplex.ID, mplex.DefaultTransport),
	)
	if err != nil {
		return nil, xerrors.Errorf("creating chain bitswap host: %w", err)
	}

	bspeers, err := builtinBootstrap()
	if err != nil {
		return nil, xerrors.Errorf("getting bootstrap peers: %w", err)
	}

	rtopts := []dht.Option{dht.Mode(dht.ModeClient),
		dht.Validator(node.RecordValidator(chainBitswapHost.Peerstore())),
		//dht.ProtocolPrefix(build.DhtProtocolName(nn)),
		dht.QueryFilter(dht.PublicQueryFilter),
		dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
		//dht.DisableProviders(),
		//dht.DisableValues(),
		dht.BootstrapPeers(bspeers...),
	}
	d, err := dht.New(
		ctx, chainBitswapHost, rtopts...,
	)
	if err != nil {
		return nil, xerrors.Errorf("creating chain dht client: %w", err)
	}

	bitswapNetwork := bsnetwork.NewFromIpfsHost(chainBitswapHost, d)
	bitswapOptions := []bitswap.Option{bitswap.ProvideEnabled(false)}

	apiBs := blockstore.NewAPIBlockstore(gw)

	bstore := ARCStore(apiBs)

	bswap := bitswap.New(ctx, bitswapNetwork, bstore, bitswapOptions...)

	fbs := &FallbackStore{
		Blockstore: bstore,
	}

	var bootstrapOnce sync.Once

	fbs.SetFallback(func(ctx context.Context, c cid.Cid) (blocks.Block, error) {
		bootstrapOnce.Do(func() {
			for _, p := range bspeers {
				go func(p peer.AddrInfo) {
					if err := chainBitswapHost.Connect(context.TODO(), p); err != nil {
						log.Errorw("failed to connect to bootstrap peer", "error", err)
					}
				}(p)
			}

			go func() {
				time.Sleep(1 * time.Second)
				err := d.Bootstrap(context.TODO())
				if err != nil {
					log.Errorw("failed to bootstrap dht", "error", err)
				}
			}()

			time.Sleep(3 * time.Second)
		})

		debug.PrintStack()

		log.Errorw("falling back to bitswap", "cid", c)
		return bswap.GetBlock(ctx, c)
	})

	fc := &FullCDU{gw: gw, bstore: fbs,
		mmCache: must.One(lru.New[cid.Cid, mmCids](2048)),
	}
	fc.cindex = store.NewChainIndex(fc.LoadTipSet)

	go func() {
		head, err := fc.gw.ChainHead(ctx)
		if err != nil {
			log.Errorw("FullCDU cache warmup: failed to get head", "error", err)
			return
		}

		to := head.Height() - builtin.EpochsInDay*14
		start := time.Now()
		_, err = fc.cindex.GetTipsetByHeight(ctx, head, to)
		if err != nil {
			log.Errorw("FullCDU cache warmup: failed to load tipset", "error", err)
			return
		}

		log.Infow("FullCDU cache warmup: done", "took", time.Since(start))
	}()

	return fc, nil
}

func (f *FullCDU) ChainGetMessage(ctx context.Context, cid cid.Cid) (*types.Message, error) {
	cm, err := f.GetCMessage(ctx, cid)
	if err != nil {
		return nil, xerrors.Errorf("fullcdu get message: %w", err)
	}

	return cm.VMMessage(), nil
}

func (f *FullCDU) StateLookupID(ctx context.Context, a address.Address, tsk types.TipSetKey) (address.Address, error) {
	ts, err := f.GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return address.Undef, xerrors.Errorf("loading tipset %s: %w", tsk, err)
	}

	cst := cbor.NewCborStore(f.bstore)
	st, err := state.LoadStateTree(cst, ts.ParentState())
	if err != nil {
		return address.Undef, xerrors.Errorf("load state tree: %w", err)
	}
	return st.LookupID(a)
}

func (f *FullCDU) StateMarketStorageDeal(ctx context.Context, id abi.DealID, tsk types.TipSetKey) (*api.MarketDeal, error) {
	act, err := f.StateGetActor(ctx, market.Address, tsk)
	if err != nil {
		return nil, xerrors.Errorf("failed to load market actor: %w", err)
	}

	state, err := market.Load(ActorStore(ctx, f.bstore), act)
	if err != nil {
		return nil, xerrors.Errorf("failed to load market actor state: %w", err)
	}

	proposals, err := state.Proposals()
	if err != nil {
		return nil, xerrors.Errorf("failed to get proposals from state : %w", err)
	}

	proposal, found, err := proposals.Get(id)

	if err != nil {
		return nil, xerrors.Errorf("failed to get proposal : %w", err)
	} else if !found {
		return nil, xerrors.Errorf(
			"deal %d not found "+
				"- deal may not have completed sealing before deal proposal "+
				"start epoch, or deal may have been slashed",
			id)
	}

	states, err := state.States()
	if err != nil {
		return nil, xerrors.Errorf("failed to get states : %w", err)
	}

	st, found, err := states.Get(id)
	if err != nil {
		return nil, xerrors.Errorf("failed to get state : %w", err)
	}

	if !found {
		st = market.EmptyDealState()
	}

	return &api.MarketDeal{
		Proposal: *proposal,
		State:    *st,
	}, nil
}

func (f *FullCDU) StateSearchMsg(ctx context.Context, tsk types.TipSetKey, msg cid.Cid, lookbackLimit abi.ChainEpoch, allowReplaced bool) (*api.MsgLookup, error) {
	fromTs, err := f.GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
	}

	ts, recpt, found, err := f.SearchForMessage(ctx, fromTs, msg, lookbackLimit, allowReplaced)
	if err != nil {
		return nil, err
	}

	if ts != nil {
		return &api.MsgLookup{
			Message: found,
			Receipt: *recpt,
			TipSet:  ts.Key(),
			Height:  ts.Height(),
		}, nil
	}
	return nil, nil
}

func (f *FullCDU) StateNetworkVersion(ctx context.Context, tsk types.TipSetKey) (network.Version, error) {
	return f.StateNetworkVersion(ctx, tsk)
}

func (f *FullCDU) ChainGetTipSet(ctx context.Context, tsk types.TipSetKey) (*types.TipSet, error) {
	return f.GetTipSetFromKey(ctx, tsk)
}

func (f *FullCDU) ChainGetTipSetByHeight(ctx context.Context, h abi.ChainEpoch, tsk types.TipSetKey) (*types.TipSet, error) {
	ts, err := f.GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
	}

	if h > ts.Height() {
		return nil, xerrors.Errorf("looking for tipset with height greater than start point")
	}

	if h == ts.Height() {
		return ts, nil
	}

	lbts, err := f.cindex.GetTipsetByHeight(ctx, ts, h)
	if err != nil {
		return nil, err
	}

	if lbts.Height() < h {
		log.Warnf("chain index returned the wrong tipset at height %d, using slow retrieval", h)
		lbts, err = f.cindex.GetTipsetByHeightWithoutCache(ctx, ts, h)
		if err != nil {
			return nil, err
		}
	}

	prev := true

	if lbts.Height() == h || !prev {
		return lbts, nil
	}

	return f.LoadTipSet(ctx, lbts.Parents())
}

func (f *FullCDU) StateGetActor(ctx context.Context, actor address.Address, tsk types.TipSetKey) (*types.Actor, error) {
	ts, err := f.GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
	}

	state, err := f.ParentState(ts)
	if err != nil {
		return nil, err
	}
	return state.GetActor(actor)
}

// borrowed from lotus chainstore / statestore

func ActorStore(ctx context.Context, bs bstore.Blockstore) adt.Store {
	return adt.WrapStore(ctx, cbor.NewCborStore(bs))
}

func (f *FullCDU) ParentState(ts *types.TipSet) (*state.StateTree, error) {
	cst := cbor.NewCborStore(f.bstore)
	state, err := state.LoadStateTree(cst, ts.ParentState())
	if err != nil {
		return nil, xerrors.Errorf("load state tree: %w", err)
	}

	return state, nil
}

func (f *FullCDU) GetTipSetFromKey(ctx context.Context, tsk types.TipSetKey) (*types.TipSet, error) {
	if tsk.IsEmpty() {
		return f.gw.ChainHead(ctx)
	}
	return f.LoadTipSet(ctx, tsk)
}

func (f *FullCDU) LoadTipSet(ctx context.Context, tsk types.TipSetKey) (*types.TipSet, error) {
	// Fetch tipset block headers from blockstore in parallel
	var eg errgroup.Group
	cids := tsk.Cids()
	blks := make([]*types.BlockHeader, len(cids))
	for i, c := range cids {
		i, c := i, c
		eg.Go(func() error {
			var blk *types.BlockHeader
			err := f.bstore.View(ctx, c, func(b []byte) (err error) {
				blk, err = types.DecodeBlock(b)
				return err
			})
			if err != nil {
				return err
			}

			blks[i] = blk
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	ts, err := types.NewTipSet(blks)
	if err != nil {
		return nil, err
	}

	return ts, nil
}

func (f *FullCDU) GetCMessage(ctx context.Context, c cid.Cid) (types.ChainMsg, error) {
	res := make(chan result.Result[types.ChainMsg], 2)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	go func() {
		res <- result.Wrap[types.ChainMsg](f.GetMessage(ctx, c))
	}()
	go func() {
		res <- result.Wrap[types.ChainMsg](f.GetSignedMessage(ctx, c))
	}()

	r1, r2 := <-res, <-res

	if r1.Error != nil && r2.Error != nil {
		return nil, xerrors.Errorf("failed to fetch message: %w, %w", r1.Error, r2.Error)
	}

	if r1.Error != nil {
		return r2.Value, nil
	}

	return r1.Value, nil
}

func (f *FullCDU) GetMessage(ctx context.Context, c cid.Cid) (*types.Message, error) {
	var msg *types.Message
	err := f.bstore.View(ctx, c, func(b []byte) (err error) {
		msg, err = types.DecodeMessage(b)
		return err
	})
	return msg, err
}

func (f *FullCDU) GetSignedMessage(ctx context.Context, c cid.Cid) (*types.SignedMessage, error) {
	var msg *types.SignedMessage
	err := f.bstore.View(ctx, c, func(b []byte) (err error) {
		msg, err = types.DecodeSignedMessage(b)
		return err
	})
	return msg, err
}

// message search, borrowed from lotus as well

func (f *FullCDU) SearchForMessage(ctx context.Context, head *types.TipSet, mcid cid.Cid, lookbackLimit abi.ChainEpoch, allowReplaced bool) (*types.TipSet, *types.MessageReceipt, cid.Cid, error) {
	msg, err := f.GetCMessage(ctx, mcid)
	if err != nil {
		return nil, nil, cid.Undef, fmt.Errorf("failed to load message: %w", err)
	}

	r, foundMsg, err := f.tipsetExecutedMessage(ctx, head, mcid, msg.VMMessage(), allowReplaced)
	if err != nil {
		return nil, nil, cid.Undef, err
	}

	if r != nil {
		return head, r, foundMsg, nil
	}

	fts, r, foundMsg, err := f.searchBackForMsg(ctx, head, msg, lookbackLimit, allowReplaced)

	if err != nil {
		log.Warnf("failed to look back through chain for message %s", mcid)
		return nil, nil, cid.Undef, err
	}

	if fts == nil {
		return nil, nil, cid.Undef, nil
	}

	return fts, r, foundMsg, nil
}

const LookbackNoLimit = api.LookbackNoLimit

func (f *FullCDU) searchBackForMsg(ctx context.Context, from *types.TipSet, m types.ChainMsg, limit abi.ChainEpoch, allowReplaced bool) (*types.TipSet, *types.MessageReceipt, cid.Cid, error) {
	limitHeight := from.Height() - limit
	noLimit := limit == LookbackNoLimit

	cur := from
	curActor, err := f.StateGetActor(ctx, m.VMMessage().From, cur.Key())
	if err != nil {
		return nil, nil, cid.Undef, xerrors.Errorf("failed to load initital tipset")
	}

	mFromId, err := f.StateLookupID(ctx, m.VMMessage().From, from.Key())
	if err != nil {
		return nil, nil, cid.Undef, xerrors.Errorf("looking up From id address: %w", err)
	}

	mNonce := m.VMMessage().Nonce

	for {
		// If we've reached the genesis block, or we've reached the limit of
		// how far back to look
		if cur.Height() == 0 || !noLimit && cur.Height() <= limitHeight {
			// it ain't here!
			return nil, nil, cid.Undef, nil
		}

		select {
		case <-ctx.Done():
			return nil, nil, cid.Undef, nil
		default:
		}

		// we either have no messages from the sender, or the latest message we found has a lower nonce than the one being searched for,
		// either way, no reason to lookback, it ain't there
		if curActor == nil || curActor.Nonce == 0 || curActor.Nonce < mNonce {
			return nil, nil, cid.Undef, nil
		}

		pts, err := f.LoadTipSet(ctx, cur.Parents())
		if err != nil {
			return nil, nil, cid.Undef, xerrors.Errorf("failed to load tipset during msg wait searchback: %w", err)
		}

		act, err := f.StateGetActor(ctx, mFromId, pts.Key())
		actorNoExist := errors.Is(err, types.ErrActorNotFound)
		if err != nil && !actorNoExist {
			return nil, nil, cid.Cid{}, xerrors.Errorf("failed to load the actor: %w", err)
		}

		// check that between cur and parent tipset the nonce fell into range of our message
		if actorNoExist || (curActor.Nonce > mNonce && act.Nonce <= mNonce) {
			r, foundMsg, err := f.tipsetExecutedMessage(ctx, cur, m.Cid(), m.VMMessage(), allowReplaced)
			if err != nil {
				return nil, nil, cid.Undef, xerrors.Errorf("checking for message execution during lookback: %w", err)
			}

			if r != nil {
				return cur, r, foundMsg, nil
			}
		}

		cur = pts
		curActor = act
	}
}

func (f *FullCDU) tipsetExecutedMessage(ctx context.Context, ts *types.TipSet, msg cid.Cid, vmm *types.Message, allowReplaced bool) (*types.MessageReceipt, cid.Cid, error) {
	// The genesis block did not execute any messages
	if ts.Height() == 0 {
		return nil, cid.Undef, nil
	}

	pts, err := f.LoadTipSet(ctx, ts.Parents())
	if err != nil {
		return nil, cid.Undef, err
	}

	cm, err := f.MessagesForTipset(ctx, pts)
	if err != nil {
		return nil, cid.Undef, err
	}

	for ii := range cm {
		// iterate in reverse because we going backwards through the chain
		i := len(cm) - ii - 1
		m := cm[i]

		if m.VMMessage().From == vmm.From { // cheaper to just check origin first
			if m.VMMessage().Nonce == vmm.Nonce {
				if !m.VMMessage().EqualCall(vmm) {
					// this is an entirely different message, fail
					return nil, cid.Undef, xerrors.Errorf("found message with equal nonce as the one we are looking for that is NOT a valid replacement message (F:%s n %d, TS: %s n%d)",
						msg, vmm.Nonce, m.Cid(), m.VMMessage().Nonce)
				}

				if m.Cid() != msg {
					if !allowReplaced {
						log.Warnw("found message with equal nonce and call params but different CID",
							"wanted", msg, "found", m.Cid(), "nonce", vmm.Nonce, "from", vmm.From)
						return nil, cid.Undef, xerrors.Errorf("found message with equal nonce as the one we are looking for (F:%s n %d, TS: %s n%d)",
							msg, vmm.Nonce, m.Cid(), m.VMMessage().Nonce)
					}
				}

				pr, err := f.GetParentReceipt(ctx, ts.Blocks()[0], i)
				if err != nil {
					return nil, cid.Undef, err
				}

				return pr, m.Cid(), nil
			}
			if m.VMMessage().Nonce < vmm.Nonce {
				return nil, cid.Undef, nil // don't bother looking further
			}
		}
	}

	return nil, cid.Undef, nil
}

func (f *FullCDU) GetParentReceipt(ctx context.Context, b *types.BlockHeader, i int) (*types.MessageReceipt, error) {
	// block headers use adt0, for now.
	a, err := blockadt.AsArray(ActorStore(ctx, f.bstore), b.ParentMessageReceipts)
	if err != nil {
		return nil, xerrors.Errorf("amt load: %w", err)
	}

	var r types.MessageReceipt
	if found, err := a.Get(uint64(i), &r); err != nil {
		return nil, err
	} else if !found {
		return nil, xerrors.Errorf("failed to find receipt %d", i)
	}

	return &r, nil
}

func (f *FullCDU) MessagesForTipset(ctx context.Context, ts *types.TipSet) ([]types.ChainMsg, error) {
	bmsgs, err := f.BlockMsgsForTipset(ctx, ts)
	if err != nil {
		return nil, err
	}

	var out []types.ChainMsg
	for _, bm := range bmsgs {
		for _, blsm := range bm.BlsMessages {
			out = append(out, blsm)
		}

		for _, secm := range bm.SecpkMessages {
			out = append(out, secm)
		}
	}

	return out, nil
}

type BlockMessages struct {
	Miner         address.Address
	BlsMessages   []types.ChainMsg
	SecpkMessages []types.ChainMsg
}

func (f *FullCDU) BlockMsgsForTipset(ctx context.Context, ts *types.TipSet) ([]BlockMessages, error) {
	// returned BlockMessages match block order in tipset

	applied := make(map[address.Address]uint64)

	cst := cbor.NewCborStore(f.bstore)
	st, err := state.LoadStateTree(cst, ts.Blocks()[0].ParentStateRoot)
	if err != nil {
		return nil, xerrors.Errorf("failed to load state tree at tipset %s: %w", ts, err)
	}

	useIds := false
	selectMsg := func(m *types.Message) (bool, error) {
		var sender address.Address
		if ts.Height() >= build.UpgradeHyperdriveHeight {
			if useIds {
				sender, err = st.LookupID(m.From)
				if err != nil {
					return false, xerrors.Errorf("failed to resolve sender: %w", err)
				}
			} else {
				if m.From.Protocol() != address.ID {
					// we haven't been told to use IDs, just use the robust addr
					sender = m.From
				} else {
					// uh-oh, we actually have an ID-sender!
					useIds = true
					for robust, nonce := range applied {
						resolved, err := st.LookupID(robust)
						if err != nil {
							return false, xerrors.Errorf("failed to resolve sender: %w", err)
						}
						applied[resolved] = nonce
					}

					sender, err = st.LookupID(m.From)
					if err != nil {
						return false, xerrors.Errorf("failed to resolve sender: %w", err)
					}
				}
			}
		} else {
			sender = m.From
		}

		// The first match for a sender is guaranteed to have correct nonce -- the block isn't valid otherwise
		if _, ok := applied[sender]; !ok {
			applied[sender] = m.Nonce
		}

		if applied[sender] != m.Nonce {
			return false, nil
		}

		applied[sender]++

		return true, nil
	}

	var out []BlockMessages
	for _, b := range ts.Blocks() {

		bms, sms, err := f.MessagesForBlock(ctx, b)
		if err != nil {
			return nil, xerrors.Errorf("failed to get messages for block: %w", err)
		}

		bm := BlockMessages{
			Miner:         b.Miner,
			BlsMessages:   make([]types.ChainMsg, 0, len(bms)),
			SecpkMessages: make([]types.ChainMsg, 0, len(sms)),
		}

		for _, bmsg := range bms {
			b, err := selectMsg(bmsg.VMMessage())
			if err != nil {
				return nil, xerrors.Errorf("failed to decide whether to select message for block: %w", err)
			}

			if b {
				bm.BlsMessages = append(bm.BlsMessages, bmsg)
			}
		}

		for _, smsg := range sms {
			b, err := selectMsg(smsg.VMMessage())
			if err != nil {
				return nil, xerrors.Errorf("failed to decide whether to select message for block: %w", err)
			}

			if b {
				bm.SecpkMessages = append(bm.SecpkMessages, smsg)
			}
		}

		out = append(out, bm)
	}

	return out, nil
}

func (f *FullCDU) MessagesForBlock(ctx context.Context, b *types.BlockHeader) ([]*types.Message, []*types.SignedMessage, error) {
	blscids, secpkcids, err := f.ReadMsgMetaCids(ctx, b.Messages)
	if err != nil {
		return nil, nil, err
	}

	blsmsgs, err := f.LoadMessagesFromCids(ctx, blscids)
	if err != nil {
		return nil, nil, xerrors.Errorf("loading bls messages for block: %w", err)
	}

	secpkmsgs, err := f.LoadSignedMessagesFromCids(ctx, secpkcids)
	if err != nil {
		return nil, nil, xerrors.Errorf("loading secpk messages for block: %w", err)
	}

	return blsmsgs, secpkmsgs, nil
}

type mmCids struct {
	bls   []cid.Cid
	secpk []cid.Cid
}

func (f *FullCDU) ReadMsgMetaCids(ctx context.Context, mmc cid.Cid) ([]cid.Cid, []cid.Cid, error) {
	if mmcids, ok := f.mmCache.Get(mmc); ok {
		return mmcids.bls, mmcids.secpk, nil
	}

	cst := cbor.NewCborStore(f.bstore)
	var msgmeta types.MsgMeta
	if err := cst.Get(ctx, mmc, &msgmeta); err != nil {
		return nil, nil, xerrors.Errorf("failed to load msgmeta (%s): %w", mmc, err)
	}

	blscids, err := f.readAMTCids(msgmeta.BlsMessages)
	if err != nil {
		return nil, nil, xerrors.Errorf("loading bls message cids for block: %w", err)
	}

	secpkcids, err := f.readAMTCids(msgmeta.SecpkMessages)
	if err != nil {
		return nil, nil, xerrors.Errorf("loading secpk message cids for block: %w", err)
	}

	f.mmCache.Add(mmc, mmCids{
		bls:   blscids,
		secpk: secpkcids,
	})

	return blscids, secpkcids, nil
}

func (f *FullCDU) readAMTCids(root cid.Cid) ([]cid.Cid, error) {
	ctx := context.TODO()
	// block headers use adt0, for now.
	a, err := blockadt.AsArray(ActorStore(ctx, f.bstore), root)
	if err != nil {
		return nil, xerrors.Errorf("amt load: %w", err)
	}

	var (
		cids    []cid.Cid
		cborCid cbg.CborCid
	)
	if err := a.ForEach(&cborCid, func(i int64) error {
		c := cid.Cid(cborCid)
		cids = append(cids, c)
		return nil
	}); err != nil {
		return nil, xerrors.Errorf("failed to traverse amt: %w", err)
	}

	if uint64(len(cids)) != a.Length() {
		return nil, xerrors.Errorf("found %d cids, expected %d", len(cids), a.Length())
	}

	return cids, nil
}

func (f *FullCDU) LoadMessagesFromCids(ctx context.Context, cids []cid.Cid) ([]*types.Message, error) {
	msgs := make([]*types.Message, 0, len(cids))
	for i, c := range cids {
		m, err := f.GetMessage(ctx, c)
		if err != nil {
			return nil, xerrors.Errorf("failed to get message: (%s):%d: %w", c, i, err)
		}

		msgs = append(msgs, m)
	}

	return msgs, nil
}

func (f *FullCDU) LoadSignedMessagesFromCids(ctx context.Context, cids []cid.Cid) ([]*types.SignedMessage, error) {
	msgs := make([]*types.SignedMessage, 0, len(cids))
	for i, c := range cids {
		m, err := f.GetSignedMessage(ctx, c)
		if err != nil {
			return nil, xerrors.Errorf("failed to get message: (%s):%d: %w", c, i, err)
		}

		msgs = append(msgs, m)
	}

	return msgs, nil
}

var _ CurrentDealInfoAPI = (*FullCDU)(nil)
