package rbdeal

import (
	"context"
	"time"

	iface "github.com/atboosty/ribs"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/actors"
	marketactor "github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
)

func (r *ribs) MarketAdd(ctx context.Context, amount abi.TokenAmount) (cid.Cid, error) {
	r.msgSendLk.Lock()
	defer r.msgSendLk.Unlock()

	r.marketFundsLk.Lock()
	defer r.marketFundsLk.Unlock()

	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}
	defer closer()

	w, err := r.wallet.GetDefault()
	if err != nil {
		return cid.Undef, xerrors.Errorf("getting default wallet: %w", err)
	}

	params, err := actors.SerializeParams(&w)
	if err != nil {
		return cid.Undef, err
	}

	m := &types.Message{
		To:     marketactor.Address,
		From:   w,
		Value:  amount,
		Method: marketactor.Methods.AddBalance,
		Params: params,
	}

	nc, err := gw.MpoolGetNonce(ctx, w)
	if err != nil {
		return cid.Cid{}, xerrors.Errorf("mpool get gas: %w", err)
	}

	m.Nonce = nc

	m, err = gw.GasEstimateMessageGas(ctx, m, nil, types.EmptyTSK)
	if err != nil {
		return cid.Cid{}, xerrors.Errorf("gas estimate message gas: %w", err)
	}

	sig, err := r.wallet.WalletSign(ctx, w, m.Cid().Bytes(), api.MsgMeta{
		Type: api.MTChainMsg,
	})
	if err != nil {
		return cid.Cid{}, xerrors.Errorf("signing message: %w", err)
	}

	sm := &types.SignedMessage{
		Message:   *m,
		Signature: *sig,
	}

	c, aerr := gw.MpoolPush(ctx, sm)
	if aerr != nil {
		return cid.Undef, aerr
	}

	log.Infow("add market balance", "cid", c, "amount", amount)

	return c, nil
}

func (r *ribs) MarketWithdraw(ctx context.Context, amount abi.TokenAmount) (cid.Cid, error) {
	//TODO implement me
	panic("implement me")
}

func (r *ribs) Withdraw(ctx context.Context, amount abi.TokenAmount, to address.Address) (cid.Cid, error) {
	//TODO implement me
	panic("implement me")
}

func (r *ribs) watchMarket(ctx context.Context) {
	defer close(r.marketWatchClosed)

	for {
		select {
		case <-r.close:
			return
		default:
		}

		i, err := r.WalletInfo()
		if err != nil {
			goto cooldown
		}
		{
			avail := types.BigSub(i.MarketBalanceDetailed.Escrow, i.MarketBalanceDetailed.Locked)

			if avail.GreaterThan(minMarketBalance) {
				goto cooldown
			}

			log.Infow("market balance low, topping up")

			toAdd := big.Sub(autoMarketBalance, avail)

			c, err := r.MarketAdd(ctx, toAdd)
			if err != nil {
				log.Errorw("error adding market funds", "error", err)
				goto cooldown
			}

			log.Errorw("AUTO-ADDED MARKET FUNDS", "amount", types.FIL(toAdd), "msg", c)
		}

	cooldown:
		select {
		case <-r.close:
			return
		case <-time.After(2 * walletUpgradeInterval):
		}
	}
}

func _must(err error, msgAndArgs ...interface{}) {
	if err != nil {
		if len(msgAndArgs) == 0 {
			panic(err)
		}
		panic(xerrors.Errorf(msgAndArgs[0].(string)+": %w", err))
	}
}

func (r *ribs) WalletInfo() (iface.WalletInfo, error) {
	r.marketFundsLk.Lock()
	defer r.marketFundsLk.Unlock()

	if r.cachedWalletInfo != nil && time.Since(r.lastWalletInfoUpdate) < walletUpgradeInterval {
		return *r.cachedWalletInfo, nil
	}

	addr, err := r.wallet.GetDefault()
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get default wallet: %w", err)
	}

	ctx := context.TODO()

	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}
	defer closer()

	b, err := gw.WalletBalance(ctx, addr)
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get wallet balance: %w", err)
	}

	mb, err := gw.StateMarketBalance(ctx, addr, types.EmptyTSK)
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get market balance: %w", err)
	}

	dc, err := gw.StateVerifiedClientStatus(ctx, addr, types.EmptyTSK)
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get verified client status: %w", err)
	}

	id, err := gw.StateLookupID(ctx, addr, types.EmptyTSK)
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get address id: %w", err)
	}

	wi := iface.WalletInfo{
		Addr:                  addr.String(),
		IDAddr:                id.String(),
		Balance:               types.FIL(b).Short(),
		MarketBalance:         types.FIL(mb.Escrow).Short(),
		MarketLocked:          types.FIL(mb.Locked).Short(),
		MarketBalanceDetailed: mb,
	}

	if dc != nil {
		wi.DataCap = types.SizeStr(*dc)
	}

	r.cachedWalletInfo = &wi
	r.lastWalletInfoUpdate = time.Now()

	return wi, nil
}
