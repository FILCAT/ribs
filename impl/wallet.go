package impl

import (
	"context"
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
	gw, closer, err := client.NewGatewayRPCV1(ctx, "http://api.chain.love/rpc/v1", nil)
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
