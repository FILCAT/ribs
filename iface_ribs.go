package ribs

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
)

type UploadStats struct {
	ActiveRequests int
	UploadBytes    int64
}

type DealMeta struct {
	UUID     string
	Provider int64

	Sealed, Failed, Rejected bool

	StartEpoch, EndEpoch int64

	Status     string
	SealStatus string
	Error      string
	DealID     int64

	BytesRecv int64
	TxSize    int64
	PubCid    string
}

type Wallet interface {
	WalletInfo() (WalletInfo, error)

	MarketAdd(ctx context.Context, amount abi.TokenAmount) (cid.Cid, error)
	MarketWithdraw(ctx context.Context, amount abi.TokenAmount) (cid.Cid, error)

	Withdraw(ctx context.Context, amount abi.TokenAmount, to address.Address) (cid.Cid, error)
}

type WalletInfo struct {
	Addr string

	DataCap string

	Balance       string
	MarketBalance string
	MarketLocked  string
}

type CrawlState struct {
	State string

	At, Reachable, Total int64
	Boost, BBswap, BHttp int64
}

type DealSummary struct {
	NonFailed, InProgress, Done, Failed int64

	TotalDataSize, TotalDealSize   int64
	StoredDataSize, StoredDealSize int64
}

type ProviderInfo struct {
	Meta        ProviderMeta
	RecentDeals []DealMeta
}

type ProviderMeta struct {
	ID     int64
	PingOk bool

	BoostDeals     bool
	BoosterHttp    bool
	BoosterBitswap bool

	IndexedSuccess int64
	IndexedFail    int64

	DealStarted  int64
	DealSuccess  int64
	DealFail     int64
	DealRejected int64

	RetrProbeSuccess int64
	RetrProbeFail    int64
	RetrProbeBlocks  int64
	RetrProbeBytes   int64

	// price in fil/gib/epoch
	AskPrice         float64
	AskVerifiedPrice float64

	AskMinPieceSize float64
	AskMaxPieceSize float64
}
