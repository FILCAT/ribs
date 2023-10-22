package ribs

import (
	"context"
	"io"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/ipfs/go-cid"
)

type RIBS interface {
	RBS

	Wallet() Wallet
	DealDiag() RIBSDiag

	io.Closer
}

type RIBSDiag interface {
	CarUploadStats() UploadStats
	DealSummary() (DealSummary, error)
	GroupDeals(gk GroupKey) ([]DealMeta, error)

	ProviderInfo(id int64) (ProviderInfo, error)
	CrawlState() CrawlState
	ReachableProviders() []ProviderMeta

	RetrStats() (RetrStats, error)

	StagingStats() (StagingStats, error)

	Filecoin(context.Context) (api.Gateway, jsonrpc.ClientCloser, error)

	P2PNodes(ctx context.Context) (map[string]Libp2pInfo, error)

	RetrChecker() RetrCheckerStats

	RetrievableDealCounts() ([]DealCountStats, error)
	SealedDealCounts() ([]DealCountStats, error)

	RepairStats() (map[int]RepairJob, error)
}

type RepairJob struct {
	GroupKey GroupKey

	State RepairJobState

	FetchProgress, FetchSize int64
	FetchUrl                 string
}

type RepairJobState string

const (
	RepairJobStateFetching   RepairJobState = "fetching"
	RepairJobStateVerifying  RepairJobState = "verifying"
	RepairJobStateIndexing   RepairJobState = "indexing"
	RepairJobStateFinalizing RepairJobState = "finalizing"
)

type DealCountStats struct {
	Count  int
	Groups int
}

type RetrCheckerStats struct {
	ToDo       int64
	Started    int64
	Success    int64
	Fail       int64
	SuccessAll int64
	FailAll    int64
}

type Libp2pInfo struct {
	PeerID string

	Listen []string

	Peers int
}

type StagingStats struct {
	UploadBytes, UploadStarted, UploadDone, UploadErr, Redirects, ReadReqs, ReadBytes int64
}

type RetrStats struct {
	Success, Bytes, Fail, CacheHit, CacheMiss, Active int64
	HTTPTries, HTTPSuccess, HTTPBytes                 int64
}

type UploadStats struct {
	ByGroup map[GroupKey]*GroupUploadStats

	LastTotalBytes int64
}

type GroupUploadStats struct {
	ActiveRequests int
	UploadBytes    int64
}

type DealMeta struct {
	UUID     string
	Provider int64

	Sealed, Failed, Rejected bool

	StartEpoch, EndEpoch, StartTime int64

	Status     string
	SealStatus string
	Error      string
	DealID     int64

	BytesRecv int64
	TxSize    int64
	PubCid    string

	RetrTTFBMs            int64
	RetrSuccess, RetrFail int64
}

type Wallet interface {
	WalletInfo() (WalletInfo, error)

	MarketAdd(ctx context.Context, amount abi.TokenAmount) (cid.Cid, error)
	MarketWithdraw(ctx context.Context, amount abi.TokenAmount) (cid.Cid, error)

	Withdraw(ctx context.Context, amount abi.TokenAmount, to address.Address) (cid.Cid, error)
}

type WalletInfo struct {
	Addr, IDAddr string

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

	MostRecentDealStart int64

	// price in fil/gib/epoch
	AskPrice         float64
	AskVerifiedPrice float64

	AskMinPieceSize float64
	AskMaxPieceSize float64

	RetrievDeals, UnretrievDeals int64
}
