package rbdeal

import (
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/lotus/chain/types"
)

const mFil = 1_000_000_000_000_000

var (
	maxVerifPrice float64 = 0

	// 2 mFil/gib/mo is roughly cloud cost currently
	maxPrice float64 = (1 * mFil) / 2 / 60 / 24 / 30.436875

	// piece size range ribs is aiming for
	minPieceSize = 32 << 30
	maxPieceSize = 64 << 30

	dealPublishFinality abi.ChainEpoch = 60

	dealStartTime = abi.ChainEpoch(builtin.EpochsInDay * 4) // 4 days
)

// deal checker
const clientReadDeadline = 10 * time.Second
const clientWriteDeadline = 10 * time.Second

var DealCheckInterval = 10 * time.Second
var ParallelDealChecks = 10

var minDatacap = types.NewInt(192 << 30)

var (
	minimumReplicaCount = 5
	targetReplicaCount  = 10

	repairReplicaThreshold = 3
)

// market wallet management
var walletUpgradeInterval = time.Minute

var minMarketBalance = types.NewInt(100_000_000_000_000_000)    // 100 mFIL
var autoMarketBalance = types.NewInt(1_000_000_000_000_000_000) // 1 FIL

// deal transfers

// todo this definitely needs to be configurable by the user
var minTransferMbps = 8  // at 10 Mbps, a 32 GiB piece takes ~7 hours to transfer
var linkSpeedMbps = 1000 // 1 Gbps

//lint:ignore U1000 To be revisited in the context of S3 offloading.
var maxTransfers = linkSpeedMbps / minTransferMbps * 8 / 10 // 80% for safety margin

// time the sp has to start the first transfer, and for data to not be flowing
// also transfer rate check interval
var transferIdleTimeout = 5 * time.Minute

var maxTransferRetries = 10
