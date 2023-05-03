package rbdeal

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	"time"
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
)

// deal checker
const clientReadDeadline = 10 * time.Second
const clientWriteDeadline = 10 * time.Second

var DealCheckInterval = 10 * time.Second
var ParallelDealChecks = 10

// deal targets
var verified = true

var (
	minimumReplicaCount = 5
	targetReplicaCount  = 7
)

// market wallet management
var walletUpgradeInterval = time.Minute

var minMarketBalance = types.NewInt(100_000_000_000_000_000)    // 100 mFIL
var autoMarketBalance = types.NewInt(1_000_000_000_000_000_000) // 1 FIL

// deal transfers

// todo this definitely needs to be configurable by the user
var minTransferMbps = 20                                    // at 20 Mbps, a 32 GiB piece takes ~3 hours to transfer
var linkSpeedMbps = 1000                                    // 1 Gbps
var maxTransfers = linkSpeedMbps / minTransferMbps * 8 / 10 // 80% for safety margin

// time the sp has to start the first transfer, and for data to not be flowing
// also transfer rate check interval
var transferIdleTimeout = 5 * time.Minute

var maxTransferRetries = 10
