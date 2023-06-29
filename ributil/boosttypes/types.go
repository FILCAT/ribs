package boosttypes

import (
	"context"
	"encoding/json"
	"fmt"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	multiaddrutil "github.com/filecoin-project/go-legs/httpsync/multiaddr"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

//*//go:generate cbor-gen-for --map-encoding StorageAsk DealParams Transfer DealResponse DealStatusRequest DealStatusResponse DealStatus
//*//go:generate go run github.com/golang/mock/mockgen -destination=mock_types/mocks.go -package=mock_types . PieceAdder,CommpCalculator,DealPublisher,ChainDealManager

// StorageAsk defines the parameters by which a miner will choose to accept or
// reject a deal. Note: making a storage deal proposal which matches the miner's
// ask is a precondition, but not sufficient to ensure the deal is accepted (the
// storage provider may run its own decision logic).
type StorageAsk struct {
	// Price per GiB / Epoch
	Price         abi.TokenAmount
	VerifiedPrice abi.TokenAmount

	MinPieceSize abi.PaddedPieceSize
	MaxPieceSize abi.PaddedPieceSize
	Miner        address.Address
}

// DealStatusRequest is sent to get the current state of a deal from a
// storage provider
type DealStatusRequest struct {
	DealUUID  uuid.UUID
	Signature crypto.Signature
}

// DealStatusResponse is the current state of a deal
type DealStatusResponse struct {
	DealUUID uuid.UUID
	// Error is non-empty if there is an error getting the deal status
	// (eg invalid request signature)
	Error          string
	DealStatus     *DealStatus
	IsOffline      bool
	TransferSize   uint64
	NBytesReceived uint64
}

type DealStatus struct {
	// Error is non-empty if the deal is in the error state
	Error string
	// Status is a string corresponding to a deal checkpoint
	Status string
	// SealingStatus is the sealing status reported by lotus miner
	SealingStatus string
	// Proposal is the deal proposal
	Proposal market.DealProposal
	// SignedProposalCid is the cid of the client deal proposal + signature
	SignedProposalCid cid.Cid
	// PublishCid is the cid of the Publish message sent on chain, if the deal
	// has reached the publish stage
	PublishCid *cid.Cid
	// ChainDealID is the id of the deal in chain state
	ChainDealID abi.DealID
}

type DealParams struct {
	DealUUID           uuid.UUID
	IsOffline          bool
	ClientDealProposal market.ClientDealProposal
	DealDataRoot       cid.Cid
	Transfer           Transfer // Transfer params will be the zero value if this is an offline deal
}

type DealFilterParams struct {
	DealParams           *DealParams
	SealingPipelineState *Status
}

type Status struct {
	SectorStates map[api.SectorState]int
	Workers      []*worker
}

type worker struct {
	ID     string
	Start  time.Time
	Stage  string
	Sector int32
}

// Transfer has the parameters for a data transfer
type Transfer struct {
	// The type of transfer eg "http"
	Type string
	// An optional ID that can be supplied by the client to identify the deal
	ClientID string
	// A byte array containing marshalled data specific to the transfer type
	// eg a JSON encoded struct { URL: "<url>", Headers: {...} }
	Params []byte
	// The size of the data transferred in bytes
	Size uint64
}

func (t *Transfer) Host() (string, error) {
	if t.Type != "http" && t.Type != "libp2p" {
		return "", fmt.Errorf("cannot parse params for unrecognized transfer type '%s'", t.Type)
	}

	// de-serialize transport opaque token
	tInfo := &HttpRequest{}
	if err := json.Unmarshal(t.Params, tInfo); err != nil {
		return "", fmt.Errorf("failed to de-serialize transport params bytes '%s': %w", string(t.Params), err)
	}

	// Parse http / multiaddr url
	u, err := ParseUrl(tInfo.URL)
	if err != nil {
		return "", fmt.Errorf("cannot parse url '%s': %w", tInfo.URL, err)
	}

	// If the url is in libp2p format
	if u.Scheme == Libp2pScheme {
		// Get the host from the multiaddr
		mahttp, err := multiaddrutil.ToURL(u.Multiaddr)
		if err != nil {
			return "", err
		}
		return mahttp.Host, nil
	}

	// Otherwise parse as an http url
	httpUrl, err := url.Parse(u.Url)
	if err != nil {
		return "", fmt.Errorf("cannot parse url '%s' from '%s': %w", u.Url, tInfo.URL, err)
	}

	return httpUrl.Host, nil
}

const Libp2pScheme = "libp2p"

type TransportUrl struct {
	Scheme    string
	Url       string
	PeerID    peer.ID
	Multiaddr multiaddr.Multiaddr
}

func ParseUrl(urlStr string) (*TransportUrl, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("parsing url '%s': %w", urlStr, err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("parsing url '%s': could not parse scheme", urlStr)
	}
	if u.Scheme == Libp2pScheme {
		return parseLibp2pUrl(urlStr)
	}
	return &TransportUrl{Scheme: u.Scheme, Url: urlStr}, nil
}

func parseLibp2pUrl(urlStr string) (*TransportUrl, error) {
	// Remove libp2p prefix
	prefix := Libp2pScheme + "://"
	if !strings.HasPrefix(urlStr, prefix) {
		return nil, fmt.Errorf("libp2p URL '%s' must start with prefix '%s'", urlStr, prefix)
	}

	// Convert to AddrInfo
	addrInfo, err := peer.AddrInfoFromString(urlStr[len(prefix):])
	if err != nil {
		return nil, fmt.Errorf("parsing address info from url '%s': %w", urlStr, err)
	}

	// There should be exactly one address
	if len(addrInfo.Addrs) != 1 {
		return nil, fmt.Errorf("expected only one address in url '%s'", urlStr)
	}

	return &TransportUrl{
		Scheme:    Libp2pScheme,
		Url:       Libp2pScheme + "://" + addrInfo.ID.String(),
		PeerID:    addrInfo.ID,
		Multiaddr: addrInfo.Addrs[0],
	}, nil
}

type DealResponse struct {
	Accepted bool
	// Message is the reason the deal proposal was rejected. It is empty if
	// the deal was accepted.
	Message string
}

type PieceAdder interface {
	AddPiece(ctx context.Context, size abi.UnpaddedPieceSize, r io.Reader, d api.PieceDealInfo) (abi.SectorNumber, abi.PaddedPieceSize, error)
}

type CommpCalculator interface {
	ComputeDataCid(ctx context.Context, pieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (abi.PieceInfo, error)
}

type DealPublisher interface {
	Publish(ctx context.Context, deal market.ClientDealProposal) (cid.Cid, error)
}

type ChainDealManager interface {
	WaitForPublishDeals(ctx context.Context, publishCid cid.Cid, proposal market.DealProposal) (*storagemarket.PublishDealsWaitResult, error)
}

type IndexProvider interface {
	Enabled() bool
	AnnounceBoostDeal(ctx context.Context, pds *ProviderDealState) (cid.Cid, error)
	Start(ctx context.Context)
}

type AskGetter interface {
	GetAsk() *storagemarket.SignedStorageAsk
}

type SignatureVerifier interface {
	VerifySignature(ctx context.Context, sig crypto.Signature, addr address.Address, input []byte, encodedTs shared.TipSetToken) (bool, error)
}

type Checkpoint int

const (
	Accepted Checkpoint = iota
	Transferred
	Published
	PublishConfirmed
	AddedPiece
	IndexedAndAnnounced
	Complete
)

var names = map[Checkpoint]string{
	Accepted:            "Accepted",
	Transferred:         "Transferred",
	Published:           "Published",
	PublishConfirmed:    "PublishConfirmed",
	AddedPiece:          "AddedPiece",
	IndexedAndAnnounced: "IndexedAndAnnounced",
	Complete:            "Complete",
}

var strToCP map[string]Checkpoint

func init() {
	strToCP = make(map[string]Checkpoint, len(names))
	for cp, str := range names {
		strToCP[str] = cp
	}
}

func (c Checkpoint) String() string {
	return names[c]
}

// ProviderDealState is the local state tracked for a deal by the StorageProvider.
type ProviderDealState struct {
	// DealUuid is an unique uuid generated by client for the deal.
	DealUuid uuid.UUID
	// CreatedAt is the time at which the deal was stored
	CreatedAt time.Time
	// ClientDealProposal is the deal proposal sent by the client.
	ClientDealProposal market.ClientDealProposal
	// IsOffline is true for offline deals i.e. deals where the actual data to be stored by the SP is sent out of band
	// and not via an online data transfer.
	IsOffline bool

	// ClientPeerID is the Clients libp2p Peer ID.
	ClientPeerID peer.ID

	// DealDataRoot is the root of the IPLD DAG that the client wants to store.
	DealDataRoot cid.Cid

	// InboundCARPath is the file-path where the storage provider will persist the CAR file sent by the client.
	InboundFilePath string

	// Transfer has the parameters for the data transfer
	Transfer Transfer

	// Chain Vars
	ChainDealID abi.DealID
	PublishCID  *cid.Cid

	// sector packing info
	SectorID abi.SectorNumber
	Offset   abi.PaddedPieceSize
	Length   abi.PaddedPieceSize

	// deal checkpoint in DB.
	Checkpoint Checkpoint
	// CheckpointAt is the time at which the deal entered in the last state
	CheckpointAt time.Time

	// set if there's an error
	Err string
	// if there was an error, indicates whether and how to retry (auto / manual)
	Retry DealRetryType

	// NBytesReceived is the number of bytes Received for this deal
	NBytesReceived int64
}

func (d *ProviderDealState) String() string {
	return fmt.Sprintf("%+v", *d)
}

func (d *ProviderDealState) SignedProposalCid() (cid.Cid, error) {
	propnd, err := cborutil.AsIpld(&d.ClientDealProposal)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to compute signed deal proposal ipld node: %w", err)
	}

	return propnd.Cid(), nil
}

type DealRetryType string

const (
	// DealRetryAuto means that when boost restarts, it will automatically
	// retry the deal
	DealRetryAuto DealRetryType = "auto"
	// DealRetryManual means that boost will not automatically retry the
	// deal, it must be manually retried by the user
	DealRetryManual DealRetryType = "manual"
	// DealRetryFatal means that the deal will fail immediately and permanently
	DealRetryFatal DealRetryType = "fatal"
)

///////////

const DataTransferProtocol = "/fil/storage/transfer/1.0.0"

// HttpRequest has parameters for an HTTP transfer
type HttpRequest struct {
	// URL can be
	// - an http URL:
	//   "https://example.com/path"
	// - a libp2p URL:
	//   "libp2p:///ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
	//   Must include a Peer ID
	URL string
	// Headers are the HTTP headers that are sent as part of the request,
	// eg "Authorization"
	Headers map[string]string
}

// TransportDealInfo has parameters for a transfer to be executed
type TransportDealInfo struct {
	OutputFile string
	DealUuid   uuid.UUID
	DealSize   int64
}

// TransportEvent is fired as a transfer progresses
type TransportEvent struct {
	NBytesReceived int64
	Error          error
}

// TransferStatus describes the status of a transfer (started, completed etc)
type TransferStatus string

const (
	// TransferStatusStarted is set when the transfer starts
	TransferStatusStarted TransferStatus = "TransferStatusStarted"
	// TransferStatusStarted is set when the transfer restarts after previously starting
	TransferStatusRestarted TransferStatus = "TransferStatusRestarted"
	TransferStatusOngoing   TransferStatus = "TransferStatusOngoing"
	TransferStatusCompleted TransferStatus = "TransferStatusCompleted"
	TransferStatusFailed    TransferStatus = "TransferStatusFailed"
)

// TransferState describes a transfer's current state
type TransferState struct {
	ID         string
	LocalAddr  string
	RemoteAddr string
	Status     TransferStatus
	Sent       uint64
	Received   uint64
	Message    string
	PayloadCid cid.Cid
}
