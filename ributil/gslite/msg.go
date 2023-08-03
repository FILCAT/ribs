package gslite

import (
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type RequestType string

const (
	// RequestTypeNew means a new request
	RequestTypeNew = RequestType("New")

	// RequestTypeCancel means cancel the request referenced by request ID
	RequestTypeCancel = RequestType("Cancel")

	// RequestTypeUpdate means the extensions contain an update about this request
	RequestTypeUpdate = RequestType("Update")
)

type GraphSyncRequest struct {
	Id          []byte        `cborgen:"id"`
	RequestType RequestType   `cborgen:"type"`
	Priority    int64         `cborgen:"pri"` // graphsync.Priority = 1
	Root        *cid.Cid      `cborgen:"root"`
	Selector    *cbg.Deferred `cborgen:"sel"`
	//Extensions  *GraphSyncExtensions  `cborgen:"ext"`
}

type GraphSyncLinkMetadatum struct {
	Link   cid.Cid `cborgen:"link"`
	Action string  `cborgen:"action"` // graphsync.LinkAction
}

type GraphSyncResponse struct {
	Id       []byte                   `cborgen:"reqid"`
	Status   ResponseStatusCode       `cborgen:"stat"`
	Metadata []GraphSyncLinkMetadatum `cborgen:"meta"`
	//Extensions *GraphSyncExtensions  `cborgen:"ext"`
}

type GraphSyncBlock struct {
	Prefix []byte `cborgen:"prefix"`
	Data   []byte `cborgen:"data"`
}

type GraphSyncMessage struct {
	Requests  []GraphSyncRequest  `cborgen:"req"`
	Responses []GraphSyncResponse `cborgen:"rsp"`
	Blocks    []GraphSyncBlock    `cborgen:"blk"`
}

type GraphSyncMessageRoot struct {
	Gs2 *GraphSyncMessage `cborgen:"gs2"`
}
