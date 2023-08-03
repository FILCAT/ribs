package gslite

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
	"io"
	"time"
)

var log = logging.Logger("gslite")

// {".": {}}
var selectOne = cbg.Deferred{
	Raw: []byte{0xA1, 0x61, 0x2E, 0xA0},
}

var (
	GS2Proto protocol.ID = "/ipfs/graphsync/2.0.0"
)

type GSClient struct {
	h host.Host
}

func New(h host.Host) *GSClient {
	c := &GSClient{
		h: h,
	}

	h.SetStreamHandler(GS2Proto, c.handleStream)

	return c
}

func (lc *GSClient) handleStream(st network.Stream) {
	// todo read deadline?

	for {
		reader := msgio.NewVarintReaderSize(st, network.MessageSizeMax)
		var gmsg GraphSyncMessageRoot

		mb, err := reader.ReadMsg()
		if err != nil {
			if err == io.EOF {
				if err := st.Close(); err != nil {
					// todo want this log?
					log.Errorw("close stream on eof", "error", err)
					return
				}
				return
			}

			log.Errorw("stream error", "error", err)

			if err := st.Reset(); err != nil {
				// todo want this log??
				log.Errorw("stream reset err", "error", err)
				return
			}
			return
		}

		if err := gmsg.UnmarshalCBOR(bytes.NewReader(mb)); err != nil {
			log.Errorw("unmarshal message", "error", err)
			_ = st.Reset()
			return
		}

		reader.ReleaseMsg(mb)

		if err := lc.handleMessage(&gmsg); err != nil {
			log.Errorw("handle message", "error", err)
			_ = st.Reset()
			return
		}
	}

}

func (lc *GSClient) handleMessage(gm *GraphSyncMessageRoot) error {
	if gm.Gs2 == nil {
		return xerrors.Errorf("v2 entry is nil")
	}

	if len(gm.Gs2.Requests) > 0 {
		return xerrors.Errorf("unexpected request")
	}

	jb, err := json.Marshal(gm)
	if err != nil {
		return err
	}

	log.Error(string(jb))
	return nil
}

func (lc *GSClient) RequestBlock(ctx context.Context, from peer.ID, c cid.Cid) (blocks.Block, error) {
	reqID := uuid.New()

	req := GraphSyncMessageRoot{
		Gs2: &GraphSyncMessage{
			Requests: []GraphSyncRequest{
				{
					Id:          reqID[:],
					RequestType: RequestTypeNew,
					Priority:    1,
					Root:        &c,
					Selector:    &selectOne,
				},
			},
			Responses: nil,
			Blocks:    nil,
		},
	}

	var lenBuf [binary.MaxVarintLen64]byte

	var rb bytes.Buffer // todo pool
	rb.Write(lenBuf[:])

	if err := req.MarshalCBOR(&rb); err != nil {
		return nil, xerrors.Errorf("marshaling request: %w", err)
	}

	st, err := lc.h.NewStream(ctx, from, GS2Proto)
	if err != nil {
		return nil, xerrors.Errorf("make stream: %w", err)
	}

	if err := st.SetWriteDeadline(time.Now().Add(time.Second * 30)); err != nil {
		return nil, xerrors.Errorf("set write deadline: %w", err)
	}

	lbuflen := binary.PutUvarint(lenBuf[:], uint64(rb.Len()-binary.MaxVarintLen64))
	out := rb.Bytes()
	copy(out[binary.MaxVarintLen64-lbuflen:], lenBuf[:lbuflen])
	_, err = st.Write(out[binary.MaxVarintLen64-lbuflen:])

	if err := st.CloseWrite(); err != nil {
		return nil, xerrors.Errorf("close write: %w", err)
	}

	for {
		var b [2000]byte
		n, err := st.Read(b[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, xerrors.Errorf("streamread: %w", err)
		}

		fmt.Printf("qdata %d: %x\n", n, b[:n])
	}

	fmt.Println("stream done")

	// wait
	select {}

	return nil, nil
}
