package ributil

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"golang.org/x/xerrors"
	"io"
)

type RepairCarLog struct {
	source *bufio.Reader

	expectCidStack [][]cid.Cid

	readBuf []byte
}

func NewCarRepairReader(source io.Reader, root cid.Cid) (*RepairCarLog, error) {
	br := bufio.NewReader(source)

	h, err := car.ReadHeader(br)
	if err != nil {
		return nil, xerrors.Errorf("read car header: %w", err)
	}

	if len(h.Roots) != 1 {
		return nil, xerrors.Errorf("expected 1 root, got %d", len(h.Roots))
	}

	if h.Roots[0] != root {
		return nil, xerrors.Errorf("root cid mismatch: %s != %s", h.Roots[0], root)
	}

	var hdrBuf bytes.Buffer
	err = car.WriteHeader(h, &hdrBuf)
	if err != nil {
		return nil, xerrors.Errorf("write car header: %w", err)
	}

	return &RepairCarLog{
		readBuf: hdrBuf.Bytes(),

		source: br,
		expectCidStack: [][]cid.Cid{
			{root},
		},
	}, nil
}

func (r *RepairCarLog) Read(p []byte) (n int, err error) {
	if len(r.readBuf) > 0 {
		n = copy(p, r.readBuf)
		r.readBuf = r.readBuf[n:]
		return
	}

	if len(r.expectCidStack) == 0 {
		return 0, io.EOF
	}

	// read next expected cid and read the entry
	topStackLayer := r.expectCidStack[len(r.expectCidStack)-1]
	firstCidInLayer := topStackLayer[0]

	// pop the entry
	if len(topStackLayer) == 1 {
		// last entry in layer, pop the layer
		r.expectCidStack = r.expectCidStack[:len(r.expectCidStack)-1]
	} else {
		// pop the entry
		r.expectCidStack[len(r.expectCidStack)-1] = topStackLayer[1:]
	}

	expCidBytes := firstCidInLayer.Bytes()
	ent, err := carutil.LdRead(r.source)
	if err != nil {
		return 0, xerrors.Errorf("read entry: %w", err)
	}

	if len(ent) < len(expCidBytes) {
		return 0, xerrors.Errorf("read expected cid: short read")
	}

	if !bytes.Equal(ent[:len(expCidBytes)], expCidBytes) {
		// todo repair
		return 0, xerrors.Errorf("expected cid mismatch %x != %x", ent[:len(expCidBytes)], expCidBytes)
	}

	data := ent[len(expCidBytes):]

	hash, err := firstCidInLayer.Prefix().Sum(data)
	if err != nil {
		return 0, xerrors.Errorf("hash data: %w", err)
	}
	if !hash.Equals(firstCidInLayer) {
		// todo repair
		return 0, xerrors.Errorf("data hash mismatch %s != %s", hash, firstCidInLayer)
	}

	// here the data is ok, put the whole entry into the read buffer
	// todo pool, ideally reuse ent buffer
	r.readBuf = make([]byte, len(ent)+binary.MaxVarintLen64)
	vn := binary.PutUvarint(r.readBuf, uint64(len(ent)))
	copy(r.readBuf[vn:], ent)
	r.readBuf = r.readBuf[:len(ent)+vn]

	// parse links
	if firstCidInLayer.Prefix().Codec == cid.DagCBOR {
		var links []cid.Cid
		// todo cbor-gen
		if err := cbor.DecodeInto(data, &links); err != nil {
			return 0, xerrors.Errorf("decoding layer links: %w", err)
		}

		// push the layer
		r.expectCidStack = append(r.expectCidStack, links)
	}

	// now perform the real read
	n = copy(p, r.readBuf)
	r.readBuf = r.readBuf[n:]
	return
}

var _ io.Reader = (*RepairCarLog)(nil)
