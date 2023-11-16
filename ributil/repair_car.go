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
	"math/bits"
)

type RepairCarLog struct {
	source *bufio.Reader

	expectCidStack [][]cid.Cid

	readBuf []byte

	repairBlock func(cid.Cid) ([]byte, error)
}

func NewCarRepairReader(source io.Reader, root cid.Cid, repair func(cid.Cid) ([]byte, error)) (*RepairCarLog, error) {
	br := bufio.NewReaderSize(source, int(carutil.MaxAllowedSectionSize))

	h, err := car.ReadHeader(br)
	if err != nil {
		return nil, xerrors.Errorf("read car header: %w", err)
	}

	if h.Version != 1 {
		return nil, xerrors.Errorf("unsupported car version: %d", h.Version)
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
		readBuf:     hdrBuf.Bytes(),
		source:      br,
		repairBlock: repair,

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

	// length header read

	maxExpectedCIDLen := 4 // car max extry size is 32MB, so 4 bytes is enough for varint length

	cidLenEnt, err := r.source.Peek(firstCidInLayer.ByteLen() + maxExpectedCIDLen)
	if err != nil {
		return 0, xerrors.Errorf("peek entry: %w", err)
	}
	cidOff, err := match32Bytes(firstCidInLayer.Bytes()[:32], cidLenEnt[1:]) // at least 1 byte for varint length
	if err != nil {
		return 0, xerrors.Errorf("match cid pos: %w", err)
	}

	varintLen := cidOff + 1

	if _, err := r.source.Discard(varintLen); err != nil {
		return 0, xerrors.Errorf("discard varint len bytes: %w", err)
	}

	// vEntLen contains the length claimed by the varint. It will, most of the time
	// be ok, but sometimes it may contain bitflips, so don't always trust it
	var ent []byte
	vEntLen, n := binary.Uvarint(cidLenEnt[:varintLen])
	if n <= 0 || vEntLen > uint64(carutil.MaxAllowedSectionSize) {
		// varint len is probably corrupted
		log.Errorw("bad varint or header is bigger than util.MaxAllowedSectionSize, varint len is probably corrupted, will try repair", "expected", firstCidInLayer, "actual", vEntLen)

		goodData, err := r.repairBlock(firstCidInLayer)
		if err != nil {
			return 0, xerrors.Errorf("repair block %s: %w", firstCidInLayer, err)
		}

		// make ent the correct length
		ent = make([]byte, len(firstCidInLayer.Bytes())+len(goodData))

		// now reconstruct correct entry for next steps
		copy(ent[:firstCidInLayer.ByteLen()], firstCidInLayer.Bytes())
		copy(ent[firstCidInLayer.ByteLen():], goodData)
	}

	if len(ent) == 0 {
		// wasn't repaired above, so just read from source stream
		ent, err = r.source.Peek(int(vEntLen))
		if err != nil {
			if err == io.EOF {
				// length was probably corrupted
				log.Errorw("read entry eof, varint len is probably corrupted, will try repair", "expected", firstCidInLayer, "actual", vEntLen)

				goodData, err := r.repairBlock(firstCidInLayer)
				if err != nil {
					return 0, xerrors.Errorf("repair block %s: %w", firstCidInLayer, err)
				}

				// make ent the correct length
				ent = make([]byte, len(firstCidInLayer.Bytes())+len(goodData))

				// now reconstruct correct entry for next steps
				copy(ent[:firstCidInLayer.ByteLen()], firstCidInLayer.Bytes())
				copy(ent[firstCidInLayer.ByteLen():], goodData)
			} else {
				return 0, xerrors.Errorf("peek entry: %w", err)
			}

		}
	}

	if len(ent) < len(expCidBytes) {
		log.Errorw("entry shorter than cid, will attempt repair", "expected", firstCidInLayer, "actual", ent)
		goodData, err := r.repairBlock(firstCidInLayer)
		if err != nil {
			return 0, xerrors.Errorf("repair block %s: %w", firstCidInLayer, err)
		}

		// make ent the correct length
		ent = make([]byte, len(firstCidInLayer.Bytes())+len(goodData))

		// now reconstruct correct entry for next steps
		copy(ent[:firstCidInLayer.ByteLen()], firstCidInLayer.Bytes())
		copy(ent[firstCidInLayer.ByteLen():], goodData)
	}

	if !bytes.Equal(ent[:len(expCidBytes)], expCidBytes) {
		log.Errorw("cid mismatch in car stream, will attempt repair", "expected", firstCidInLayer, "actual", ent[:len(expCidBytes)])

		// repair here is really just copying the right cid into the entry
		copy(ent[:len(expCidBytes)], expCidBytes)
	}

	hash, err := firstCidInLayer.Prefix().Sum(ent[len(expCidBytes):])
	if err != nil {
		return 0, xerrors.Errorf("hash data: %w", err)
	}
	if !hash.Equals(firstCidInLayer) {
		log.Errorw("data hash mismatch in car stream, will attempt repair", "expected", firstCidInLayer, "actual", hash)

		// block data repair
		goodData, err := r.repairBlock(firstCidInLayer)
		if err != nil {
			return 0, xerrors.Errorf("repair block %s: %w", firstCidInLayer, err)
		}

		if len(goodData) != len(ent[len(expCidBytes):]) {
			// resize ent to the correct length
			ent = make([]byte, len(expCidBytes)+len(goodData))
			// copy in cid bytes again..
			copy(ent[:len(expCidBytes)], expCidBytes)
		}

		copy(ent[len(expCidBytes):], goodData)
		// note: it won't be that easy when we'll want to save allocations here
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
		if err := cbor.DecodeInto(ent[len(expCidBytes):], &links); err != nil {
			return 0, xerrors.Errorf("decoding layer links: %w", err)
		}

		// push the layer
		r.expectCidStack = append(r.expectCidStack, links)
	}

	// advance the source by the correct amount
	if _, err := r.source.Discard(len(ent)); err != nil {
		return 0, xerrors.Errorf("discard entry: %w", err)
	}

	// now perform the real read
	n = copy(p, r.readBuf)
	r.readBuf = r.readBuf[n:]
	return
}

// finds pattern in buf
func match32Bytes(pattern []byte, buf []byte) (off int, err error) {
	// data might be corrupted, so we can't use bytes.Index
	// we count matching bits at offsets 0,1,2,3, and select highest overlap

	if len(pattern) != 32 {
		return 0, xerrors.Errorf("pattern must be 32 bytes")
	}
	if len(buf) < 4+32 {
		return 0, xerrors.Errorf("buf must be at least 36 bytes")
	}

	var maxOverlap int
	var maxOverlapOff int

	for i := 0; i < 4; i++ {
		overlap := b32overlap(pattern, buf[i:])
		if overlap == 32*8 {
			return i, nil
		}
		if overlap > maxOverlap {
			maxOverlap = overlap
			maxOverlapOff = i
		}
	}

	return maxOverlapOff, nil
}

func b32overlap(patt, b []byte) (overlap int) {
	var matchingBits int

	for i, pb := range patt {
		matchingBits += 8 - bits.OnesCount8(pb^b[i])
	}

	return
}

var _ io.Reader = (*RepairCarLog)(nil)
