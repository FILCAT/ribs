package carlog

import (
	"github.com/atboosty/ribs/bsst"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

type BSSTIndex struct {
	bsi *bsst.BSST
}

func (b *BSSTIndex) Has(c []mh.Multihash) ([]bool, error) {
	return b.bsi.Has(c)
}

func (b *BSSTIndex) Get(c []mh.Multihash) ([]int64, error) {
	return b.bsi.Get(c)
}

func (b *BSSTIndex) Close() error {
	return b.bsi.Close()
}

func (b *BSSTIndex) Entries() (int64, error) {
	panic("not supported")
}

func (b *BSSTIndex) List(f func(c mh.Multihash, offs []int64) error) error {
	panic("not supported")
}

func OpenBSSTIndex(path string) (*BSSTIndex, error) {
	bss, err := bsst.Open(path)
	if err != nil {
		return nil, err
	}
	return &BSSTIndex{
		bsi: bss,
	}, nil
}

type IndexSource interface {
	bsst.Source
	Entries() (int64, error)
}

func CreateBSSTIndex(path string, index IndexSource) (*BSSTIndex, error) {
	ents, err := index.Entries()
	if err != nil {
		return nil, xerrors.Errorf("getting level index entries: %w", err)
	}
	bss, err := bsst.Create(path, ents, index)
	if err != nil {
		return nil, xerrors.Errorf("bsst create: %w", err)
	}
	return &BSSTIndex{
		bsi: bss,
	}, nil
}

var _ ReadableIndex = (*BSSTIndex)(nil)
