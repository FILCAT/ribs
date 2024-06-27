package rbstor

import (
	"context"
	"sync/atomic"

	iface "github.com/atboosty/ribs"
	"github.com/multiformats/go-multihash"
)

type MeteredIndex struct {
	sub iface.Index

	reads, writes int64
}

func (m *MeteredIndex) Close() error {
	return m.sub.Close()
}

func (m *MeteredIndex) EstimateSize(ctx context.Context) (int64, error) {
	return m.sub.EstimateSize(ctx)
}

func (m *MeteredIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func(cidx int, gk iface.GroupKey) (more bool, err error)) error {
	atomic.AddInt64(&m.reads, int64(len(mh)))
	return m.sub.GetGroups(ctx, mh, cb)
}

func (m *MeteredIndex) GetSizes(ctx context.Context, mh []multihash.Multihash, cb func([]int32) error) error {
	atomic.AddInt64(&m.reads, int64(len(mh)))
	return m.sub.GetSizes(ctx, mh, cb)
}

func (m *MeteredIndex) AddGroup(ctx context.Context, mh []multihash.Multihash, sizes []int32, group iface.GroupKey) error {
	atomic.AddInt64(&m.writes, int64(len(mh)))
	return m.sub.AddGroup(ctx, mh, sizes, group)
}

func (m *MeteredIndex) Sync(ctx context.Context) error {
	return m.sub.Sync(ctx)
}

func (m *MeteredIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	atomic.AddInt64(&m.writes, int64(len(mh)))
	return m.sub.DropGroup(ctx, mh, group)
}

func NewMeteredIndex(sub iface.Index) *MeteredIndex {
	return &MeteredIndex{sub: sub}
}

var _ iface.Index = &MeteredIndex{}
