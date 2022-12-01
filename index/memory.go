package index

import (
	"context"
	"sync"

	"github.com/multiformats/go-multihash"

	"github.com/magik6k/carsplit/ribs"
)

type IndexEntry struct {
	Groups []int64
	Set    map[int64]int // GroupID -> Groups idx
}

type MemIndex struct {
	// key: string(multihash)
	Groups map[string]*IndexEntry

	lk sync.Mutex
}

func (m *MemIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group int64) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	for _, h := range mh {
		ent, found := m.Groups[string(h)]
		if found {
			listIdx, has := ent.Set[group]
			if has {
				switch {
				case len(ent.Groups) == 1: // only entry
					// drop mh mapping
					delete(m.Groups, string(h))
				case listIdx == len(ent.Groups)-1: // last entry
					// drop last entry
					ent.Groups = ent.Groups[:len(ent.Groups)-1]
					delete(ent.Set, group)
				default:
					// swap last entry into this one, trunc groups
					lastGroup := ent.Groups[len(ent.Groups)-1]
					ent.Groups[listIdx] = lastGroup
					ent.Set[lastGroup] = listIdx
					ent.Groups = ent.Groups[:len(ent.Groups)-1]
					delete(ent.Set, group)
				}
			}
		}
	}

	return nil
}

func (m *MemIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func([][]int64) (bool, error)) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	out := make([][]int64, len(mh))
	for i, h := range mh {
		out[i] = m.Groups[string(h)].Groups
	}

	_, err := cb(out)
	return err
}

func (m *MemIndex) AddGroup(ctx context.Context, mh []multihash.Multihash, group int64) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	for _, h := range mh {
		ent, found := m.Groups[string(h)]
		if !found {
			ent = &IndexEntry{}
			m.Groups[string(h)] = ent
		}
		_, has := ent.Set[group]
		if !has {
			ent.Set[group] = len(ent.Groups)
			ent.Groups = append(ent.Groups, group)
		}
	}

	return nil
}

var _ ribs.Index = &MemIndex{}
