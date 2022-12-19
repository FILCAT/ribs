package impl

import (
	"context"
	"database/sql"
	iface "github.com/magik6k/carsplit/ribs"
	"github.com/magik6k/carsplit/ribs/jbob"
	mh "github.com/multiformats/go-multihash"
	"path/filepath"
	"strconv"
	"sync"

	"golang.org/x/xerrors"
	"os"
)

const (
	// 100MB for now
	// TODO: make this configurable
	maxGroupSize = 100 << 20
)

type Group struct {
	db    *sql.DB
	index iface.Index

	path string
	id   int64

	committedBlocks int64
	committedSize   int64

	inflightBlocks int64
	inflightSize   int64

	writable bool

	lk sync.Mutex

	// // backends
	// jbob

	jblk sync.RWMutex
	jb   *jbob.JBOB
}

func OpenGroup(db *sql.DB, index iface.Index, id int64, path string, writable bool, create bool) (*Group, error) {
	groupPath := filepath.Join(path, "grp", strconv.FormatInt(id, 32))

	if err := os.MkdirAll(groupPath, 0755); err != nil {
		return nil, xerrors.Errorf("create group directory: %w", err)
	}

	// open jbob

	jbOpenFunc := jbob.Open
	if create {
		jbOpenFunc = jbob.Create
	}

	jb, err := jbOpenFunc(filepath.Join(groupPath, "jbidx"), filepath.Join(groupPath, "jbdata"))
	if err != nil {
		return nil, xerrors.Errorf("open jbob: %w", err)
	}

	return &Group{
		db:    db,
		index: index,

		jb: jb,

		path:     groupPath,
		id:       id,
		writable: writable,
	}, nil
}

func (m *Group) Put(ctx context.Context, c []mh.Multihash, datas [][]byte) (int, error) {
	// reserve space
	m.lk.Lock()

	availSpace := maxGroupSize - m.inflightSize

	var writeSize int64
	var writeBlocks int

	for _, data := range datas {
		if int64(len(data))+writeSize > availSpace {
			break
		}
		writeSize += int64(len(data))
		writeBlocks++
	}

	if writeBlocks == 0 {
		m.lk.Unlock()
		return 0, nil
	}

	m.inflightBlocks += int64(writeBlocks)
	m.inflightSize += writeSize

	defer func() {
		m.lk.TryLock()
		m.inflightBlocks -= int64(writeBlocks)
		m.inflightSize -= writeSize
		m.lk.Unlock()
	}()

	m.lk.Unlock()

	// backend write

	m.jblk.Lock()
	err := m.jb.Put(c[:writeBlocks], datas[:writeBlocks])
	m.jblk.Unlock()
	if err != nil {
		m.lk.Lock()
		return 0, xerrors.Errorf("writing to jbob: %w", err)
	}

	// todo async commit
	at, err := m.jb.Commit()
	if err != nil {
		m.lk.Lock()
		return 0, xerrors.Errorf("committing jbob: %w", err)
	}

	// write idx
	err = m.index.AddGroup(ctx, c[:writeBlocks], m.id)
	if err != nil {
		m.lk.Lock()
		return 0, xerrors.Errorf("writing index: %w", err)
	}

	// update head
	m.lk.Lock()
	m.committedBlocks += int64(writeBlocks)
	m.committedSize += writeSize

	if writeBlocks < len(datas) {
		// this group is full
		m.writable = false
	}

	_, err = m.db.ExecContext(ctx, `begin transaction;
		update groups set blocks = ?, bytes = ?, writable = ? where id = ?;
		update g_jbob set recorded_head = ? where "group" = ?;
		commit;`, m.committedBlocks, m.committedSize, m.writable, m.id, at, m.id)
	if err != nil {
		return 0, xerrors.Errorf("update group head: %w", err)
	}

	return writeBlocks, nil
}

func (m *Group) Unlink(ctx context.Context, c []mh.Multihash) error {
	// write log

	// write idx

	// update head

	//TODO implement me
	panic("implement me")
}

func (m *Group) View(ctx context.Context, c []mh.Multihash, cb func(cidx int, data []byte)) error {
	//TODO implement me
	panic("implement me")
}

func (m *Group) Close() error {
	//TODO implement me
	panic("implement me")
}

func (m *Group) Sync(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

var _ iface.Group = &Group{}
