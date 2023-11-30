package smbtst

// NOTE: This file is dual licensed MIT/Apache like the rest of the repo
//  but it pulls in macos-fuse-t/go-smb2 which is AGPL

import (
	"context"
	"fmt"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/mfs"
	format "github.com/ipfs/go-ipld-format"
	"github.com/macos-fuse-t/go-smb2/vfs"
	"golang.org/x/xerrors"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	blockSize = 4092
	volSize   = 400 << 50

	// todo show filecoin metrics in those
	volBlocks = volSize / blockSize
	volBavail = volBlocks * 10 / 9

	// todo cid.contact metrics
	totalFsFiles  = 12_000_000_000_000 * 10
	freeFileNodes = totalFsFiles / 10

	ioSize = 256 << 20
)

var fileTime = time.Date(2020, 10, 14, 0, 0, 0, 0, time.UTC)

type mfsSmbFs struct {
	mr *mfs.Root

	fdlk sync.Mutex
	fds  map[vfs.VfsHandle]*mfsHandle
}

type mfsHandle struct {
	lk sync.Mutex

	path string
}

func (m *mfsSmbFs) getOpenHandle(handle vfs.VfsHandle) (*mfsHandle, func(), error) {
	m.fdlk.Lock()
	hnd, ok := m.fds[handle]
	if !ok {
		m.fdlk.Unlock()
		return nil, nil, xerrors.Errorf("handle %d not found", handle)
	}
	hnd.lk.Lock() // todo this may block m.fdlk for an undesirably long time
	m.fdlk.Unlock()

	return hnd, func() {
		hnd.lk.Unlock()
	}, nil
}

func (m *mfsSmbFs) GetAttr(handle vfs.VfsHandle) (*vfs.Attributes, error) {
	hnd, done, err := m.getOpenHandle(handle)
	if err != nil {
		return nil, err
	}
	defer done()

	nd, err := getNodeFromPath(context.TODO(), m.mr, hnd.path)
	if err != nil {
		return nil, err
	}

	switch n := nd.(type) {
	case *dag.ProtoNode:
		d, err := ft.FSNodeFromBytes(n.Data())
		if err != nil {
			return nil, err
		}

		mode := os.FileMode(0644)
		if d.Type() == ft.TDirectory {
			mode = os.FileMode(0755) | os.ModeDir
		}

		a := vfs.Attributes{}
		a.SetInodeNumber(123) // todo this may be very bad
		a.SetSizeBytes(d.FileSize())
		a.SetDiskSizeBytes(d.FileSize())
		a.SetUnixMode(uint32(mode))
		a.SetPermissions(vfs.NewPermissionsFromMode(uint32(mode)))

		a.SetAccessTime(fileTime) // todo can we not set those 4?
		a.SetLastDataModificationTime(fileTime)
		a.SetBirthTime(fileTime)
		a.SetLastStatusChangeTime(fileTime)

		if d.Type() == ft.TDirectory {
			a.SetFileType(vfs.FileTypeDirectory)
		} else if d.Type() == ft.TSymlink {
			a.SetFileType(vfs.FileTypeSymlink)
		} else {
			a.SetFileType(vfs.FileTypeRegularFile)
		}
		return &a, nil
	case *dag.RawNode:
		a := vfs.Attributes{}
		a.SetInodeNumber(123) // todo this may be very bad
		a.SetSizeBytes(uint64(len(n.RawData())))
		a.SetDiskSizeBytes(uint64(len(n.RawData())))
		a.SetUnixMode(uint32(0644))
		a.SetPermissions(vfs.NewPermissionsFromMode(uint32(0644)))

		a.SetAccessTime(fileTime) // todo can we not set those 4?
		a.SetLastDataModificationTime(fileTime)
		a.SetBirthTime(fileTime)
		a.SetLastStatusChangeTime(fileTime)
		a.SetFileType(vfs.FileTypeRegularFile)
		return &a, nil
	default:
		return nil, fmt.Errorf("not unixfs node (proto or raw)")
	}
}

func (m *mfsSmbFs) SetAttr(handle vfs.VfsHandle, attributes *vfs.Attributes) (*vfs.Attributes, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) StatFS(handle vfs.VfsHandle) (*vfs.FSAttributes, error) {
	a := vfs.FSAttributes{}
	a.SetAvailableBlocks(volBavail)
	a.SetBlockSize(blockSize)
	a.SetBlocks(volBlocks)
	a.SetFiles(totalFsFiles)
	a.SetFreeBlocks(volBavail)
	a.SetFreeFiles(freeFileNodes)
	a.SetIOSize(ioSize)
	return &a, nil
}

func (m *mfsSmbFs) FSync(handle vfs.VfsHandle) error {
	hnd, done, err := m.getOpenHandle(handle)
	if err != nil {
		return err
	}
	defer done()

}

func (m *mfsSmbFs) Flush(handle vfs.VfsHandle) error {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Open(s string, i int, i2 int) (vfs.VfsHandle, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Close(handle vfs.VfsHandle) error {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Lookup(handle vfs.VfsHandle, s string) (*vfs.Attributes, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Mkdir(s string, mode int) (*vfs.Attributes, error) {
	err := mfs.Mkdir(m.mr, s, mfs.MkdirOpts{
		Mkparents: false,
		Flush:     false,
	})
	if err != nil {
		return nil, xerrors.Errorf("mfs mkdir: %w", err)
	}

}

func (m *mfsSmbFs) Read(handle vfs.VfsHandle, bytes []byte, u uint64, i int) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Write(handle vfs.VfsHandle, bytes []byte, u uint64, i int) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) OpenDir(s string) (vfs.VfsHandle, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) ReadDir(handle vfs.VfsHandle, i int, i2 int) ([]vfs.DirInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Readlink(handle vfs.VfsHandle) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Unlink(handle vfs.VfsHandle) error {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Truncate(handle vfs.VfsHandle, u uint64) error {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Rename(handle vfs.VfsHandle, s string, i int) error {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Symlink(handle vfs.VfsHandle, s string, i int) (*vfs.Attributes, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Link(node vfs.VfsNode, node2 vfs.VfsNode, s string) (*vfs.Attributes, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Listxattr(handle vfs.VfsHandle) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Getxattr(handle vfs.VfsHandle, s string, bytes []byte) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Setxattr(handle vfs.VfsHandle, s string, bytes []byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mfsSmbFs) Removexattr(handle vfs.VfsHandle, s string) error {
	//TODO implement me
	panic("implement me")
}

var _ vfs.VFSFileSystem = &mfsSmbFs{}

func getNodeFromPath(ctx context.Context, mr *mfs.Root, p string) (format.Node, error) {
	switch {
	case strings.HasPrefix(p, "/ipfs/"):
		//return api.ResolveNode(ctx, path.New(p))
		panic("todo")
	default:
		fsn, err := mfs.Lookup(mr, p)
		if err != nil {
			return nil, err
		}

		return fsn.GetNode()
	}
}

// todo grep log use of closed file
