package smbtst

// NOTE: This file is dual licensed MIT/Apache like the rest of the repo
//  but it pulls in macos-fuse-t/go-smb2 which is AGPL

import (
	"context"
	"errors"
	"fmt"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/mfs"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	smb2 "github.com/macos-fuse-t/go-smb2/server"
	"github.com/macos-fuse-t/go-smb2/vfs"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"io"
	"os"
	gopath "path"
	"strings"
	"sync"
	"time"
)

var log = logging.Logger("mfssmb")

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

var v1CidPrefix = cid.Prefix{
	Codec:    cid.DagProtobuf,
	MhLength: -1,
	MhType:   mh.SHA2_256,
	Version:  1,
}

var fileTime = time.Date(2020, 10, 14, 0, 0, 0, 0, time.UTC)

type MfsSmbFs struct {
	mr *mfs.Root

	fdlk  sync.Mutex
	fdctr vfs.VfsHandle
	fds   map[vfs.VfsHandle]*mfsHandle
}

func NewMfsSmbFs(mr *mfs.Root) (*MfsSmbFs, error) {
	return &MfsSmbFs{
		mr:  mr,
		fds: make(map[vfs.VfsHandle]*mfsHandle),
	}, nil
}

func StartMfsSmbFs(lc fx.Lifecycle, fs *MfsSmbFs) error {
	hostname, err := os.Hostname()
	if err != nil {
		return xerrors.Errorf("getting hostname: %w", err)
	}

	srv := smb2.NewServer(
		&smb2.ServerConfig{
			AllowGuest:  true,
			MaxIOReads:  1024,
			MaxIOWrites: 1024,
			Xatrrs:      false,
		},
		&smb2.NTLMAuthenticator{
			TargetSPN:    "",
			NbDomain:     hostname,
			NbName:       hostname,
			DnsName:      hostname + ".local",
			DnsDomain:    ".local",
			UserPassword: map[string]string{"a": "a"},
			AllowGuest:   true,
		},
		map[string]vfs.VFSFileSystem{"filecoin": fs},
	)

	listen := ":4455"
	log.Errorf("Starting SMB server at %s", listen)
	go srv.Serve(listen)

	return nil
}

type mfsHandle struct {
	lk sync.Mutex

	mr   *mfs.Root
	mfdf mfs.FileDescriptor // if file
	mfdi *mfs.Directory     // if dir

	dirPos int

	path string
}

func (m *MfsSmbFs) allocHandle(fh *mfsHandle) vfs.VfsHandle {
	m.fdlk.Lock()
	defer m.fdlk.Unlock()

	m.fdctr++
	out := m.fdctr

	m.fds[out] = fh
	return out
}

func (m *MfsSmbFs) getOpenHandle(handle vfs.VfsHandle) (*mfsHandle, func(), error) {
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

func (m *MfsSmbFs) GetAttr(handle vfs.VfsHandle) (*vfs.Attributes, error) {
	log.Errorw("GET ATTR", "handle", handle)

	hnd, done, err := m.getOpenHandle(handle)
	if err != nil {
		return nil, err
	}
	defer done()

	nd, err := getNodeFromPath(context.TODO(), m.mr, hnd.path)
	if err != nil {
		return nil, err
	}

	return m.attrForNode(nd)
}

func (m *MfsSmbFs) attrForNode(nd format.Node) (*vfs.Attributes, error) {
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

func (m *MfsSmbFs) SetAttr(handle vfs.VfsHandle, attributes *vfs.Attributes) (*vfs.Attributes, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) StatFS(handle vfs.VfsHandle) (*vfs.FSAttributes, error) {
	log.Errorw("STAT FS", "handle", handle)

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

func (m *MfsSmbFs) FSync(handle vfs.VfsHandle) error {
	log.Errorw("FSYNC", "handle", handle)
	/*hnd, done, err := m.getOpenHandle(handle)
	if err != nil {
		return err
	}
	defer done()

	*/

	panic("impl")
}

func (m *MfsSmbFs) Flush(handle vfs.VfsHandle) error {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Open(name string, flag int, perm int) (vfs.VfsHandle, error) {
	return m.open(name, flag, perm, false) // todo allow dir??
}

func (m *MfsSmbFs) open(name string, flag int, perm int, mustDir bool) (h vfs.VfsHandle, err error) {
	log.Errorw("OPEN FILE", "name", name, "flag", flag, "perm", perm, "mustDir", mustDir)

	defer func() {
		log.Errorw("OPEN FILE DONE", "name", name, "flag", flag, "perm", perm, "h", h, "err", err)
	}()

	path, err := checkPath(name)
	if err != nil {
		return 0, err
	}

	create := flag&os.O_CREATE != 0
	truncate := flag&os.O_TRUNC != 0
	exclusive := flag&os.O_EXCL != 0
	write := flag&os.O_WRONLY != 0 || flag&os.O_RDWR != 0
	read := flag&os.O_RDWR != 0
	app := flag&os.O_APPEND != 0
	flush := flag&os.O_SYNC != 0

	if flag == os.O_RDONLY { // read only (O_RDONLY is 0x0)
		read = true
	}

	_ = exclusive

	var fi *mfs.File

	target, err := mfs.Lookup(m.mr, path)
	switch err {
	case nil:
		var ok bool
		fi, ok = target.(*mfs.File)
		if !ok {
			_, isdir := target.(*mfs.Directory)
			if !isdir {
				return 0, xerrors.Errorf("file '%s' is neither a file or a directory", path)
			}
			if !mustDir {
				return 0, xerrors.Errorf("can open a dir as a file")
			}
			return m.allocHandle(&mfsHandle{
				mr:   m.mr,
				mfdf: nil,
				mfdi: target.(*mfs.Directory),
				path: path,
			}), nil
		}

	case os.ErrNotExist:
		if !create || mustDir {
			return 0, err
		}

		// if create is specified and the file doesn't exist, we create the file
		dirname, fname := gopath.Split(path)
		pdir, err := getParentDir(m.mr, dirname)
		if err != nil {
			return 0, err
		}

		nd := dag.NodeWithData(ft.FilePBData(nil, 0))
		nd.SetCidBuilder(v1CidPrefix)
		err = pdir.AddChild(fname, nd)
		if err != nil {
			return 0, err
		}

		fsn, err := pdir.Child(fname)
		if err != nil {
			return 0, err
		}

		var ok bool
		fi, ok = fsn.(*mfs.File)
		if !ok {
			return 0, xerrors.New("expected *mfs.File, didn't get it. This is likely a race condition")
		}
	default:
		return 0, err
	}

	fi.RawLeaves = true

	fd, err := fi.Open(mfs.Flags{Read: read, Write: write, Sync: flush})
	if err != nil {
		return 0, xerrors.Errorf("mfsfile open (%t %t %t, %x): %w", read, write, flush, flag, err)
	}

	if truncate {
		if err := fd.Truncate(0); err != nil {
			return 0, xerrors.Errorf("truncate: %w", err)
		}
	}

	seek := io.SeekStart
	if app {
		seek = io.SeekEnd
	}

	_, err = fd.Seek(0, seek)
	if err != nil {
		return 0, xerrors.Errorf("mfs open seek: %w", err)
	}

	return m.allocHandle(&mfsHandle{
		mr: m.mr,

		mfdf: fd,
		mfdi: nil,

		path: path,
	}), nil
}

func getParentDir(root *mfs.Root, dir string) (*mfs.Directory, error) {
	parent, err := mfs.Lookup(root, dir)
	if err != nil {
		return nil, err
	}

	pdir, ok := parent.(*mfs.Directory)
	if !ok {
		return nil, xerrors.New("expected *mfs.Directory, didn't get it. This is likely a race condition")
	}
	return pdir, nil
}

func (m *MfsSmbFs) Close(handle vfs.VfsHandle) error {
	log.Errorw("CLOSE", "handle", handle)

	hnd, done, err := m.getOpenHandle(handle)
	if err != nil {
		return err
	}
	defer done()

	if hnd.mfdf != nil {
		return hnd.mfdf.Close()
	}
	if hnd.mfdi != nil {
		return hnd.mfdi.Flush()
	}

	return nil
}

func (m *MfsSmbFs) Lookup(handle vfs.VfsHandle, subPath string) (*vfs.Attributes, error) {
	log.Errorw("LOOKUP", "handle", handle, "subPath", subPath)

	p := "/"

	if handle != 0 {
		hnd, done, err := m.getOpenHandle(handle)
		if err != nil {
			return nil, err
		}
		defer done()

		p = hnd.path
	}

	p = gopath.Join(p, subPath)

	nd, err := getNodeFromPath(context.TODO(), m.mr, p)
	if err != nil {
		return nil, err
	}

	return m.attrForNode(nd)
}

func (m *MfsSmbFs) Mkdir(s string, mode int) (*vfs.Attributes, error) {
	log.Errorw("MKDIR", "s", s, "mode", mode)

	err := mfs.Mkdir(m.mr, s, mfs.MkdirOpts{
		Mkparents: false,
		Flush:     true,
	})
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (m *MfsSmbFs) Read(handle vfs.VfsHandle, bytes []byte, u uint64, i int) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Write(handle vfs.VfsHandle, bytes []byte, u uint64, i int) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) OpenDir(name string) (vfs.VfsHandle, error) {
	return m.open(name, 0, 0, true)
}

var errHalt = errors.New("halt readdir")

func (m *MfsSmbFs) ReadDir(handle vfs.VfsHandle, pos int, maxEntries int) ([]vfs.DirInfo, error) {
	log.Errorw("READ DIR", "handle", handle, "pos", pos, "maxEntries", maxEntries)

	if pos != 0 {
		panic("todo")
	}

	hnd, done, err := m.getOpenHandle(handle)
	if err != nil {
		return nil, err
	}
	defer done()

	if hnd.mfdi == nil {
		return nil, xerrors.Errorf("not a directory")
	}

	var out []vfs.DirInfo

	if maxEntries != 0 && maxEntries < 2 {
		return nil, xerrors.Errorf("maxEntries must be 0 or >= 2")
	}

	if hnd.dirPos == 0 {
		dnd, err := hnd.mfdi.GetNode()
		if err != nil {
			return nil, xerrors.Errorf("get dir attr: %w", err)
		}

		rootAttr, err := m.attrForNode(dnd)
		if err != nil {
			return nil, xerrors.Errorf("get dir attr: %w", err)
		}

		out = append(out,
			vfs.DirInfo{
				Name:       ".",
				Attributes: *rootAttr,
			},
			vfs.DirInfo{
				Name:       "..",
				Attributes: *rootAttr,
			})

		if maxEntries > 0 {
			maxEntries -= 2
		}

		hnd.dirPos = 2
	}

	seekPos := hnd.dirPos - 2

	// todo start iter in goroutine for better seq reads
	err = hnd.mfdi.ForEachEntry(context.TODO(), func(nl mfs.NodeListing) error {
		if seekPos > 0 {
			seekPos--
			return nil
		}

		if maxEntries == 0 {
			return errHalt
		}

		if maxEntries > 0 {
			maxEntries--
		}

		var a vfs.DirInfo
		a.Name = nl.Name

		mode := os.FileMode(0644)
		if nl.Type == int(mfs.TDir) {
			mode = os.FileMode(0755) | os.ModeDir
		}

		a.SetInodeNumber(123) // todo this may be very bad)
		a.SetSizeBytes(uint64(nl.Size))
		a.SetDiskSizeBytes(uint64(nl.Size))
		a.SetUnixMode(uint32(mode))
		a.SetPermissions(vfs.NewPermissionsFromMode(uint32(mode)))
		a.SetAccessTime(fileTime)
		a.SetLastDataModificationTime(fileTime)
		a.SetBirthTime(fileTime)
		a.SetLastStatusChangeTime(fileTime)

		if nl.Type == int(mfs.TDir) {
			a.SetFileType(vfs.FileTypeDirectory)
		} else {
			a.SetFileType(vfs.FileTypeRegularFile)
		}

		hnd.dirPos++
		out = append(out, a)
		return nil
	})
	if err != nil && err != errHalt {
		return nil, err
	}

	log.Errorw("READ DIR DONE", "handle", handle, "pos", pos, "seekPos", seekPos, "hpos", hnd.dirPos, "maxEntries", maxEntries, "out", out, "err", err)

	return out, nil
}

func (m *MfsSmbFs) Readlink(handle vfs.VfsHandle) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Unlink(handle vfs.VfsHandle) error {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Truncate(handle vfs.VfsHandle, u uint64) error {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Rename(handle vfs.VfsHandle, s string, i int) error {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Symlink(handle vfs.VfsHandle, s string, i int) (*vfs.Attributes, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Link(node vfs.VfsNode, node2 vfs.VfsNode, s string) (*vfs.Attributes, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Listxattr(handle vfs.VfsHandle) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Getxattr(handle vfs.VfsHandle, s string, bytes []byte) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Setxattr(handle vfs.VfsHandle, s string, bytes []byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *MfsSmbFs) Removexattr(handle vfs.VfsHandle, s string) error {
	//TODO implement me
	panic("implement me")
}

var _ vfs.VFSFileSystem = &MfsSmbFs{}

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

func checkPath(p string) (string, error) {
	if len(p) == 0 {
		return "", fmt.Errorf("paths must not be empty")
	}

	if p[0] != '/' {
		return "", fmt.Errorf("paths must start with a leading slash")
	}

	cleaned := gopath.Clean(p)
	if p[len(p)-1] == '/' && p != "/" {
		cleaned += "/"
	}
	return cleaned, nil
}
