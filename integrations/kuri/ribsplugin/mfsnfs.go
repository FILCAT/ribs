package kuboribs

import (
	"context"
	"fmt"
	"github.com/go-git/go-billy/v5"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/mfs"
	format "github.com/ipfs/go-ipld-format"
	"github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"io"
	"net"
	"os"
	gopath "path"
	"sync"
	"time"
)

func StartMfsNFSFs(lc fx.Lifecycle, mr *mfs.Root, ng format.DAGService) error {
	listener, err := net.Listen("tcp", ":4499")
	if err != nil {
		return xerrors.Errorf("failed to listen: %w", err)
	}

	log.Errorw("starting mfs nfs server", "addr", listener.Addr())

	handler := &AuthHandler{&mfsNfsFs{mr, ng}}
	cacheHelper := nfshelper.NewCachingHandler(handler, 1024)
	go func() {
		fmt.Printf("%v", nfs.Serve(listener, cacheHelper))
	}()

	return nil
}

// AuthHandler returns a NFS backing that exposes a given file system in response to all mount requests.
type AuthHandler struct {
	fs billy.Filesystem
}

// Mount backs Mount RPC Requests, allowing for access control policies.
func (h *AuthHandler) Mount(ctx context.Context, conn net.Conn, req nfs.MountRequest) (status nfs.MountStatus, hndl billy.Filesystem, auths []nfs.AuthFlavor) {
	status = nfs.MountStatusOk
	hndl = h.fs
	auths = []nfs.AuthFlavor{nfs.AuthFlavorNull}
	return
}

// Change provides an interface for updating file attributes.
func (h *AuthHandler) Change(fs billy.Filesystem) billy.Change {
	if c, ok := h.fs.(billy.Change); ok {
		return c
	}
	return nil
}

const (
	blockSize uint64 = 4092
	volSize   uint64 = 8900 << 50
	volUsed   uint64 = 1700 << 50

	// todo show filecoin metrics in those
	volBlocks     uint64 = volSize / blockSize
	volBlocksUsed uint64 = volUsed / blockSize
	volBavail     uint64 = volBlocks - volBlocksUsed

	// todo cid.contact metrics
	totalFsFiles  = 12_000_000_000_000 * 10
	freeFileNodes = totalFsFiles / 10

	ioSize = 256 << 20
)

// FSStat provides information about a filesystem.
func (h *AuthHandler) FSStat(ctx context.Context, f billy.Filesystem, s *nfs.FSStat) error {
	*s = nfs.FSStat{
		TotalSize:      volSize,
		FreeSize:       volBavail * blockSize,
		AvailableSize:  volBavail * blockSize,
		TotalFiles:     totalFsFiles,
		FreeFiles:      freeFileNodes,
		AvailableFiles: freeFileNodes,
		CacheHint:      0,
	}

	return nil
}

// ToHandle handled by CachingHandler
func (h *AuthHandler) ToHandle(f billy.Filesystem, s []string) []byte {
	return []byte{}
}

// FromHandle handled by CachingHandler
func (h *AuthHandler) FromHandle([]byte) (billy.Filesystem, []string, error) {
	return nil, []string{}, nil
}

// HandleLImit handled by cachingHandler
func (h *AuthHandler) HandleLimit() int {
	return -1
}

type mfsNfsFs struct {
	mr *mfs.Root

	ng format.NodeGetter
}

func (m *mfsNfsFs) Capabilities() billy.Capability {
	return billy.WriteCapability | billy.ReadCapability |
		billy.ReadAndWriteCapability | billy.SeekCapability | billy.TruncateCapability
}

func (m *mfsNfsFs) Create(filename string) (billy.File, error) {
	return m.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func (m *mfsNfsFs) Open(filename string) (billy.File, error) {
	return m.OpenFile(filename, os.O_RDONLY, 0)
}

func (m *mfsNfsFs) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	log.Errorw("OPEN FILE", "name", name, "flag", flag, "perm", perm)

	path, err := checkPath(name)
	if err != nil {
		return nil, err
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
			/*return &mfsNfsFile{
				mr:  m.mr,
				mfd: target.(*mfs.Directory),

				path: name,
			}, nil*/

			return nil, xerrors.Errorf("file %s is a directory", path)
		}

	case os.ErrNotExist:
		if !create {
			return nil, err
		}

		// if create is specified and the file doesn't exist, we create the file
		dirname, fname := gopath.Split(path)
		pdir, err := getParentDir(m.mr, dirname)
		if err != nil {
			return nil, err
		}

		nd := dag.NodeWithData(ft.FilePBData(nil, 0))
		nd.SetCidBuilder(v1CidPrefix)
		err = pdir.AddChild(fname, nd)
		if err != nil {
			return nil, err
		}

		fsn, err := pdir.Child(fname)
		if err != nil {
			return nil, err
		}

		var ok bool
		fi, ok = fsn.(*mfs.File)
		if !ok {
			return nil, xerrors.New("expected *mfs.File, didn't get it. This is likely a race condition")
		}
	default:
		return nil, err
	}

	fi.RawLeaves = true

	fd, err := fi.Open(mfs.Flags{Read: read, Write: write, Sync: flush})
	if err != nil {
		return nil, xerrors.Errorf("mfsfile open (%t %t %t, %x): %w", read, write, flush, flag, err)
	}

	if truncate {
		if err := fd.Truncate(0); err != nil {
			return nil, xerrors.Errorf("truncate: %w", err)
		}
	}

	seek := io.SeekStart
	if app {
		seek = io.SeekEnd
	}

	_, err = fd.Seek(0, seek)
	if err != nil {
		return nil, xerrors.Errorf("mfs open seek: %w", err)
	}

	return &mfsNfsFile{
		mr:  m.mr,
		mfd: fd,

		path: name,
	}, nil

}

type mfsNfsFile struct {
	mr  *mfs.Root
	mfd mfs.FileDescriptor

	posLk  sync.Mutex
	posErr error

	path string
}

func (m *mfsNfsFile) Name() string {
	return m.path
}

func (m *mfsNfsFile) Write(p []byte) (n int, err error) {
	m.posLk.Lock()
	defer m.posLk.Unlock()

	if m.posErr != nil {
		return 0, m.posErr
	}

	return m.mfd.Write(p)
}

func (m *mfsNfsFile) Read(p []byte) (n int, err error) {
	m.posLk.Lock()
	defer m.posLk.Unlock()

	if m.posErr != nil {
		return 0, m.posErr
	}

	return m.mfd.Read(p)
}

func (m *mfsNfsFile) ReadAt(p []byte, off int64) (n int, err error) {
	m.posLk.Lock()
	defer m.posLk.Unlock()

	if m.posErr != nil {
		return 0, m.posErr
	}

	oldPos, err := m.mfd.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	defer func() {
		_, err := m.mfd.Seek(oldPos, io.SeekStart)
		if err != nil {
			log.Errorw("failed to seek back to original position", "err", err)
			m.posErr = err
		}
	}()

	_, err = m.mfd.Seek(off, io.SeekStart)
	if err != nil {
		return 0, xerrors.Errorf("read at seek: %w", err)
	}

	return io.ReadFull(m.mfd, p)
}

func (m *mfsNfsFile) Seek(offset int64, whence int) (int64, error) {
	return m.mfd.Seek(offset, whence)
}

func (m *mfsNfsFile) Close() error {
	return m.mfd.Close()
}

func (m *mfsNfsFile) Lock() error {
	//TODO implement me
	panic("implement me")
}

func (m *mfsNfsFile) Unlock() error {
	//TODO implement me
	panic("implement me")
}

func (m *mfsNfsFile) Truncate(size int64) error {
	return m.mfd.Truncate(size)
}

func (m *mfsNfsFs) Stat(filename string) (os.FileInfo, error) {
	return m.Lstat(filename)
}

func (m *mfsNfsFs) Rename(oldName, newName string) error {
	log.Errorw("RENAME", "oldName", oldName, "newName", newName)
	src, err := checkPath(oldName)
	if err != nil {
		return err
	}
	dst, err := checkPath(newName)
	if err != nil {
		return err
	}

	err = mfs.Mv(m.mr, src, dst)
	/*if err == nil && flush {
		_, err = mfs.FlushPath(ctx, m.mr, "/")
	}*/

	return err
}

func (m *mfsNfsFs) Remove(name string) error {
	log.Errorw("REMOVE", "name", name)

	path, err := checkPath(name)
	if err != nil {
		return err
	}

	// 'rm a/b/c/' will fail unless we trim the slash at the end
	if path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}

	dir, name := gopath.Split(path)

	pdir, err := getParentDir(m.mr, dir)
	if err != nil {
		return xerrors.Errorf("%s: parent lookup: %w", path, err)
	}

	// get child node by name, when the node is corrupted and nonexistent,
	// it will return specific error.
	child, err := pdir.Child(name)
	if err != nil {
		return xerrors.Errorf("%s: %w", path, err)
	}

	switch d := child.(type) {
	case *mfs.Directory:
		n, err := d.ListNames(context.Background())
		if err != nil {
			return xerrors.Errorf("list %s: %w", path, err)
		}

		if len(n) > 0 {
			return xerrors.Errorf("%s: directory not empty", path)
		}
	}

	err = pdir.Unlink(name)
	if err != nil {
		return xerrors.Errorf("%s: %w", path, err)
	}

	err = pdir.Flush()
	if err != nil {
		return xerrors.Errorf("%s: %w", path, err)
	}

	return nil
}

func (m *mfsNfsFs) Join(elem ...string) string {
	return gopath.Join(elem...)
}

func (m *mfsNfsFs) TempFile(dir, prefix string) (billy.File, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsNfsFs) ReadDir(path string) ([]os.FileInfo, error) {
	log.Errorw("READ DIR", "path", path)

	var out []os.FileInfo

	target, err := mfs.Lookup(m.mr, path)
	if err != nil {
		return nil, err
	}

	mfd, ok := target.(*mfs.Directory)
	if !ok {
		return nil, xerrors.Errorf("%s: not a directory", path)
	}

	preloadctx, done := context.WithCancel(context.Background())
	defer done()

	rn, err := mfd.GetNode()
	if err != nil {
		log.Errorw("failed to preload directory", "err", err)
	} else {
		go func() {
			for _, l := range rn.Links() {
				go func(l format.Link) {
					_, err := m.ng.Get(preloadctx, l.Cid)
					if err != nil {
						log.Errorw("failed to preload directory link", "err", err, "cid", l.Cid, "name", l.Name)
					}
					log.Errorw("readdir node preload ok", "cid", l.Cid, "name", l.Name)
				}(*l)
			}
		}()
	}

	err = mfd.ForEachEntry(context.TODO(), func(listing mfs.NodeListing) error {
		ent := basicFileInfos{
			name:    listing.Name,
			size:    listing.Size,
			mode:    0644,
			modTime: time.Unix(0, 0),
			isDir:   false,
		}

		if listing.Type == int(mfs.TDir) {
			ent.isDir = true
			ent.mode = 0755
		}

		out = append(out, &ent)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (m *mfsNfsFs) MkdirAll(filename string, perm os.FileMode) error {
	log.Errorw("MKDIR ALL", "filename", filename, "perm", perm)
	return mfs.Mkdir(m.mr, filename, mfs.MkdirOpts{Mkparents: true})
}

func (m *mfsNfsFs) Lstat(name string) (os.FileInfo, error) {
	log.Errorw("LSTAT", "filename", name)

	path, err := checkPath(name)
	if err != nil {
		return nil, err
	}

	nd, err := getNodeFromPath(context.TODO(), m.mr, path)
	if err != nil {
		return nil, err
	}

	switch n := nd.(type) {
	case *dag.ProtoNode:
		d, err := ft.FSNodeFromBytes(n.Data())
		if err != nil {
			return nil, err
		}

		/*var ndtype string
		switch d.Type() {
		case ft.TDirectory, ft.THAMTShard:
			ndtype = "directory"
		case ft.TFile, ft.TMetadata, ft.TRaw:
			ndtype = "file"
		default:
			return nil, fmt.Errorf("unrecognized node type: %s", d.Type())
		}*/

		/*return &statOutput{
			Hash:           enc.Encode(c),
			Blocks:         len(nd.Links()),
			Size:           d.FileSize(),
			CumulativeSize: cumulsize,
			Type:           ndtype,
		}, nil*/

		mode := os.FileMode(0644)
		if d.Type() == ft.TDirectory {
			mode = os.FileMode(0755) | os.ModeDir
		}

		return &basicFileInfos{
			name:    gopath.Base(path),
			size:    int64(d.FileSize()), // nd.Size?
			mode:    mode,
			modTime: time.Unix(0, 0),
			isDir:   d.Type() == ft.TDirectory,
		}, nil
	case *dag.RawNode:
		return &basicFileInfos{
			name:    gopath.Base(path),
			size:    int64(len(n.RawData())),
			mode:    0644,
			modTime: time.Unix(0, 0),
			isDir:   false,
		}, nil
	default:
		return nil, fmt.Errorf("not unixfs node (proto or raw)")
	}
}

func (m *mfsNfsFs) Symlink(target, link string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mfsNfsFs) Readlink(link string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsNfsFs) Chroot(path string) (billy.Filesystem, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mfsNfsFs) Root() string {
	return "/"
}
