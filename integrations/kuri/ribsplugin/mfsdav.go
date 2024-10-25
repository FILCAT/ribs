package kuboribs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	gopath "path"
	"strings"
	"time"

	"go.uber.org/fx"
	"golang.org/x/net/webdav"
	"golang.org/x/xerrors"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/mfs"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

func StartMfsDav(lc fx.Lifecycle, fr *mfs.Root) {
	davHandler := &webdav.Handler{
		Prefix:     "",
		FileSystem: &mfsDavFs{mr: fr},
		LockSystem: webdav.NewMemLS(),

		Logger: func(r *http.Request, err error) {
			log.Errorw("dav", "err", err, "req", r.URL, "method", r.Method)
		},
	}

	srv := &http.Server{
		Addr:    ":8077",
		Handler: davHandler,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := srv.ListenAndServe(); err != nil {
					log.Errorf("failed to start ribs http server: %s", err)
				}

				fmt.Println("dav http at http://localhost:8077")
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})
}

type mfsDavFs struct {
	mr *mfs.Root
}

func (m *mfsDavFs) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return mfs.Mkdir(m.mr, name, mfs.MkdirOpts{Mkparents: true, Mode: perm, ModTime: time.Now()})
}

type mfsDavFile struct {
	mr  *mfs.Root
	mfd mfs.FileDescriptor

	mode  os.FileMode
	mtime time.Time

	path string
}

func (m *mfsDavFile) Close() error {
	return m.mfd.Close()
}

func (m *mfsDavFile) Read(p []byte) (n int, err error) {
	return m.mfd.Read(p)
}

func (m *mfsDavFile) Seek(offset int64, whence int) (int64, error) {
	return m.mfd.Seek(offset, whence)
}

func (m *mfsDavFile) Readdir(count int) ([]fs.FileInfo, error) {
	return nil, xerrors.Errorf("not supported on files")
}

func (m *mfsDavFile) Stat() (fs.FileInfo, error) {
	fsz, err := m.mfd.Size()
	if err != nil {
		return nil, err
	}

	return &basicFileInfos{
		name:    gopath.Base(m.path),
		size:    fsz,
		mode:    m.mode,
		modTime: m.mtime,
		isDir:   false,
	}, nil
}

func (m *mfsDavFile) Write(p []byte) (n int, err error) {
	return m.mfd.Write(p)
}

type mfsDavDir struct {
	mr  *mfs.Root
	mfd *mfs.Directory

	path string
}

func (m *mfsDavDir) Close() error {
	return m.mfd.Flush()
}

func (m *mfsDavDir) Read(p []byte) (n int, err error) {
	return 0, xerrors.Errorf("not supported on dirs")
}

func (m *mfsDavDir) Seek(offset int64, whence int) (int64, error) {
	return 0, xerrors.Errorf("not supported on dirs")
}

func (m *mfsDavDir) Readdir(count int) ([]fs.FileInfo, error) {
	var out []fs.FileInfo

	var errStop = errors.New("stop")

	err := m.mfd.ForEachEntry(context.TODO(), func(listing mfs.NodeListing) error {
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

		if len(out) == count {
			return errStop
		}

		return nil
	})
	if err != nil && err != errStop {
		return nil, err
	}

	return out, nil
}

func (m *mfsDavDir) Stat() (fs.FileInfo, error) {
	nd, err := m.mfd.GetNode()
	if err != nil {
		return nil, xerrors.Errorf("get node for dir stat: %w", err)
	}

	out := &basicFileInfos{
		name:    gopath.Base(m.path),
		size:    0,
		mode:    0755,
		modTime: time.Unix(0, 0),
		isDir:   true,
	}

	switch n := nd.(type) {
	case *dag.ProtoNode:
		d, err := ft.FSNodeFromBytes(n.Data())
		if err != nil {
			return nil, err
		}

		out.mode = d.Mode()
		out.modTime = d.ModTime()
	}

	return out, nil
}

func (m *mfsDavDir) Write(p []byte) (n int, err error) {
	return 0, xerrors.Errorf("not supported on dirs")
}

func (m *mfsDavFs) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	log.Debugw("OPEN FILE", "name", name, "flag", flag, "perm", perm)

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
			return &mfsDavDir{
				mr:  m.mr,
				mfd: target.(*mfs.Directory),

				path: name,
			}, nil
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

		nd := dag.NodeWithData(ft.FilePBDataWithStat(nil, 0, perm, time.Now()))
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

	var mode os.FileMode
	if m, err := fi.Mode(); err == nil {
		mode = m
	}

	var mtime time.Time
	if m, err := fi.ModTime(); err == nil {
		mtime = m
	}

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

	return &mfsDavFile{
		mr:  m.mr,
		mfd: fd,

		mode:  mode,
		mtime: mtime,

		path: name,
	}, nil
}

var v1CidPrefix = cid.Prefix{
	Codec:    cid.DagProtobuf,
	MhLength: -1,
	MhType:   mh.SHA2_256,
	Version:  1,
}

func (m *mfsDavFs) RemoveAll(ctx context.Context, name string) error {
	log.Errorw("REMOVE ALL", "name", name)
	arg := name

	path, err := checkPath(arg)
	if err != nil {
		return err
	}

	if path == "/" {
		return xerrors.Errorf("%s: cannot delete root", path)
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

	/*	if force {
			err := pdir.Unlink(name)
			if err != nil {
				if err == os.ErrNotExist {
					continue
				}
				errs = append(errs, fmt.Errorf("%s: %w", path, err))
				continue
			}
			err = pdir.Flush()
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", path, err))
			}
			continue
		}
	*/
	// get child node by name, when the node is corrupted and nonexistent,
	// it will return specific error.
	child, err := pdir.Child(name)
	if err != nil {
		return xerrors.Errorf("%s: %w", path, err)
	}

	switch child.(type) {
	case *mfs.Directory:
		/*if !dashr {
			errs = append(errs, fmt.Errorf("%s is a directory, use -r to remove directories", path))
			continue
		}*/
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

func checkPath(p string) (string, error) {
	if len(p) == 0 {
		p = "/"
	}

	if p[0] != '/' {
		p = "/" + p
	}

	cleaned := gopath.Clean(p)
	if p[len(p)-1] == '/' && p != "/" {
		cleaned += "/"
	}
	return cleaned, nil
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

func (m *mfsDavFs) Rename(ctx context.Context, oldName, newName string) error {
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

func (m *mfsDavFs) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	log.Errorw("STAT", "name", name)
	path, err := checkPath(name)
	if err != nil {
		return nil, err
	}

	nd, err := getNodeFromPath(ctx, m.mr, path)
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

		return &basicFileInfos{
			name:    gopath.Base(path),
			size:    int64(d.FileSize()), // nd.Size?
			mode:    d.Mode(),
			modTime: d.ModTime(),
			isDir:   d.Type() == ft.TDirectory || d.Type() == ft.THAMTShard,
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

type basicFileInfos struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (b *basicFileInfos) Name() string {
	return b.name
}

func (b *basicFileInfos) Size() int64 {
	return b.size
}

func (b *basicFileInfos) Mode() fs.FileMode {
	return b.mode
}

func (b *basicFileInfos) ModTime() time.Time {
	return b.modTime
}

func (b *basicFileInfos) IsDir() bool {
	return b.isDir
}

func (b *basicFileInfos) Sys() any {
	return nil
}

func getNodeFromPath(ctx context.Context, mr *mfs.Root, p string) (ipld.Node, error) {
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
