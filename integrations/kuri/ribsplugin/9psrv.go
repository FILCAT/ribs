package kuboribs

import (
	"bytes"
	"context"
	"fmt"
	"github.com/docker/go-p9p"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/mfs"
	"go.uber.org/fx"
)

func StartMfs9p(lc fx.Lifecycle, fr *mfs.Root) {
	addr := ":5640" // Default port for 9P

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Errorf("failed to start 9p server: %s", err)
		return
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				log.Infof("9P server started on %s", addr)
				for {
					conn, err := listener.Accept()
					if err != nil {
						log.Errorf("failed to accept 9p connection: %s", err)
						return
					}
					// For each connection, serve 9P
					go func(conn net.Conn) {
						defer conn.Close()
						// Create a new session for this connection
						session := NewMfsSession(fr)
						// Use p9p.Dispatch to create a handler
						handler := p9p.Dispatch(session)
						// Serve the connection using p9p
						ctx := context.Background()
						err := p9p.ServeConn(ctx, conn, handler)
						if err != nil {
							log.Errorf("error serving 9p connection: %s", err)
						}
					}(conn)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return listener.Close()
		},
	})
}

// MfsSession implements the p9p.Session interface using MFS
type MfsSession struct {
	mr *mfs.Root

	mu          sync.Mutex
	fidMap      map[p9p.Fid]*fidContext
	nextQidPath uint64 // For generating unique Qid paths
}

type fidContext struct {
	path string
	node mfs.FSNode
	fd   mfs.FileDescriptor
	open bool
}

// NewMfsSession creates a new MfsSession
func NewMfsSession(mr *mfs.Root) *MfsSession {
	return &MfsSession{
		mr:          mr,
		fidMap:      make(map[p9p.Fid]*fidContext),
		nextQidPath: 1,
	}
}

func (s *MfsSession) Version() (msize int, version string) {
	return p9p.DefaultMSize, p9p.DefaultVersion
}

func (s *MfsSession) Auth(ctx context.Context, afid p9p.Fid, uname, aname string) (p9p.Qid, error) {
	// We don't support authentication
	return p9p.Qid{}, p9p.ErrUnexpectedMsg
}

func (s *MfsSession) Attach(ctx context.Context, fid, afid p9p.Fid, uname, aname string) (p9p.Qid, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if afid != p9p.NOFID {
		// Authentication not supported
		log.Warnw("9P attach with afid", "afid", afid)
		return p9p.Qid{}, p9p.ErrUnexpectedMsg
	}

	// Check if fid is already in use
	if _, exists := s.fidMap[fid]; exists {
		return p9p.Qid{}, p9p.ErrDupfid
	}

	// Create a new fidContext for the root directory
	path := "/"
	node := s.mr.GetDirectory()

	qid, _ := s.nodeToQid(node)

	s.fidMap[fid] = &fidContext{
		path: path,
		node: node,
	}

	return qid, nil
}

func (s *MfsSession) Walk(ctx context.Context, fid p9p.Fid, newfid p9p.Fid, names ...string) ([]p9p.Qid, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fc, exists := s.fidMap[fid]
	if !exists {
		return nil, p9p.ErrUnknownfid
	}

	// Check if newfid is already in use
	if _, exists := s.fidMap[newfid]; exists {
		return nil, p9p.ErrDupfid
	}

	// Starting from fc.path, walk through the names
	var qids []p9p.Qid
	currentPath := fc.path
	currentNode := fc.node

	for _, name := range names {
		// Handle "." and ".."
		if name == "." {
			// Do nothing
		} else if name == ".." {
			// Move up one level
			currentPath = path.Dir(currentPath)
			if currentPath == "" {
				currentPath = "/"
			}
			node, err := mfs.Lookup(s.mr, currentPath)
			if err != nil {
				return nil, err
			}
			currentNode = node
		} else {
			// Walk into the next name
			if currentPath == "/" {
				currentPath = "/" + name
			} else {
				currentPath = currentPath + "/" + name
			}
			node, err := mfs.Lookup(s.mr, currentPath)
			if err != nil {
				return nil, err
			}
			currentNode = node
		}

		// Create a Qid for the current node
		qid, _ := s.nodeToQid(currentNode)
		qids = append(qids, qid)
	}

	// Create a new fidContext for newfid
	s.fidMap[newfid] = &fidContext{
		path: currentPath,
		node: currentNode,
	}

	return qids, nil
}

func (s *MfsSession) Open(ctx context.Context, fid p9p.Fid, mode p9p.Flag) (p9p.Qid, uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fc, exists := s.fidMap[fid]
	if !exists {
		return p9p.Qid{}, 0, p9p.ErrUnknownfid
	}

	// Open the file
	// For directories, we might not need to do anything
	// For files, we need to open the file

	if fc.open {
		log.Warnw("9P open on already open fid", "fid", fid)
		return p9p.Qid{}, 0, p9p.ErrUnexpectedMsg
	}

	var err error

	if mode&p9p.OREAD != 0 || mode&p9p.ORDWR != 0 {
		// Open for reading
		err = s.openFile(fc, os.O_RDONLY)
		if err != nil {
			return p9p.Qid{}, 0, err
		}
	}

	if mode&p9p.OWRITE != 0 || mode&p9p.ORDWR != 0 {
		// Open for writing
		err = s.openFile(fc, os.O_WRONLY)
		if err != nil {
			return p9p.Qid{}, 0, err
		}
	}

	fc.open = true

	qid, _ := s.nodeToQid(fc.node)
	return qid, 0, nil // iounit set to 0
}

func (s *MfsSession) Read(ctx context.Context, fid p9p.Fid, p []byte, offset int64) (n int, err error) {
	s.mu.Lock()
	fc, exists := s.fidMap[fid]
	s.mu.Unlock()
	if !exists {
		return 0, p9p.ErrUnknownfid
	}

	// If fc.fd is nil, we need to open it
	if fc.fd == nil {
		err = s.openFile(fc, os.O_RDONLY)
		if err != nil {
			return 0, err
		}
	}

	// For directories, we need to return directory entries
	switch node := fc.node.(type) {
	case *mfs.Directory:
		// Read directory entries
		entries, err := s.readDirEntries(node)
		if err != nil {
			return 0, err
		}
		// Serialize entries into p9p.Dir structures
		// Encode entries into p

		var buf bytes.Buffer
		codec := p9p.NewCodec()
		for _, entry := range entries {
			dir := s.entryToDir(entry)
			err := p9p.EncodeDir(codec, &buf, &dir)
			if err != nil {
				return 0, err
			}
		}

		data := buf.Bytes()
		if offset >= int64(len(data)) {
			return 0, io.EOF
		}
		n = copy(p, data[offset:])
		return n, nil

	case *mfs.File:
		// Read from file
		// Seek to the offset
		_, err := fc.fd.Seek(offset, io.SeekStart)
		if err != nil {
			return 0, err
		}

		// Read into p
		n, err = fc.fd.Read(p)
		return n, err
	default:
		return 0, fmt.Errorf("unknown node type")
	}
}

func (s *MfsSession) Write(ctx context.Context, fid p9p.Fid, p []byte, offset int64) (n int, err error) {
	s.mu.Lock()
	fc, exists := s.fidMap[fid]
	s.mu.Unlock()
	if !exists {
		return 0, p9p.ErrUnknownfid
	}

	// If fc.fd is nil, we need to open it
	if fc.fd == nil {
		err = s.openFile(fc, os.O_WRONLY)
		if err != nil {
			return 0, err
		}
	}

	// Seek to the offset
	_, err = fc.fd.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	// Write from p
	n, err = fc.fd.Write(p)
	return n, err
}

func (s *MfsSession) Clunk(ctx context.Context, fid p9p.Fid) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fc, exists := s.fidMap[fid]
	if !exists {
		return p9p.ErrUnknownfid
	}

	// Close any open file descriptors
	if fc.fd != nil {
		err := fc.fd.Close()
		if err != nil {
			return err
		}
	}

	// Remove the fid from the map
	delete(s.fidMap, fid)
	return nil
}

func (s *MfsSession) Remove(ctx context.Context, fid p9p.Fid) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fc, exists := s.fidMap[fid]
	if !exists {
		return p9p.ErrUnknownfid
	}

	// Remove the file or directory at fc.path
	parentPath, name := path.Split(fc.path)

	parentNode, err := mfs.Lookup(s.mr, parentPath)
	if err != nil {
		return err
	}

	pdir, ok := parentNode.(*mfs.Directory)
	if !ok {
		return fmt.Errorf("parent is not a directory")
	}

	err = pdir.Unlink(name)
	if err != nil {
		return err
	}

	// Remove the fid
	delete(s.fidMap, fid)
	return nil
}

func (s *MfsSession) Create(ctx context.Context, parentFid p9p.Fid, name string, perm uint32, mode p9p.Flag) (p9p.Qid, uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	parentFc, exists := s.fidMap[parentFid]
	if !exists {
		return p9p.Qid{}, 0, p9p.ErrUnknownfid
	}

	parentDir, ok := parentFc.node.(*mfs.Directory)
	if !ok {
		log.Warnw("9P create on non-directory", "fid", parentFid)
		return p9p.Qid{}, 0, p9p.ErrBaddir
	}

	// Create the new file or directory
	// Determine if it's a file or directory based on perm
	var node mfs.FSNode
	if perm&p9p.DMDIR != 0 {
		// Create directory
		err := mfs.Mkdir(s.mr, path.Join(parentFc.path, name), mfs.MkdirOpts{Mkparents: false})
		if err != nil {
			return p9p.Qid{}, 0, err
		}
		node, err = parentDir.Child(name)
		if err != nil {
			return p9p.Qid{}, 0, err
		}
	} else {
		// Create file
		nd := dag.NodeWithData(ft.FilePBData(nil, 0))
		nd.SetCidBuilder(v1CidPrefix)
		err := parentDir.AddChild(name, nd)
		if err != nil {
			return p9p.Qid{}, 0, err
		}
		node, err = parentDir.Child(name)
		if err != nil {
			return p9p.Qid{}, 0, err
		}
	}

	qid, _ := s.nodeToQid(node)

	// Open the file as per the mode
	fc := &fidContext{
		path: path.Join(parentFc.path, name),
		node: node,
		open: true,
	}
	err := s.openFile(fc, int(mode))
	if err != nil {
		return p9p.Qid{}, 0, err
	}

	// Assign the new fid for the created file (reuse parentFid)
	s.fidMap[parentFid] = fc

	return qid, 0, nil
}

func (s *MfsSession) Stat(ctx context.Context, fid p9p.Fid) (p9p.Dir, error) {
	s.mu.Lock()
	fc, exists := s.fidMap[fid]
	s.mu.Unlock()
	if !exists {
		return p9p.Dir{}, p9p.ErrUnknownfid
	}

	// Get node info and construct p9p.Dir
	dir, err := s.nodeToDir(fc.node)
	if err != nil {
		return p9p.Dir{}, err
	}

	return dir, nil
}

func (s *MfsSession) WStat(ctx context.Context, fid p9p.Fid, dir p9p.Dir) error {
	// Not implemented
	return p9p.ErrNostat
}

func (s *MfsSession) openFile(fc *fidContext, flags int) error {
	// Open the file
	fileNode, ok := fc.node.(*mfs.File)
	if !ok {
		return fmt.Errorf("node is not a file")
	}

	fd, err := fileNode.Open(mfs.Flags{Read: flags&os.O_RDONLY != 0, Write: flags&os.O_WRONLY != 0 || flags&os.O_RDWR != 0})
	if err != nil {
		return err
	}

	fc.fd = fd
	return nil
}

func (s *MfsSession) nodeToQid(node mfs.FSNode) (p9p.Qid, string) {
	var name string

	var qtype p9p.QType
	switch n := node.(type) {
	case *mfs.Directory:
		qtype = p9p.QTDIR
		name = filepath.Base(n.Path())
	case *mfs.File:
		qtype = p9p.QTFILE
	default:
		qtype = p9p.QTFILE
	}

	s.mu.Lock()
	path := s.nextQidPath
	s.nextQidPath++
	s.mu.Unlock()

	return p9p.Qid{
		Type:    qtype,
		Version: 0,
		Path:    path,
	}, name
}

func (s *MfsSession) nodeToDir(node mfs.FSNode) (p9p.Dir, error) {
	qid, name := s.nodeToQid(node)
	mode := uint32(0644)
	length := uint64(0)

	switch n := node.(type) {
	case *mfs.Directory:
		mode |= p9p.DMDIR
	case *mfs.File:
		size, err := n.Size()
		if err != nil {
			return p9p.Dir{}, err
		}
		length = uint64(size)
	}

	dir := p9p.Dir{
		Name:    name,
		Qid:     qid,
		Mode:    mode,
		Length:  length,
		ModTime: time.Unix(0, 0),
	}

	return dir, nil
}

func (s *MfsSession) readDirEntries(dir *mfs.Directory) ([]mfs.NodeListing, error) {
	var entries []mfs.NodeListing
	err := dir.ForEachEntry(context.TODO(), func(entry mfs.NodeListing) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

func (s *MfsSession) entryToDir(entry mfs.NodeListing) p9p.Dir {
	var qtype p9p.QType
	if entry.Type == int(mfs.TDir) {
		qtype = p9p.QTDIR
	} else {
		qtype = p9p.QTFILE
	}

	qid := p9p.Qid{
		Type:    qtype,
		Version: 0,
		Path:    s.genQidPath(),
	}

	dir := p9p.Dir{
		Name:    entry.Name,
		Qid:     qid,
		Mode:    uint32(0644),
		Length:  uint64(entry.Size),
		ModTime: time.Unix(0, 0),
	}

	if qtype == p9p.QTDIR {
		dir.Mode |= p9p.DMDIR
	}

	return dir
}

func (s *MfsSession) genQidPath() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.nextQidPath
	s.nextQidPath++
	return path
}
