package kuboribs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/docker/go-p9p"
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
						log.Errorf("Accepted new connection from %s", conn.RemoteAddr())
						defer func() {
							log.Infof("Connection from %s closed", conn.RemoteAddr())
							conn.Close()
						}()
						// Create a new session for this connection
						session := NewMfsSession(fr)
						// Use p9p.Dispatch to create a handler
						handler := p9p.Dispatch(session)
						// Serve the connection using p9p
						ctx := context.Background()
						err := p9p.ServeConn(ctx, conn, &loggingHandler{handler})
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
	log.Debugf("Creating new MfsSession")
	return &MfsSession{
		mr:          mr,
		fidMap:      make(map[p9p.Fid]*fidContext),
		nextQidPath: 1,
	}
}

func (s *MfsSession) Version() (msize int, version string) {
	log.Debugf("Version called")
	return p9p.DefaultMSize, p9p.DefaultVersion
}

func (s *MfsSession) Auth(ctx context.Context, afid p9p.Fid, uname, aname string) (p9p.Qid, error) {
	log.Debugf("Auth called with afid=%d, uname=%s, aname=%s", afid, uname, aname)
	// We don't support authentication
	return p9p.Qid{}, p9p.ErrUnexpectedMsg
}

func (s *MfsSession) Attach(ctx context.Context, fid, afid p9p.Fid, uname, aname string) (p9p.Qid, error) {
	log.Debugf("Attach called with fid=%d, afid=%d, uname=%s, aname=%s", fid, afid, uname, aname)
	s.mu.Lock()
	defer s.mu.Unlock()

	if afid != p9p.NOFID {
		// Authentication not supported
		log.Warnw("9P attach with afid", "afid", afid)
		return p9p.Qid{}, p9p.ErrUnexpectedMsg
	}

	// Check if fid is already in use
	if _, exists := s.fidMap[fid]; exists {
		log.Warnf("Attach: fid %d already in use", fid)
		return p9p.Qid{}, p9p.ErrDupfid
	}

	// Create a new fidContext for the root directory
	path := "/"
	node := s.mr.GetDirectory()

	qid, _ := s.nodeToQid(node, true)

	s.fidMap[fid] = &fidContext{
		path: path,
		node: node,
	}

	log.Debugf("Attach: attached fid %d to path %s", fid, path)

	return qid, nil
}

func (s *MfsSession) Walk(ctx context.Context, fid p9p.Fid, newfid p9p.Fid, names ...string) ([]p9p.Qid, error) {
	log.Debugf("Walk called with fid=%d, newfid=%d, names=%v", fid, newfid, names)
	s.mu.Lock()
	defer s.mu.Unlock()

	fc, exists := s.fidMap[fid]
	if !exists {
		log.Warnf("Walk: unknown fid %d", fid)
		return nil, p9p.ErrUnknownfid
	}

	// Check if newfid is already in use
	if _, exists := s.fidMap[newfid]; exists {
		log.Warnf("Walk: newfid %d already in use", newfid)
		return nil, p9p.ErrDupfid
	}

	// Starting from fc.path, walk through the names
	var qids []p9p.Qid
	currentPath := fc.path
	currentNode := fc.node

	for _, name := range names {
		log.Debugf("Walk: currentPath=%s, name=%s", currentPath, name)
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
				log.Errorf("Walk: error looking up %s: %v", currentPath, err)
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
				log.Errorf("Walk: error looking up %s: %v", currentPath, err)
				return nil, err
			}
			currentNode = node
		}

		// Create a Qid for the current node
		qid, _ := s.nodeToQid(currentNode, true)
		qids = append(qids, qid)
	}

	// Create a new fidContext for newfid
	s.fidMap[newfid] = &fidContext{
		path: currentPath,
		node: currentNode,
	}

	log.Debugf("Walk: newfid %d points to path %s", newfid, currentPath)

	return qids, nil
}

func (s *MfsSession) Open(ctx context.Context, fid p9p.Fid, mode p9p.Flag) (p9p.Qid, uint32, error) {
	log.Debugf("Open called with fid=%d, mode=%v", fid, mode)
	s.mu.Lock()
	defer s.mu.Unlock()

	fc, exists := s.fidMap[fid]
	if !exists {
		log.Warnf("Open: unknown fid %d", fid)
		return p9p.Qid{}, 0, p9p.ErrUnknownfid
	}

	if fc.open {
		log.Warnw("9P open on already open fid", "fid", fid)
		return p9p.Qid{}, 0, p9p.ErrUnexpectedMsg
	}

	var err error

	switch fc.node.(type) {
	case *mfs.Directory:
		// Directories can be opened without opening a file descriptor
		log.Debugf("Open: fid %d is a directory", fid)
		fc.open = true
	case *mfs.File:
		// Open the file
		err = s.openFile(fc, int(mode))
		if err != nil {
			log.Errorf("Open: error opening fid %d: %v", fid, err)
			return p9p.Qid{}, 0, err
		}
		fc.open = true
	default:
		log.Errorf("Open: unknown node type for fid %d", fid)
		return p9p.Qid{}, 0, fmt.Errorf("unknown node type")
	}

	qid, _ := s.nodeToQid(fc.node, true)
	log.Debugf("Open: fid %d opened", fid)
	return qid, 0, nil // iounit set to 0
}

func (s *MfsSession) Read(ctx context.Context, fid p9p.Fid, p []byte, offset int64) (n int, err error) {
	log.Debugf("Read called with fid=%d, offset=%d, len(p)=%d", fid, offset, len(p))
	s.mu.Lock()
	fc, exists := s.fidMap[fid]
	s.mu.Unlock()
	if !exists {
		log.Warnf("Read: unknown fid %d", fid)
		return 0, p9p.ErrUnknownfid
	}

	// For directories, we need to return directory entries
	switch node := fc.node.(type) {
	case *mfs.Directory:
		log.Debugf("Read: fid %d is a directory", fid)
		// Read directory entries
		entries, err := s.readDirEntries(node)
		if err != nil {
			log.Errorf("Read: error reading directory entries for fid %d: %v", fid, err)
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
				log.Errorf("Read: error encoding dir entry for fid %d: %v", fid, err)
				return 0, err
			}
		}

		data := buf.Bytes()
		if offset >= int64(len(data)) {
			log.Debugf("Read: offset %d beyond data length %d", offset, len(data))
			return 0, io.EOF
		}
		n = copy(p, data[offset:])
		log.Debugf("Read: read %d bytes from directory fid %d", n, fid)
		return n, nil

	case *mfs.File:
		log.Debugf("Read: fid %d is a file", fid)
		// If fc.fd is nil, we need to open it
		if fc.fd == nil {
			err = s.openFile(fc, os.O_RDONLY)
			if err != nil {
				log.Errorf("Read: error opening fid %d for reading: %v", fid, err)
				return 0, err
			}
		}
		// Read from file
		// Seek to the offset
		_, err := fc.fd.Seek(offset, io.SeekStart)
		if err != nil {
			log.Errorf("Read: error seeking in fid %d: %v", fid, err)
			return 0, err
		}

		// Read into p
		n, err = fc.fd.Read(p)
		if err != nil && err != io.EOF {
			log.Errorf("Read: error reading from fid %d: %v", fid, err)
		} else {
			log.Debugf("Read: read %d bytes from fid %d", n, fid)
		}
		return n, err
	default:
		log.Errorf("Read: unknown node type for fid %d", fid)
		return 0, fmt.Errorf("unknown node type")
	}
}

func (s *MfsSession) Write(ctx context.Context, fid p9p.Fid, p []byte, offset int64) (n int, err error) {
	log.Debugf("Write called with fid=%d, offset=%d, len(p)=%d", fid, offset, len(p))
	s.mu.Lock()
	fc, exists := s.fidMap[fid]
	s.mu.Unlock()
	if !exists {
		log.Warnf("Write: unknown fid %d", fid)
		return 0, p9p.ErrUnknownfid
	}

	switch fc.node.(type) {
	case *mfs.File:
		// If fc.fd is nil, we need to open it
		if fc.fd == nil {
			err = s.openFile(fc, os.O_WRONLY)
			if err != nil {
				log.Errorf("Write: error opening fid %d for writing: %v", fid, err)
				return 0, err
			}
		}

		// Seek to the offset
		_, err = fc.fd.Seek(offset, io.SeekStart)
		if err != nil {
			log.Errorf("Write: error seeking in fid %d: %v", fid, err)
			return 0, err
		}

		// Write from p
		n, err = fc.fd.Write(p)
		if err != nil {
			log.Errorf("Write: error writing to fid %d: %v", fid, err)
		} else {
			log.Debugf("Write: wrote %d bytes to fid %d", n, fid)
		}
		return n, err
	default:
		log.Errorf("Write: cannot write to fid %d: not a file", fid)
		return 0, fmt.Errorf("cannot write to directory or unknown node type")
	}
}

func (s *MfsSession) Clunk(ctx context.Context, fid p9p.Fid) error {
	log.Debugf("Clunk called with fid=%d", fid)
	s.mu.Lock()
	defer s.mu.Unlock()

	fc, exists := s.fidMap[fid]
	if !exists {
		log.Warnf("Clunk: unknown fid %d", fid)
		return p9p.ErrUnknownfid
	}

	// Close any open file descriptors
	if fc.fd != nil {
		err := fc.fd.Close()
		if err != nil {
			log.Errorf("Clunk: error closing fid %d: %v", fid, err)
			return err
		}
	}

	// Remove the fid from the map
	delete(s.fidMap, fid)
	log.Debugf("Clunk: fid %d closed and removed", fid)
	return nil
}

func (s *MfsSession) Remove(ctx context.Context, fid p9p.Fid) error {
	log.Debugf("Remove called with fid=%d", fid)
	s.mu.Lock()
	defer s.mu.Unlock()

	fc, exists := s.fidMap[fid]
	if !exists {
		log.Warnf("Remove: unknown fid %d", fid)
		return p9p.ErrUnknownfid
	}

	// Remove the file or directory at fc.path
	parentPath, name := path.Split(fc.path)
	log.Debugf("Remove: removing %s from parent %s", name, parentPath)

	parentNode, err := mfs.Lookup(s.mr, parentPath)
	if err != nil {
		log.Errorf("Remove: error looking up parent path %s: %v", parentPath, err)
		return err
	}

	pdir, ok := parentNode.(*mfs.Directory)
	if !ok {
		log.Errorf("Remove: parent %s is not a directory", parentPath)
		return fmt.Errorf("parent is not a directory")
	}

	err = pdir.Unlink(name)
	if err != nil {
		log.Errorf("Remove: error unlinking %s: %v", fc.path, err)
		return err
	}

	// Remove the fid
	delete(s.fidMap, fid)
	log.Debugf("Remove: fid %d removed", fid)
	return nil
}

func (s *MfsSession) Create(ctx context.Context, parentFid p9p.Fid, name string, perm uint32, mode p9p.Flag) (p9p.Qid, uint32, error) {
	log.Debugf("Create called with parentFid=%d, name=%s, perm=%o, mode=%v", parentFid, name, perm, mode)
	s.mu.Lock()
	defer s.mu.Unlock()

	parentFc, exists := s.fidMap[parentFid]
	if !exists {
		log.Warnf("Create: unknown parent fid %d", parentFid)
		return p9p.Qid{}, 0, p9p.ErrUnknownfid
	}

	parentDir, ok := parentFc.node.(*mfs.Directory)
	if !ok {
		log.Warnw("9P create on non-directory", "fid", parentFid)
		return p9p.Qid{}, 0, p9p.ErrBaddir
	}

	// Create the new file or directory
	var node mfs.FSNode
	if perm&p9p.DMDIR != 0 {
		// Create directory
		log.Debugf("Create: creating directory %s", path.Join(parentFc.path, name))
		err := mfs.Mkdir(s.mr, path.Join(parentFc.path, name), mfs.MkdirOpts{Mkparents: false})
		if err != nil {
			log.Errorf("Create: error creating directory %s: %v", name, err)
			return p9p.Qid{}, 0, err
		}
		node, err = parentDir.Child(name)
		if err != nil {
			log.Errorf("Create: error getting child node %s: %v", name, err)
			return p9p.Qid{}, 0, err
		}
	} else {
		// Create file
		log.Debugf("Create: creating file %s", path.Join(parentFc.path, name))
		nd := dag.NodeWithData(ft.FilePBData(nil, 0))
		nd.SetCidBuilder(v1CidPrefix)
		err := parentDir.AddChild(name, nd)
		if err != nil {
			log.Errorf("Create: error adding child %s: %v", name, err)
			return p9p.Qid{}, 0, err
		}
		node, err = parentDir.Child(name)
		if err != nil {
			log.Errorf("Create: error getting child node %s: %v", name, err)
			return p9p.Qid{}, 0, err
		}
	}

	qid, _ := s.nodeToQid(node, true)

	// Open the file as per the mode
	fc := &fidContext{
		path: path.Join(parentFc.path, name),
		node: node,
		open: true,
	}
	err := s.openFile(fc, int(mode))
	if err != nil {
		log.Errorf("Create: error opening new file %s: %v", name, err)
		return p9p.Qid{}, 0, err
	}

	// Assign the new fid for the created file (reuse parentFid)
	s.fidMap[parentFid] = fc

	log.Debugf("Create: created fid %d for %s", parentFid, fc.path)

	return qid, 0, nil
}

func (s *MfsSession) Stat(ctx context.Context, fid p9p.Fid) (p9p.Dir, error) {
	log.Debugf("Stat called with fid=%d", fid)
	s.mu.Lock()
	fc, exists := s.fidMap[fid]
	s.mu.Unlock()
	if !exists {
		log.Warnf("Stat: unknown fid %d", fid)
		return p9p.Dir{}, p9p.ErrUnknownfid
	}

	// Get node info and construct p9p.Dir
	dir, err := s.nodeToDir(fc.node)
	if err != nil {
		log.Errorf("Stat: error getting dir for fid %d: %v", fid, err)
		return p9p.Dir{}, err
	}

	return dir, nil
}

func (s *MfsSession) WStat(ctx context.Context, fid p9p.Fid, dir p9p.Dir) error {
	log.Debugf("WStat called with fid=%d", fid)
	// Not implemented
	log.Warnf("WStat not implemented")
	return p9p.ErrNostat
}

func (s *MfsSession) openFile(fc *fidContext, flags int) error {
	log.Debugf("openFile called with path=%s, flags=%d", fc.path, flags)
	// Open the file
	fileNode, ok := fc.node.(*mfs.File)
	if !ok {
		log.Errorf("openFile: node at %s is not a file", fc.path)
		return fmt.Errorf("node is not a file")
	}

	fd, err := fileNode.Open(mfs.Flags{
		Read:  flags&os.O_RDONLY != 0,
		Write: flags&os.O_WRONLY != 0 || flags&os.O_RDWR != 0,
	})
	if err != nil {
		log.Errorf("openFile: error opening file at %s: %v", fc.path, err)
		return err
	}

	fc.fd = fd
	log.Debugf("openFile: file at %s opened", fc.path)
	return nil
}

func (s *MfsSession) nodeToQid(node mfs.FSNode, locked bool) (p9p.Qid, string) {
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

	if !locked {
		s.mu.Lock()
	}
	path := s.nextQidPath
	s.nextQidPath++
	if !locked {
		s.mu.Unlock()
	}

	return p9p.Qid{
		Type:    qtype,
		Version: 0,
		Path:    path,
	}, name
}

func (s *MfsSession) nodeToDir(node mfs.FSNode) (p9p.Dir, error) {
	qid, name := s.nodeToQid(node, false)
	mode := uint32(0644)
	length := uint64(0)

	switch n := node.(type) {
	case *mfs.Directory:
		mode |= p9p.DMDIR
	case *mfs.File:
		size, err := n.Size()
		if err != nil {
			log.Errorf("nodeToDir: error getting size of file %s: %v", name, err)
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

	log.Debugf("nodeToDir: created dir entry for %s", name)

	return dir, nil
}

func (s *MfsSession) readDirEntries(dir *mfs.Directory) ([]mfs.NodeListing, error) {
	var entries []mfs.NodeListing
	err := dir.ForEachEntry(context.TODO(), func(entry mfs.NodeListing) error {
		entries = append(entries, entry)
		log.Debugf("readDirEntries: found entry %s", entry.Name)
		return nil
	})
	if err != nil {
		log.Errorf("readDirEntries: error reading directory %v", err)
	}
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

	log.Debugf("entryToDir: created dir entry for %s", entry.Name)

	return dir
}

func (s *MfsSession) genQidPath() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.nextQidPath
	s.nextQidPath++
	log.Debugf("genQidPath: generated qid path %d", path)
	return path
}

type loggingHandler struct {
	inner p9p.Handler
}

func (l *loggingHandler) Handle(ctx context.Context, msg p9p.Message) (p9p.Message, error) {
	log.Debugf("9P message: %v %s", msg, msg.Type())
	resp, err := l.inner.Handle(ctx, msg)
	if err != nil {
		log.Errorf("9P error: %v", err)
	}
	if resp != nil {
		log.Debugf("9P response: %v %s", resp, resp.Type())
	}

	return resp, err
}

var _ p9p.Handler = (*loggingHandler)(nil)
