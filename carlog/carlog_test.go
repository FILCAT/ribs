package carlog

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
)

func TestCarLogBasic(t *testing.T) {
	td := t.TempDir()
	t.Cleanup(func() {
		if err := filepath.Walk(td, func(path string, info fs.FileInfo, err error) error {
			t.Log(path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	jb, err := Create(nil, filepath.Join(td, "index"), filepath.Join(td, "data.car"), nil)
	require.NoError(t, err)

	b := blocks.NewBlock([]byte("hello world"))
	h := b.Cid().Hash()

	err = jb.Put([]multihash.Multihash{h}, []blocks.Block{b})
	require.NoError(t, err)

	_, err = jb.Commit()
	require.NoError(t, err)

	err = jb.View([]multihash.Multihash{h}, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, []byte("hello world"))
		return nil
	})
	require.NoError(t, err)

	err = jb.Close()
	require.NoError(t, err)

	noTrunc := func(to int64, h []multihash.Multihash) error {
		require.Fail(t, "not expected")
		return nil
	}

	jb, err = Open(nil, filepath.Join(td, "index"), filepath.Join(td, "data.car"), noTrunc)
	require.NoError(t, err)

	// test that we can read the data back out again
	err = jb.View([]multihash.Multihash{h}, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, []byte("hello world"))
		return nil
	})
	require.NoError(t, err)

	// test finalization
	require.NoError(t, jb.MarkReadOnly())
	require.NoError(t, jb.Finalize(context.TODO()))
	err = jb.View([]multihash.Multihash{h}, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, []byte("hello world"))
		return nil
	})
	require.NoError(t, err)

	// test open finalized
	err = jb.Close()
	require.NoError(t, err)

	jb, err = Open(nil, filepath.Join(td, "index"), filepath.Join(td, "data.car"), noTrunc)
	require.NoError(t, err)

	err = jb.View([]multihash.Multihash{h}, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, []byte("hello world"))
		return nil
	})
	require.NoError(t, err)

	// test interate
	err = jb.iterate(jb.dataEnd, func(off int64, len uint64, hs cid.Cid, b []byte) error {
		require.Equal(t, b, []byte("hello world"))
		require.Equal(t, h, hs.Hash())
		return nil
	})
	require.NoError(t, err)

	// test offload
	require.NoError(t, jb.Offload())
	err = jb.View([]multihash.Multihash{h}, func(i int, found bool, b []byte) error {
		require.Fail(t, "should not be here")
		return nil
	})
	require.ErrorContains(t, err, "cannot read from closing or offloaded carlog")

	s, err := jb.HashSample()
	require.NoError(t, err)
	require.Len(t, s, 1)

	require.NoError(t, jb.Close())
	// test open offloaded
	jb, err = Open(nil, filepath.Join(td, "index"), filepath.Join(td, "data.car"), noTrunc)
	require.NoError(t, err)

	err = jb.View([]multihash.Multihash{h}, func(i int, found bool, b []byte) error {
		require.Fail(t, "should not be here")
		return nil
	})
	require.ErrorContains(t, err, "cannot read from closing or offloaded carlog")

	s, err = jb.HashSample()
	require.NoError(t, err)
	require.Len(t, s, 1)
	require.NoError(t, jb.Close())
}

func TestCarLog3K(t *testing.T) {
	td := t.TempDir()
	t.Cleanup(func() {
		if err := filepath.Walk(td, func(path string, info fs.FileInfo, err error) error {
			t.Log(path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	jb, err := Create(nil, filepath.Join(td, "index"), filepath.Join(td, "data.car"), nil)
	require.NoError(t, err)

	const numBlocks = 3000
	blockData := make([][]byte, numBlocks)
	mhList := make([]multihash.Multihash, numBlocks)
	blockList := make([]blocks.Block, numBlocks)

	for i := 0; i < numBlocks; i++ {
		blockData[i] = make([]byte, 64)
		_, err := rand.Read(blockData[i])
		require.NoError(t, err)

		mh, err := multihash.Sum(blockData[i], multihash.SHA2_256, -1)
		require.NoError(t, err)

		b, _ := blocks.NewBlockWithCid(blockData[i], cid.NewCidV1(cid.Raw, mh))
		mhList[i] = b.Cid().Hash()
		blockList[i] = b
	}

	err = jb.Put(mhList, blockList)
	require.NoError(t, err)

	_, err = jb.Commit()
	require.NoError(t, err)

	err = jb.View(mhList, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, blockData[i])
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, jb.MarkReadOnly())
	require.NoError(t, jb.Finalize(context.TODO()))

	err = jb.View(mhList, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, blockData[i])
		return nil
	})
	require.NoError(t, err)

	err = jb.Close()
	require.NoError(t, err)

	noTrunc := func(to int64, h []multihash.Multihash) error {
		require.Fail(t, "not expected")
		return nil
	}

	jb, err = Open(nil, filepath.Join(td, "index"), filepath.Join(td, "data.car"), noTrunc)
	require.NoError(t, err)

	err = jb.View(mhList, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, blockData[i])
		return nil
	})
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(td, "canon.car"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	_, _, err = jb.WriteCar(f)
	require.NoError(t, err)

	err = jb.Close()
	require.NoError(t, err)

	//require.NoError(t, VerifyCar(filepath.Join(td, "canon.car")))
	//require.NoError(t, VerifyCar(filepath.Join(td, "data.car")))
}

/*
// borrowed from go-car
func VerifyCar(file string) error {
	// header
	rx, err := carv2.OpenReader(file)
	if err != nil {
		return err
	}
	defer rx.Close()
	roots, err := rx.Roots()
	if err != nil {
		return err
	}
	if len(roots) == 0 {
		return fmt.Errorf("no roots listed in car header")
	}
	rootMap := make(map[cid.Cid]struct{})
	for _, r := range roots {
		rootMap[r] = struct{}{}
	}

	fmt.Println("roots", roots)

	if rx.Version != 1 {
		return xerrors.New("expected carv1")
	}

	// blocks
	fd, err := os.Open(file)
	if err != nil {
		return err
	}
	rd, err := carv2.NewBlockReader(fd)
	if err != nil {
		return err
	}

	cidList := make([]cid.Cid, 0)
	for {
		blk, err := rd.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		delete(rootMap, blk.Cid())
		cidList = append(cidList, blk.Cid())
	}

	if len(rootMap) > 0 {
		return fmt.Errorf("header lists root(s) not present as a block: %v", rootMap)
	}

	return nil
}
*/

func TestCarStaging(t *testing.T) {
	td := t.TempDir()
	t.Cleanup(func() {
		if err := filepath.Walk(td, func(path string, info fs.FileInfo, err error) error {
			t.Log(path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	tsp := &testStagingProvider{}

	jb, err := Create(tsp, filepath.Join(td, "index"), filepath.Join(td, "data.car"), nil)
	require.NoError(t, err)

	const numBlocks = 3000
	blockData := make([][]byte, numBlocks)
	mhList := make([]multihash.Multihash, numBlocks)
	blockList := make([]blocks.Block, numBlocks)

	for i := 0; i < numBlocks; i++ {
		blockData[i] = make([]byte, 64)
		_, err := rand.Read(blockData[i])
		require.NoError(t, err)

		mh, err := multihash.Sum(blockData[i], multihash.SHA2_256, -1)
		require.NoError(t, err)

		b, _ := blocks.NewBlockWithCid(blockData[i], cid.NewCidV1(cid.Raw, mh))
		mhList[i] = b.Cid().Hash()
		blockList[i] = b
	}

	err = jb.Put(mhList, blockList)
	require.NoError(t, err)

	_, err = jb.Commit()
	require.NoError(t, err)

	require.NoError(t, jb.MarkReadOnly())

	require.NoError(t, jb.Finalize(context.TODO()))

	err = jb.View(mhList, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, blockData[i])
		return nil
	})
	require.NoError(t, err)

	///////////////////////

	err = jb.Close()
	require.NoError(t, err)

	noTrunc := func(to int64, h []multihash.Multihash) error {
		require.Fail(t, "not expected")
		return nil
	}

	jb, err = Open(tsp, filepath.Join(td, "index"), filepath.Join(td, "data.car"), noTrunc)
	require.NoError(t, err)

	err = jb.View(mhList, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, blockData[i])
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, jb.Offload())

	err = jb.View(mhList, func(i int, found bool, b []byte) error {
		require.Fail(t, "no")
		return nil
	})
	require.Error(t, err)

	err = jb.Close()
	require.NoError(t, err)

	jb, err = Open(tsp, filepath.Join(td, "index"), filepath.Join(td, "data.car"), noTrunc)
	require.NoError(t, err)

	err = jb.View(mhList, func(i int, found bool, b []byte) error {
		require.Fail(t, "no")
		return nil
	})
	require.Error(t, err)

	err = jb.Close()
	require.NoError(t, err)
}

var _ CarStorageProvider = (*testStagingProvider)(nil)

type testStagingProvider struct {
	lk sync.Mutex

	bdata []byte
}

func (t *testStagingProvider) Upload(ctx context.Context, size int64, src func(writer io.Writer) error) error {
	t.lk.Lock()
	defer t.lk.Unlock()

	if len(t.bdata) > 0 {
		return xerrors.New("had data")
	}

	var buf bytes.Buffer

	err := src(&buf)
	if err != nil {
		return err
	}

	t.bdata = buf.Bytes()

	return nil
}

func (t *testStagingProvider) ReadCar(ctx context.Context, off, size int64) (io.ReadCloser, error) {
	t.lk.Lock()
	defer t.lk.Unlock()

	return io.NopCloser(io.LimitReader(bytes.NewReader(t.bdata[off:]), size)), nil
}

func (t *testStagingProvider) ReadAt(p []byte, off int64) (n int, err error) {
	t.lk.Lock()
	defer t.lk.Unlock()
	n = copy(p, t.bdata[off:])
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (t *testStagingProvider) Release(ctx context.Context) error {
	t.lk.Lock()
	defer t.lk.Unlock()

	t.bdata = nil
	return nil
}

func (t *testStagingProvider) URL(ctx context.Context) (string, error) {
	return "http://aaaaaaa", nil
}

var _ CarStorageProvider = &testStagingProvider{}
