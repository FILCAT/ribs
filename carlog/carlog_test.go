package carlog

import (
	"crypto/rand"
	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	"io/fs"
	"path/filepath"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
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

	jb, err := Create(filepath.Join(td, "index"), filepath.Join(td, "data.car"), nil)
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

	_, err = jb.Close()
	require.NoError(t, err)

	jb, err = Open(filepath.Join(td, "index"), filepath.Join(td, "data.car"), nil)
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
	require.NoError(t, jb.Finalize())
	require.NoError(t, jb.DropLevel())
	err = jb.View([]multihash.Multihash{h}, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, []byte("hello world"))
		return nil
	})

	// test open finalized
	_, err = jb.Close()
	require.NoError(t, err)

	jb, err = Open(filepath.Join(td, "index"), filepath.Join(td, "data.car"), nil)
	require.NoError(t, err)

	err = jb.View([]multihash.Multihash{h}, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, []byte("hello world"))
		return nil
	})
	require.NoError(t, err)

	// test interate
	err = jb.Iterate(func(hs cid.Cid, b []byte) error {
		require.Equal(t, b, []byte("hello world"))
		require.Equal(t, h, hs.Hash())
		return nil
	})
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

	jb, err := Create(filepath.Join(td, "index"), filepath.Join(td, "data.car"), nil)
	require.NoError(t, err)

	const numBlocks = 3000
	blockData := make([][]byte, numBlocks)
	mhList := make([]multihash.Multihash, numBlocks)
	blockList := make([]blocks.Block, numBlocks)

	for i := 0; i < numBlocks; i++ {
		blockData[i] = make([]byte, 64)
		_, err := rand.Read(blockData[i])
		require.NoError(t, err)

		b, _ := blocks.NewBlockWithCid(blockData[i], cid.NewCidV0(u.Hash(blockData[i])))
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
	require.NoError(t, jb.Finalize())
	require.NoError(t, jb.DropLevel())

	err = jb.View(mhList, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, blockData[i])
		return nil
	})

	_, err = jb.Close()
	require.NoError(t, err)

	jb, err = Open(filepath.Join(td, "index"), filepath.Join(td, "data.car"), nil)
	require.NoError(t, err)

	err = jb.View(mhList, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, blockData[i])
		return nil
	})
	require.NoError(t, err)
}
