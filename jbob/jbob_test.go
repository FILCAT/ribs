package jbob

import (
	"io/fs"
	"path/filepath"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestJbobBasic(t *testing.T) {
	td := t.TempDir()
	t.Cleanup(func() {
		if err := filepath.Walk(td, func(path string, info fs.FileInfo, err error) error {
			t.Log(path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	jb, err := Create(filepath.Join(td, "index"), filepath.Join(td, "data"))
	require.NoError(t, err)

	b := blocks.NewBlock([]byte("hello world"))
	h := b.Cid().Hash()

	err = jb.Put([]multihash.Multihash{h}, [][]byte{b.RawData()})
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

	jb, err = Open(filepath.Join(td, "index"), filepath.Join(td, "data"))
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

	jb, err = Open(filepath.Join(td, "index"), filepath.Join(td, "data"))
	require.NoError(t, err)

	err = jb.View([]multihash.Multihash{h}, func(i int, found bool, b []byte) error {
		require.True(t, found)
		require.Equal(t, b, []byte("hello world"))
		return nil
	})
	require.NoError(t, err)
}
