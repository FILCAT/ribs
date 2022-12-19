package impl

import (
	"context"
	blocks "github.com/ipfs/go-block-format"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"io/fs"
	"path/filepath"
	"testing"
)

func TestBasic(t *testing.T) {
	td := t.TempDir()
	t.Cleanup(func() {
		if err := filepath.Walk(td, func(path string, info fs.FileInfo, err error) error {
			t.Log(path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	ctx := context.Background()

	ri, err := Open(td)
	require.NoError(t, err)

	sess := ri.Session(ctx)

	wb := sess.Batch(ctx)

	b := blocks.NewBlock([]byte("hello world"))
	h := b.Cid().Hash()

	err = wb.Put(ctx, []multihash.Multihash{h}, [][]byte{b.RawData()})
	require.NoError(t, err)

	err = wb.Flush(ctx)
	require.NoError(t, err)

	err = sess.View(ctx, []multihash.Multihash{h}, func(i int, b []byte) {
		require.Equal(t, 0, i)
		require.Equal(t, b, []byte("hello world"))
	})
	require.NoError(t, err)
}
