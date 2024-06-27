package rbstor

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	iface "github.com/atboosty/ribs"
	blocks "github.com/ipfs/go-block-format"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	t.Skip()
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

	err = wb.Put(ctx, []blocks.Block{b})
	require.NoError(t, err)

	err = wb.Flush(ctx)
	require.NoError(t, err)

	err = sess.View(ctx, []multihash.Multihash{h}, func(i int, b []byte) {
		require.Equal(t, 0, i)
		require.Equal(t, b, []byte("hello world"))
	})
	require.NoError(t, err)

	require.NoError(t, ri.Close())
}

func TestFullGroup(t *testing.T) {
	t.Skip("worker gate needed to re-enable this test")
	maxGroupSize = 100 << 20

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
	// TODO there is no more worker gate; make this play nice with tests
	// workerGate := make(chan struct{}, 1)
	// ri, err := Open(td, WithWorkerGate(workerGate))
	ri, err := Open(td)
	require.NoError(t, err)

	sess := ri.Session(ctx)

	wb := sess.Batch(ctx)

	var h multihash.Multihash

	for i := 0; i < 500; i++ {
		var blk [200_000]byte
		binary.BigEndian.PutUint64(blk[:], uint64(i))

		b := blocks.NewBlock(blk[:])
		h = b.Cid().Hash()

		err = wb.Put(ctx, []blocks.Block{b})
		require.NoError(t, err)

		err = wb.Flush(ctx)
		require.NoError(t, err)
	}

	gs, err := ri.StorageDiag().GroupMeta(1)
	require.NoError(t, err)
	require.Equal(t, iface.GroupStateFull, gs.State)

	err = sess.View(ctx, []multihash.Multihash{h}, func(i int, b []byte) {
		require.Equal(t, 0, i)
		//require.Equal(t, b, []byte("hello world"))
	})
	require.NoError(t, err)

	//workerGate <- struct{}{} // trigger a worker to run for one cycle

	require.Eventually(t, func() bool {
		gs, err := ri.StorageDiag().GroupMeta(1)
		require.NoError(t, err)
		fmt.Println("state now ", gs.State)
		return gs.State == iface.GroupStateVRCARDone
	}, 10*time.Second, 40*time.Millisecond)

	//workerGate <- struct{}{} // trigger a worker to allow processing close

	/*f, err := os.OpenFile("/tmp/ri.car", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err)
	defer f.Close()

	err = ri.(*rbs).openGroups[1].writeCar(f)
	require.NoError(t, err)*/

	require.NoError(t, ri.Close())
}
