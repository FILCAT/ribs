package rbstor

import (
	"context"
	"crypto/rand"
	"math/big"
	"os"
	"testing"

	iface "github.com/atboosty/ribs"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestPebbleIndex(t *testing.T) {
	idx, err := NewPebbleIndex(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, idx.Close())
	})

	mhs, sizes := genMhashList(t, 10)
	testGroup := iface.GroupKey(2)

	err = idx.AddGroup(context.Background(), mhs, sizes, testGroup)
	require.NoError(t, err)

	result := map[int][]iface.GroupKey{}
	err = idx.GetGroups(context.Background(), mhs, func(cidx int, group iface.GroupKey) (bool, error) {
		result[cidx] = append(result[cidx], group)
		return true, nil
	})
	require.NoError(t, err)

	for _, groupKeys := range result {
		require.Contains(t, groupKeys, testGroup)
	}

	err = idx.DropGroup(context.Background(), mhs, testGroup)
	require.NoError(t, err)

	err = idx.Sync(context.Background())
	require.NoError(t, err)

	err = idx.GetGroups(context.Background(), mhs, func(cidx int, group iface.GroupKey) (bool, error) {
		require.NotEqual(t, testGroup, group, "g %d should have been dropped", testGroup)
		return true, nil
	})
	require.NoError(t, err)
}

func TestMultipleGroupsPerHash(t *testing.T) {
	idx, err := NewPebbleIndex(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, idx.Close())
	})

	mhs, sizes := genMhashList(t, 10)
	group1 := iface.GroupKey(2)
	group2 := iface.GroupKey(3)

	err = idx.AddGroup(context.Background(), mhs, sizes, group1)
	require.NoError(t, err)

	err = idx.AddGroup(context.Background(), mhs, sizes, group2)
	require.NoError(t, err)

	result := map[int][]iface.GroupKey{}
	err = idx.GetGroups(context.Background(), mhs, func(cidx int, group iface.GroupKey) (bool, error) {
		result[cidx] = append(result[cidx], group)
		return true, nil
	})
	require.NoError(t, err)

	for _, groupKeys := range result {
		require.Contains(t, groupKeys, group1)
		require.Contains(t, groupKeys, group2)
	}

	err = idx.DropGroup(context.Background(), mhs, group1)
	require.NoError(t, err)

	err = idx.GetGroups(context.Background(), mhs, func(cidx int, group iface.GroupKey) (bool, error) {
		require.NotEqual(t, group1, group, "g %d should have been dropped", group1)
		require.Equal(t, group2, group, "g %d should not have been dropped", group2)
		return false, nil
	})
	require.NoError(t, err)
}

func genMhashList(t testing.TB, count int) ([]multihash.Multihash, []int32) {
	const maxSize = 1 << 20 // 1 MiB
	maxSizeBigInt := big.NewInt(maxSize)
	mhashes := make([]multihash.Multihash, count)
	sizes := make([]int32, count)
	for i := 0; i < count; i++ {
		buf := make([]byte, 32)
		_, err := rand.Read(buf)
		if err != nil {
			panic(err)
		}
		mhash, err := multihash.Sum(buf, multihash.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		mhashes[i] = mhash
		size, err := rand.Int(rand.Reader, maxSizeBigInt)
		require.NoError(t, err)
		sizes[i] = int32(size.Int64())
	}
	return mhashes, sizes
}

func BenchmarkAddGet16k(b *testing.B) {
	tdir := b.TempDir()

	idx, err := NewPebbleIndex(tdir)
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		require.NoError(b, idx.Close())
	})

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		mhs, sizes := genMhashList(b, 16_000)
		group := iface.GroupKey(n)
		b.StartTimer()

		err = idx.AddGroup(context.Background(), mhs, sizes, group)
		if err != nil {
			b.Fatal(err)
		}

		result := map[int][]iface.GroupKey{}
		err = idx.GetGroups(context.Background(), mhs, func(cidx int, group iface.GroupKey) (bool, error) {
			result[cidx] = append(result[cidx], group)
			return false, nil
		})
		if err != nil {
			b.Fatal(err)
		}

		for _, groupKeys := range result {
			if len(groupKeys) != 1 || groupKeys[0] != group {
				b.Fatalf("expected group %v, got %v", group, groupKeys)
			}
		}
	}
}

func BenchmarkGetSingleHash100k(b *testing.B) {
	tdir := b.TempDir()

	require.NoError(b, os.MkdirAll(tdir, 0755))

	idx, err := NewPebbleIndex(tdir)
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		require.NoError(b, idx.Close())
	})

	// Prepare the database with 100,000 entries
	mhs, sizes := genMhashList(b, 1_000_000)
	for i, mh := range mhs {
		group := iface.GroupKey(i)
		err = idx.AddGroup(context.Background(), []multihash.Multihash{mh}, []int32{sizes[i]}, group)
		if err != nil {
			b.Fatal(err)
		}
	}

	// Run the benchmark for individual GetGroups calls for single hashes
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		hashIndex := n % len(mhs) // Cycle through the hashes
		expectedGroup := iface.GroupKey(hashIndex)

		result := map[int][]iface.GroupKey{}
		err = idx.GetGroups(context.Background(), []multihash.Multihash{mhs[hashIndex]}, func(cidx int, group iface.GroupKey) (bool, error) {
			result[cidx] = append(result[cidx], group)
			return false, nil
		})
		if err != nil {
			b.Fatal(err)
		}

		groupKeys := result[0]
		if len(groupKeys) != 1 || groupKeys[0] != expectedGroup {
			b.Fatalf("expected group %v, got %v", expectedGroup, groupKeys)
		}
	}
}

func BenchmarkAddSingleHash100k(b *testing.B) {
	tdir := b.TempDir()

	idx, err := NewPebbleIndex(tdir)
	if err != nil {
		b.Fatal(err)
	}

	mhs, sizes := genMhashList(b, 100_000)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		hashIndex := n % len(mhs)            // Cycle through the hashes
		group := iface.GroupKey(100_000 + n) // Use new groups for each AddGroup call
		b.StartTimer()

		err = idx.AddGroup(context.Background(), []multihash.Multihash{mhs[hashIndex]}, []int32{sizes[hashIndex]}, group)
		if err != nil {
			b.Fatal(err)
		}
	}
}
