package impl

import (
	"context"
	"crypto/rand"
	"os"
	"testing"

	iface "github.com/lotus-web3/ribs"
	"github.com/stretchr/testify/require"

	"github.com/multiformats/go-multihash"
)

func TestPebbleIndex(t *testing.T) {
	tdir := t.TempDir()

	idx, err := NewPebbleIndex(tdir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, idx.Close())
	})

	mhs := genMhashList(10)
	group := iface.GroupKey(2)

	err = idx.AddGroup(context.Background(), mhs, group)
	require.NoError(t, err)

	var result [][]iface.GroupKey
	err = idx.GetGroups(context.Background(), mhs, func(groups [][]iface.GroupKey) (bool, error) {
		result = groups
		return false, nil
	})
	require.NoError(t, err)

	for _, groupKeys := range result {
		require.Contains(t, groupKeys, group)
	}

	err = idx.DropGroup(context.Background(), mhs, group)
	require.NoError(t, err)

	err = idx.Sync(context.Background())
	require.NoError(t, err)

	err = idx.GetGroups(context.Background(), mhs, func(groups [][]iface.GroupKey) (bool, error) {
		for i, groupKeys := range groups {
			require.NotContains(t, groupKeys, group, "g %d should have been dropped", i)
		}
		return false, nil
	})
	require.NoError(t, err)
}

func TestMultipleGroupsPerHash(t *testing.T) {
	tdir := t.TempDir()

	idx, err := NewPebbleIndex(tdir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, idx.Close())
	})

	mhs := genMhashList(10)
	group1 := iface.GroupKey(2)
	group2 := iface.GroupKey(3)

	err = idx.AddGroup(context.Background(), mhs, group1)
	require.NoError(t, err)

	err = idx.AddGroup(context.Background(), mhs, group2)
	require.NoError(t, err)

	var result [][]iface.GroupKey
	err = idx.GetGroups(context.Background(), mhs, func(groups [][]iface.GroupKey) (bool, error) {
		result = groups
		return false, nil
	})
	require.NoError(t, err)

	for _, groupKeys := range result {
		require.Contains(t, groupKeys, group1)
		require.Contains(t, groupKeys, group2)
	}

	err = idx.DropGroup(context.Background(), mhs, group1)
	require.NoError(t, err)

	err = idx.GetGroups(context.Background(), mhs, func(groups [][]iface.GroupKey) (bool, error) {
		for _, groupKeys := range groups {
			require.NotContains(t, groupKeys, group1)
			require.Contains(t, groupKeys, group2)
		}
		return false, nil
	})
	require.NoError(t, err)
}

func genMhashList(count int) []multihash.Multihash {
	mhashes := make([]multihash.Multihash, count)
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
	}
	return mhashes
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
		mhs := genMhashList(16_000)
		group := iface.GroupKey(n)
		b.StartTimer()

		err = idx.AddGroup(context.Background(), mhs, group)
		if err != nil {
			b.Fatal(err)
		}

		var result [][]iface.GroupKey
		err = idx.GetGroups(context.Background(), mhs, func(groups [][]iface.GroupKey) (bool, error) {
			result = groups
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
	mhs := genMhashList(1_000_000)
	for i, mh := range mhs {
		group := iface.GroupKey(i)
		err = idx.AddGroup(context.Background(), []multihash.Multihash{mh}, group)
		if err != nil {
			b.Fatal(err)
		}
	}

	// Run the benchmark for individual GetGroups calls for single hashes
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		hashIndex := n % len(mhs) // Cycle through the hashes
		expectedGroup := iface.GroupKey(hashIndex)

		var result [][]iface.GroupKey
		err = idx.GetGroups(context.Background(), []multihash.Multihash{mhs[hashIndex]}, func(groups [][]iface.GroupKey) (bool, error) {
			result = groups
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

	mhs := genMhashList(100_000)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		hashIndex := n % len(mhs)            // Cycle through the hashes
		group := iface.GroupKey(100_000 + n) // Use new groups for each AddGroup call
		b.StartTimer()

		err = idx.AddGroup(context.Background(), []multihash.Multihash{mhs[hashIndex]}, group)
		if err != nil {
			b.Fatal(err)
		}
	}
}
