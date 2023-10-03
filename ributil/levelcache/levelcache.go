package levelcache

import (
	"github.com/lotus-web3/ribs/ributil/logcache"
	"github.com/lotus-web3/ribs/ributil/mlru"
	"github.com/multiformats/go-multihash"
	"sync"
)

type mhStr string

type LevelCache struct {
	// root dir
	// Contents:
	// /l0.lru
	// /l0.car
	// /l0.offs
	// ...
	root string

	// settings
	l0size         int64
	levelExpansion int

	// mlru group
	lrugroup *mlru.LRUGroup

	// persistent top level lru
	// (the top layer is special because it's the only actively mutated layer)
	topLru *mlru.MLRU[mhStr, bool]
	topLog *logcache.LogCache

	// levels, with, at least for now, one logcache+mlru per level
	levels []*cacheLevel

	// compaction stuff
	compactLk sync.RWMutex

	// todo mem tier
	// temp memory cache used during compaction
	compactCache map[mhStr][]byte
}

type cacheLevel struct {
	lru *mlru.MLRU[mhStr, bool]
	log *logcache.LogCache
}

func (c *LevelCache) compactTop() error {
	c.compactLk.Lock()
	defer c.compactLk.Unlock()

	c.levels[0].lru.EvictStats()
}

func (c *LevelCache) Put(k multihash.Multihash, v []byte) error {

}

func (c *LevelCache) Get(k multihash.Multihash, cb func([]byte) error) error {

}
