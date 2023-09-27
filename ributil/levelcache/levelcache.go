package levelcache

import "github.com/multiformats/go-multihash"

type LevelCache struct {
	// mlru group

	// persistent top level lru

	// levels, with, at least for now, one logcache+mlru per level
}

func (c *LevelCache) Put(k multihash.Multihash, v []byte) error {

}

func (c *LevelCache) Get(k multihash.Multihash, cb func([]byte) error) error {

}
