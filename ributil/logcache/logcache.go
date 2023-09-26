package logcache

import (
	"bufio"
	"os"
	"sync"

	"github.com/multiformats/go-multihash"
)

const (
	carExt = ".car"
	idxExt = ".offs"
)

type mhStr string

type LogCache struct {
	carFile os.File

	// index file
	// [[i24:dataLen][i48:offset]:i64][mhLen:i8][multihash]...
	indexFile os.File

	carBuf, idxBuf bufio.ReaderAt

	indexLk sync.Mutex
	index   map[mhStr]int64

	// entirely separate write mutex because reads happen on read-only data
	read  sync.RWMutex
	write sync.Mutex
}

func Open(basePath string) (*LogCache, error) {
	// open car file

	// open index file

	// load index file into index

	// check that last index file points to a valid car entry, and that there's no
	// data at the end of carFile
	// - if there is trailer data in the car, truncate it
	// - if the index points beyond the end of the car file, scan the index for the
	//   last entry which is within the car, truncate the index, then read the car (if last entry is truncated, do truncate it)
}

// Put returns an error if the multihash is already stored in the cache, otherwise
// it appends the entry to the cache car, then appends an index entry
func (lc *LogCache) Put(mh multihash.Multihash, data []byte) (err error) {

}

func (lc *LogCache) Get(mh multihash.Multihash, cb func([]byte) error) error {

}

func (lc *LogCache) Flush() error {

}
