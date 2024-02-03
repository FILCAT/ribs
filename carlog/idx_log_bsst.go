package carlog

import (
	mh "github.com/multiformats/go-multihash"
)

type LogBsstIndex struct {
}

func (l *LogBsstIndex) Put(c []mh.Multihash, offs []int64) error {
	// append to current log
	
	// if log is too big, create new log, schedule compaction for old log

	//TODO implement me
	panic("implement me")
}

func (l *LogBsstIndex) Del(c []mh.Multihash) error {
	//TODO implement me
	panic("implement me")
}

func (l *LogBsstIndex) ToTruncate(atOrAbove int64) ([]mh.Multihash, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LogBsstIndex) Close() error {
	//TODO implement me
	panic("implement me")
}

var _ WritableIndex = (*LogBsstIndex)(nil)
