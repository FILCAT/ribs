package ributil

import (
	"sync"
	"time"
)

type CachedQuery[T any] struct {
	cached T

	lk sync.Mutex

	refreshInterval time.Duration
	lastRefresh     time.Time

	refreshFunc func() (T, error)
}

func NewCachedQuery[T any](refreshInterval time.Duration, refreshFunc func() (T, error)) *CachedQuery[T] {
	return &CachedQuery[T]{
		refreshInterval: refreshInterval,
		refreshFunc:     refreshFunc,
	}
}

func (cq *CachedQuery[T]) Get() (T, error) {
	cq.lk.Lock()
	defer cq.lk.Unlock()

	if time.Since(cq.lastRefresh) > cq.refreshInterval {
		var err error
		cq.cached, err = cq.refreshFunc()
		if err != nil {
			return cq.cached, err
		}
		cq.lastRefresh = time.Now()
	}

	return cq.cached, nil
}
