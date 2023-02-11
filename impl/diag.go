package impl

import (
	iface "github.com/lotus-web3/ribs"
)

func (r *ribs) Diagnostics() iface.Diag {
	return r
}

func (r *ribs) Groups() ([]iface.GroupKey, error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	return r.db.Groups()
}

func (r *ribs) GroupMeta(gk iface.GroupKey) (iface.GroupMeta, error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	return r.db.GroupMeta(gk)
}
