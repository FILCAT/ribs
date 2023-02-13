package impl

import (
	iface "github.com/lotus-web3/ribs"
)

func (r *ribs) Diagnostics() iface.Diag {
	return r
}

func (r *ribs) Groups() ([]iface.GroupKey, error) {
	return r.db.Groups()
}

func (r *ribs) GroupMeta(gk iface.GroupKey) (iface.GroupMeta, error) {
	return r.db.GroupMeta(gk)
}

func (r *ribs) CrawlState() string {
	return *r.crawlState.Load()
}

func (r *ribs) ReachableProviders() []iface.ProviderMeta {
	return r.db.ReachableProviders()
}
