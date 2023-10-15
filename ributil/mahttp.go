package ributil

import (
	"github.com/ipni/go-libipni/maurl"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"
	"net/url"
)

func MaddrsToUrl(addrs []multiaddr.Multiaddr) (*url.URL, error) {
	var err error
	var url *url.URL
	for _, addr := range addrs {
		url, err = maurl.ToURL(addr)
		if err == nil && url != nil {
			return url, nil
		}
	}
	if err == nil && url == nil {
		return nil, xerrors.New("no valid multiaddrs")
	}
	// we have to do this because we get ws and wss from maurl.ToURL
	if url != nil && !(url.Scheme == "http" || url.Scheme == "https") {
		return nil, xerrors.New("no valid HTTP multiaddrs")
	}
	return url, err
}
