package impl

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/transport/types"
	"github.com/gbrlsnchs/jwt/v3"
	gostream "github.com/libp2p/go-libp2p-gostream"
	"github.com/libp2p/go-libp2p/core/host"
	iface "github.com/lotus-web3/ribs"
	"golang.org/x/xerrors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

func (r *ribs) setupCarServer(ctx context.Context, host host.Host) error {
	// todo protect incoming streams

	listener, err := gostream.Listen(host, types.DataTransferProtocol)
	if err != nil {
		return fmt.Errorf("starting gostream listener: %w", err)
	}

	handler := http.NewServeMux()
	handler.HandleFunc("/", r.handleCarRequest)
	server := &http.Server{
		Handler: handler, // todo gzip handler assuming that it works with boost
		// This context will be the parent of the context associated with all
		// incoming requests
		BaseContext: func(listener net.Listener) context.Context {
			return ctx
		},
	}
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Errorw("car server failed to start", "error", err)
			return
		}
	}()

	go r.carStatsWorker(ctx)

	// todo also serve tcp

	return nil
}

func (r *ribs) carStatsWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond * 250):
			r.updateCarStats()
		}
	}
}

func (r *ribs) updateCarStats() {
	r.uploadStatsLk.Lock()
	defer r.uploadStatsLk.Unlock()

	r.uploadStatsSnap = make(map[iface.GroupKey]*iface.UploadStats)
	for k, v := range r.uploadStats {
		r.uploadStatsSnap[k] = &iface.UploadStats{
			ActiveRequests:       v.ActiveRequests,
			Last250MsUploadBytes: atomic.SwapInt64(&v.Last250MsUploadBytes, 0),
		}
	}

	for k, v := range r.uploadStats {
		if v.ActiveRequests == 0 {
			delete(r.uploadStats, k)
		}
	}
}

func (r *ribs) CarUploadStats() map[iface.GroupKey]*iface.UploadStats {
	r.uploadStatsLk.Lock()
	defer r.uploadStatsLk.Unlock()

	return r.uploadStatsSnap
}

type carStatWriter struct {
	ctr *int64
	w   io.Writer
}

func (c *carStatWriter) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	atomic.AddInt64(c.ctr, int64(n))
	return
}

var jwtKey = func() *jwt.HMACSHA { // todo generate / store
	return jwt.NewHS256([]byte("this is super safe"))
}()

type carRequestToken struct {
	Group   int64
	Timeout int64
}

func (r *ribs) verify(ctx context.Context, token string) (carRequestToken, error) {
	var payload carRequestToken
	if _, err := jwt.Verify([]byte(token), jwtKey, &payload); err != nil {
		return carRequestToken{}, xerrors.Errorf("JWT Verification failed: %w", err)
	}

	if payload.Timeout < time.Now().Unix() {
		return carRequestToken{}, xerrors.Errorf("token expired")
	}

	return payload, nil
}

func (r *ribs) makeCarRequestToken(ctx context.Context, group int64, timeout time.Duration) ([]byte, error) {
	p := carRequestToken{
		Group:   group,
		Timeout: time.Now().Add(timeout).Unix(),
	}

	return jwt.Sign(&p, jwtKey)
}

func (r *ribs) handleCarRequest(w http.ResponseWriter, req *http.Request) {
	if req.Header.Get("Authorization") == "" {
		log.Errorw("car request auth: no auth header", "url", req.URL)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	reqToken, err := r.verify(req.Context(), req.Header.Get("Authorization"))
	if err != nil {
		log.Errorw("car request auth: failed to verify token", "error", err, "url", req.URL)
		http.Error(w, xerrors.Errorf("car request auth: %w", err).Error(), http.StatusUnauthorized)
		return
	}

	r.uploadStatsLk.Lock()
	if r.uploadStats[reqToken.Group] == nil {
		r.uploadStats[reqToken.Group] = &iface.UploadStats{}
	}

	r.uploadStats[reqToken.Group].ActiveRequests++

	sw := &carStatWriter{
		ctr: &r.uploadStats[reqToken.Group].Last250MsUploadBytes,
		w:   w,
	}

	r.uploadStatsLk.Unlock()

	defer func() {
		r.uploadStatsLk.Lock()
		r.uploadStats[reqToken.Group].ActiveRequests--
		r.uploadStatsLk.Unlock()
	}()

	err = r.withReadableGroup(reqToken.Group, func(group *Group) error {
		// todo range request handling
		//http.ServeContent(w, req, "deal.car", time.Now(), &carWriter{})

		_, _, err := group.writeCar(sw)
		return err
	})
	if err != nil {
		log.Errorw("car request: write car", "error", err, "url", req.URL)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
