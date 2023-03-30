package impl

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/transport/types"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	gostream "github.com/libp2p/go-libp2p-gostream"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	iface "github.com/lotus-web3/ribs"
	"golang.org/x/xerrors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
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
			ActiveRequests: v.ActiveRequests,
			UploadBytes:    atomic.LoadInt64(&v.UploadBytes),
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
	ctr       *int64
	w         io.Writer
	toDiscard int64
}

func (c *carStatWriter) Write(p []byte) (n int, err error) {
	var discarded int64
	if c.toDiscard > 0 {
		if int64(len(p)) <= c.toDiscard {
			c.toDiscard -= int64(len(p))
			return len(p), nil
		}
		p = p[c.toDiscard:]
		discarded = c.toDiscard
		c.toDiscard = 0
	}
	n, err = c.w.Write(p)
	atomic.AddInt64(c.ctr, int64(n))
	n += int(discarded)
	return
}

var jwtKey = func() *jwt.HMACSHA { // todo generate / store
	return jwt.NewHS256([]byte("this is super safe"))
}()

type carRequestToken struct {
	Group   int64
	Timeout int64
	CarSize int64

	// todo SP
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

func (r *ribs) makeCarRequestToken(ctx context.Context, group int64, timeout time.Duration, carSize int64) ([]byte, error) {
	p := carRequestToken{
		Group:   group,
		Timeout: time.Now().Add(timeout).Unix(),
		CarSize: carSize,
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

	pid, err := peer.Decode(req.RemoteAddr)
	if err != nil {
		log.Infow("data transfer request failed: parsing remote address as peer ID",
			"remote-addr", req.RemoteAddr, "err", err)
		http.Error(w, "Failed to parse remote address '"+req.RemoteAddr+"' as peer ID", http.StatusBadRequest)
		return
	}

	// Protect the libp2p connection for the lifetime of the transfer
	tag := uuid.New().String()
	r.host.ConnManager().Protect(pid, tag)
	defer r.host.ConnManager().Unprotect(pid, tag)

	var toDiscard int64
	if req.Header.Get("Range") != "" {
		s1 := strings.Split(req.Header.Get("Range"), "=")
		if len(s1) != 2 {
			log.Errorw("invalid content range (1)", "range", req.Header.Get("Content-Range"), "s1", s1)
			http.Error(w, "invalid content range", http.StatusBadRequest)
			return
		}
		if !strings.HasPrefix(s1[0], "bytes") {
			log.Errorw("invalid content range (2)", "range", req.Header.Get("Content-Range"), "s1", s1)
			http.Error(w, "invalid content range", http.StatusBadRequest)
			return
		}

		s2 := strings.Split(s1[1], "-")
		if len(s2) != 2 {
			log.Errorw("invalid content range (3)", "range", req.Header.Get("Content-Range"), "s2", s2)
			http.Error(w, "invalid content range", http.StatusBadRequest)
			return
		}

		toDiscard, err = strconv.ParseInt(s2[0], 10, 64)
		if err != nil {
			log.Errorw("invalid content range (4)", "range", req.Header.Get("Content-Range"), "s2", s2)
			http.Error(w, "invalid content range", http.StatusBadRequest)
			return
		}

		if s2[1] != "" {
			log.Errorw("invalid content range (5)", "range", req.Header.Get("Content-Range"), "s2", s2)
			http.Error(w, "invalid content range", http.StatusBadRequest)
			return
		}
	}

	r.uploadStatsLk.Lock()
	if r.uploadStats[reqToken.Group] == nil {
		r.uploadStats[reqToken.Group] = &iface.UploadStats{}
	}

	r.uploadStats[reqToken.Group].ActiveRequests++

	sw := &carStatWriter{
		ctr:       &r.uploadStats[reqToken.Group].UploadBytes,
		w:         w,
		toDiscard: toDiscard,
	}

	r.uploadStatsLk.Unlock()

	defer func() {
		r.uploadStatsLk.Lock()
		r.uploadStats[reqToken.Group].ActiveRequests--
		r.uploadStatsLk.Unlock()
	}()

	err = r.withReadableGroup(context.TODO(), reqToken.Group, func(group *Group) error {
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
