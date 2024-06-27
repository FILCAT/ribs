package rbdeal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	iface "github.com/atboosty/ribs"
	"github.com/atboosty/ribs/ributil"
	types "github.com/atboosty/ribs/ributil/boosttypes"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	gostream "github.com/libp2p/go-libp2p-gostream"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/xerrors"
)

var bootTime = time.Now()

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

	r.uploadStatsSnap = make(map[iface.GroupKey]*iface.GroupUploadStats)
	for k, v := range r.uploadStats {
		r.uploadStatsSnap[k] = &iface.GroupUploadStats{
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

func (r *ribs) CarUploadStats() iface.UploadStats {
	lastTotalBytes, err := r.db.LastTotalUploadedBytes()
	if err != nil {
		log.Errorw("getting last total uploaded bytes", "error", err)
	}

	r.uploadStatsLk.Lock()
	defer r.uploadStatsLk.Unlock()

	return iface.UploadStats{
		ByGroup:        r.uploadStatsSnap,
		LastTotalBytes: lastTotalBytes,
	}
}

type carStatWriter struct {
	groupCtr *int64
	wrote    int64

	w         io.Writer
	toDiscard int64
}

func (c *carStatWriter) Write(p []byte) (n int, err error) {
	defer func() {
		c.wrote += int64(n)
	}()

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
	atomic.AddInt64(c.groupCtr, int64(n))
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

	DealUUID uuid.UUID
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

func (r *ribs) makeCarRequestToken(ctx context.Context, group int64, timeout time.Duration, carSize int64, deal uuid.UUID) ([]byte, error) {
	p := carRequestToken{
		Group:    group,
		Timeout:  time.Now().Add(timeout).Unix(),
		CarSize:  carSize,
		DealUUID: deal,
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

	log := log.With("peer", pid, "deal", reqToken.DealUUID)

	// Protect the libp2p connection for the lifetime of the transfer
	tag := uuid.New().String()
	r.host.ConnManager().Protect(pid, tag)
	defer r.host.ConnManager().Unprotect(pid, tag)

	var toDiscard int64
	var toLimit int64 = -1 // -1 means no limit, read to the end

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
			toLimit, err = strconv.ParseInt(s2[1], 10, 64)
			if err != nil {
				log.Errorw("invalid content range (5)", "range", req.Header.Get("Content-Range"), "s2", s2)
				http.Error(w, "invalid content range", http.StatusBadRequest)
				return
			}
		}
	}

	gm, err := r.RBS.StorageDiag().GroupMeta(reqToken.Group)
	if err != nil {
		log.Errorw("car request: group meta", "error", err, "url", req.URL)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if gm.DealCarSize == nil {
		log.Errorw("car request: no deal car size", "url", req.URL)
		http.Error(w, "no deal car size", http.StatusInternalServerError)
		return
	}

	// todo run more checks here?

	s3u, err := r.maybeGetS3URL(reqToken.Group)
	if err != nil {
		log.Errorw("car request: s3 url", "error", err, "url", req.URL)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if s3u != "" {
		// in s3, redirect
		log.Errorw("car request: redir to s3 url", "error", err, "url", s3u)

		r.s3Redirects.Add(1)

		http.Redirect(w, req, s3u, http.StatusFound)
		return
	}

	// this is a local transfer, track stats

	r.uploadStatsLk.Lock()
	if _, found := r.activeUploads[reqToken.DealUUID]; found {
		http.Error(w, "transfer for deal already ongoing", http.StatusTooManyRequests)
		r.uploadStatsLk.Unlock()
		return
	}

	r.activeUploads[reqToken.DealUUID] = struct{}{}

	if r.uploadStats[reqToken.Group] == nil {
		r.uploadStats[reqToken.Group] = &iface.GroupUploadStats{}
	}

	r.uploadStats[reqToken.Group].ActiveRequests++

	sw := &carStatWriter{
		groupCtr:  &r.uploadStats[reqToken.Group].UploadBytes,
		w:         w,
		toDiscard: toDiscard,
	}

	r.uploadStatsLk.Unlock()

	defer func() {
		r.uploadStatsLk.Lock()
		delete(r.activeUploads, reqToken.DealUUID)
		r.uploadStats[reqToken.Group].ActiveRequests--
		r.uploadStatsLk.Unlock()
	}()

	transferInfo, err := r.db.GetTransferStatusByDealUUID(reqToken.DealUUID)
	if err != nil {
		log.Errorw("car request: get transfer status by deal uuid", "error", err, "url", req.URL)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if transferInfo.Failed == 1 {
		http.Error(w, "deal is failed", http.StatusGone)
		return
	}

	if transferInfo.CarTransferAttempts >= maxTransferRetries {
		if err := r.db.UpdateTransferStats(reqToken.DealUUID, sw.wrote, xerrors.Errorf("transfer has been retried too much")); err != nil {
			log.Errorw("car request: update transfer stats", "error", err, "url", req.URL)
			return
		}

		http.Error(w, "transfer has been retried too much", http.StatusTooManyRequests)
		return
	}

	startTime := DerefOr(transferInfo.CarTransferStartTime, 0)

	if time.Since(bootTime) > transferIdleTimeout && startTime > 0 {
		if time.Since(time.Unix(DerefOr(transferInfo.CarTransferLastEndTime, 0), 0)) > transferIdleTimeout {
			if err := r.db.UpdateTransferStats(reqToken.DealUUID, sw.wrote, xerrors.Errorf("transfer not restarted for too long")); err != nil {
				log.Errorw("car request: update transfer stats", "error", err, "url", req.URL)
				return
			}

			http.Error(w, "transfer not restarted for too long", http.StatusGone)
			return
		}

		// if the transfer was started already, and going on for a while, check the speed
		elapsedTime := time.Since(time.Unix(startTime, 0))
		transferredBytes := DerefOr(transferInfo.CarTransferLastBytes, 0)
		transferSpeedMbps := float64(transferredBytes*8) / 1e6 / elapsedTime.Seconds()

		if transferSpeedMbps < float64(minTransferMbps) {
			log.Errorw("car request: transfer speed too slow", "url", req.URL, "speed", transferSpeedMbps, "deal", reqToken.DealUUID, "group", reqToken.Group)
			http.Error(w, "transfer speed too slow", http.StatusGone)
			return
		}
	}

	rc := r.rateCounters.Get(pid)
	rateWriter := ributil.NewRateEnforcingWriter(sw, rc, transferIdleTimeout)
	defer rateWriter.Done()

	respLen := *gm.DealCarSize - toDiscard
	if toLimit != -1 {
		respLen = toLimit - toDiscard + 1
	}

	w.Header().Set("Content-Length", strconv.FormatInt(respLen, 10))
	w.Header().Set("Content-Type", "application/vnd.ipld.car")

	// limit writer in case we have a range request
	var errLimitReached = errors.New("byte limit reached")
	var writerToUse io.Writer = rateWriter

	if toLimit != -1 {
		writerToUse = &LimitWriter{
			W:   rateWriter,
			N:   toLimit - toDiscard + 1,
			Err: errLimitReached,
		}
	}

	err = r.RBS.Storage().ReadCar(req.Context(), reqToken.Group, func(int64) {}, writerToUse)

	defer func() {
		if err := r.db.UpdateTransferStats(reqToken.DealUUID, sw.wrote, rateWriter.WriteError()); err != nil {
			log.Errorw("car request: update transfer stats", "error", err, "url", req.URL)
			return
		}
	}()

	if err != nil {
		log.Errorw("car request: write car", "error", err, "url", req.URL, "group", reqToken.Group, "deal", reqToken.DealUUID, "remote", req.RemoteAddr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type LimitWriter struct {
	W       io.Writer
	N       int64
	Err     error
	Reached bool // flag to indicate whether the limit has been reached
}

func (lw *LimitWriter) Write(p []byte) (n int, err error) {
	if lw.Reached {
		return 0, lw.Err
	}
	if lw.N <= 0 {
		lw.Reached = true
		return 0, lw.Err
	}
	if int64(len(p)) > lw.N {
		p = p[0:lw.N]
		lw.Reached = true
	}
	n, err = lw.W.Write(p)
	lw.N -= int64(n)
	if lw.N <= 0 {
		lw.Reached = true
		lw.Err = err
	}
	return
}
