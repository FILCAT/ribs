package rbdeal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	gostream "github.com/libp2p/go-libp2p-gostream"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/ributil"
	types "github.com/lotus-web3/ribs/ributil/boosttypes"
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

func (r *ribs) makeCarRequestToken(group int64, timeout time.Duration, carSize int64, deal uuid.UUID) ([]byte, error) {
	p := carRequestToken{
		Group:    group,
		Timeout:  time.Now().Add(timeout).Unix(),
		CarSize:  carSize,
		DealUUID: deal,
	}

	return jwt.Sign(&p, jwtKey)
}

func (r *ribs) makeCarRequest(group int64, timeout time.Duration, carSize int64, deal uuid.UUID) (types.Transfer, error) {
	reqToken, err := r.makeCarRequestToken(group, timeout, carSize, deal)
	if err != nil {
		return types.Transfer{}, xerrors.Errorf("make car request token: %w", err)
	}

	prefAddrs := getPreferredAddrs(r.host)

	transferParams := &types.HttpRequest{URL: "libp2p://" + prefAddrs[0].String() + "/p2p/" + r.host.ID().String()} // todo get from autonat / config
	transferParams.Headers = map[string]string{
		"Authorization": string(reqToken),
	}

	paramsBytes, err := json.Marshal(transferParams)
	if err != nil {
		return types.Transfer{}, fmt.Errorf("marshalling request parameters: %w", err)
	}

	transfer := types.Transfer{
		Type:   "libp2p",
		Params: paramsBytes,
		Size:   uint64(carSize),
	}

	if os.Getenv("S3_ENDPOINT") != "" {
		// with s3 we don't need to transfer the data, use a p2p proxy for more reliable connectivity
		transfer, err = convertLibp2pTransferToProxy(transfer, r.host.ID(), prefAddrs)
		if err != nil {
			return types.Transfer{}, fmt.Errorf("convert libp2p transfer to proxy: %w", err)
		}
	}

	return transfer, nil
}

// ConvertLibp2pTransferToProxy takes a libp2p Transfer and outputs a proxied HTTP Transfer
func convertLibp2pTransferToProxy(transfer types.Transfer, peerid peer.ID, prefAddrs []ma.Multiaddr) (types.Transfer, error) {
	// Check that the transfer is of Type "libp2p"
	if transfer.Type != "libp2p" {
		return types.Transfer{}, fmt.Errorf("transfer is not of type libp2p")
	}

	// Unmarshal the Params field to get the HttpRequest
	var httpReq types.HttpRequest
	err := json.Unmarshal(transfer.Params, &httpReq)
	if err != nil {
		return types.Transfer{}, fmt.Errorf("unmarshal transfer Params: %w", err)
	}

	// Construct new URL pointing to the proxy server
	proxyURL := fmt.Sprintf("https://libp2p.me/%s/", peerid.String())

	// Create new Headers, including the X-Multiaddr headers
	headers := httpReq.Headers
	if headers == nil {
		headers = make(map[string]string)
	}

	// Add X-Multiaddr headers
	headers["X-Multiaddr"] = ""
	for _, maddr := range prefAddrs {
		headers["X-Multiaddr"] = headers["X-Multiaddr"] + maddr.String() + ","
	}
	headers["X-Multiaddr"] = strings.TrimSuffix(headers["X-Multiaddr"], ",")

	// Add X-P2P-Protocol header
	headers["X-P2P-Protocol"] = types.DataTransferProtocol

	// Create new HttpRequest with the proxy URL and updated headers
	newHttpReq := types.HttpRequest{
		URL:     proxyURL,
		Headers: headers,
	}

	// Marshal the new HttpRequest to Params
	newParams, err := json.Marshal(newHttpReq)
	if err != nil {
		return types.Transfer{}, fmt.Errorf("marshal new HttpRequest: %w", err)
	}

	// Create new Transfer object of type "http"
	newTransfer := types.Transfer{
		Type:   "http",
		Params: newParams,
		Size:   transfer.Size,
	}

	return newTransfer, nil
}

func getPreferredAddrs(h host.Host) []ma.Multiaddr {
	type addrWithPref struct {
		addr ma.Multiaddr
		pref int
	}

	var addrs []addrWithPref

	for _, addr := range h.Addrs() {
		// Default preference for 'other' addresses
		pref := 4

		// Extract the protocols from the multiaddress
		protocols := addr.Protocols()

		// Flags to identify the type of address
		isDNS := false
		isIP4 := false
		isIP6 := false

		for _, p := range protocols {
			switch p.Code {
			case ma.P_DNS, ma.P_DNS4, ma.P_DNS6, ma.P_DNSADDR:
				isDNS = true
			case ma.P_IP4:
				isIP4 = true
			case ma.P_IP6:
				isIP6 = true
			}
		}

		// Handle DNS addresses
		if isDNS {
			pref = 1
			addrs = append(addrs, addrWithPref{addr: addr, pref: pref})
			continue
		}

		// Skip private addresses
		if manet.IsPrivateAddr(addr) {
			continue
		}

		// Handle public IP addresses
		if isIP4 {
			pref = 2
		} else if isIP6 {
			pref = 3
		}

		addrs = append(addrs, addrWithPref{addr: addr, pref: pref})
	}

	if len(addrs) == 0 {
		for _, a := range h.Addrs() {
			addrs = append(addrs, addrWithPref{addr: a, pref: 4})
		}

		log.Errorw("no non-private addresses found, using all addresses", "addrs", h.Addrs())
	}

	// Sort the addresses based on the preference
	sort.SliceStable(addrs, func(i, j int) bool {
		return addrs[i].pref < addrs[j].pref
	})

	// Extract the sorted multiaddresses
	var sortedAddrs []ma.Multiaddr
	for _, ap := range addrs {
		sortedAddrs = append(sortedAddrs, ap.addr)
	}

	return sortedAddrs
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
		log.Debugw("car request: redir to s3 url", "error", err, "url", s3u)

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
