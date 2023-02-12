package impl

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/transport/types"
	"github.com/gbrlsnchs/jwt/v3"
	gostream "github.com/libp2p/go-libp2p-gostream"
	"github.com/libp2p/go-libp2p/core/host"
	"golang.org/x/xerrors"
	"net"
	"net/http"
	"time"
)

func (r *ribs) setupCarServer(ctx context.Context, host host.Host) error {
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

	// todo also serve tcp

	return nil
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

	err = r.withReadableGroup(reqToken.Group, func(group *Group) error {
		_, _, err := group.writeCar(w)
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
