package ributil

import (
	"context"
	"fmt"
	"golang.org/x/xerrors"
	"io"
	"net"
	"net/http"
	"time"
)

type robustHttpResponse struct {
	getRC func() *RateCounter

	url string

	cur             io.Reader
	curCloser       io.Closer
	atOff, dataSize int64
}

var maxRetryCount = 5

func (r *robustHttpResponse) Read(p []byte) (n int, err error) {
	defer func() {
		r.atOff += int64(n)
	}()

	for i := 0; i < maxRetryCount; i++ {
		if r.cur == nil {
			err := r.startReq()
			if err != nil {
				return 0, err
			}
		}

		n, err = r.cur.Read(p)
		if err == io.EOF {
			r.curCloser.Close()
			r.cur = nil

			return n, err
		}
		if err != nil {
			if n > 0 {
				r.curCloser.Close()
				r.cur = nil
				return n, nil
			}

			log.Errorw("robust http read error", "err", err)
			r.curCloser.Close()
			r.cur = nil
			continue
		}
		if n == 0 {
			r.curCloser.Close()
			return 0, xerrors.Errorf("read 0 bytes")
		}

		return n, nil
	}

	return 0, xerrors.Errorf("http read failed after %d retries", maxRetryCount)
}

func (r *robustHttpResponse) Close() error {
	if r.curCloser != nil {
		return r.curCloser.Close()
	}

	return nil
}

func (r *robustHttpResponse) startReq() error {
	dialer := &net.Dialer{
		Timeout: 20 * time.Second,
	}

	var nc net.Conn

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if nc != nil {
					return nil, xerrors.Errorf("expected one conn per client")
				}

				conn, err := dialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}

				nc = conn

				// Set a deadline for the whole operation, including reading the response
				if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
					return nil, xerrors.Errorf("set deadline: %w", err)
				}

				return conn, nil
			},
		},
	}

	req, err := http.NewRequest("GET", r.url, nil)
	if err != nil {
		//return xerrors.Errorf("new request: %w", err)
		log.Errorw("failed to create request", "err", err)
		return xerrors.Errorf("failed to create request")
	}

	req.Header.Set("Content-Range", fmt.Sprintf("bytes=%d-%d", r.atOff, r.dataSize))

	resp, err := client.Do(req)
	if err != nil {
		return xerrors.Errorf("do request: %w", err)
	}

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return xerrors.Errorf("http status: %d", resp.StatusCode)
	}

	if nc == nil {
		return xerrors.Errorf("nc was nil")
	}

	var reqTxIdleTimeout = 30 * time.Second

	dlRead := &readerDeadliner{
		Reader:      resp.Body,
		setDeadline: nc.SetDeadline,
	}

	rc := r.getRC()
	rw := NewRateEnforcingReader(dlRead, rc, reqTxIdleTimeout)

	r.cur = rw
	r.curCloser = funcCloser(func() error {
		rc.release()
		return resp.Body.Close()
	})

	return nil
}

type funcCloser func() error

func (fc funcCloser) Close() error {
	return fc()
}

func RobustGet(url string, dataSize int64, rcf func() *RateCounter) io.ReadCloser {
	return &robustHttpResponse{
		getRC:    rcf,
		url:      url,
		dataSize: dataSize,
	}
}

type readerDeadliner struct {
	io.Reader
	setDeadline func(time.Time) error
}

func (rd *readerDeadliner) SetReadDeadline(t time.Time) error {
	return rd.setDeadline(t)
}
