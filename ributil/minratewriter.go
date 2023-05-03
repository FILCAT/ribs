package ributil

import (
	"golang.org/x/xerrors"
	"io"
	"time"
)

type RateEnforcingWriter struct {
	w        io.Writer
	rateMbps float64

	minRateMbps      float64
	writeError       error
	errorLowSpeed    bool
	bytesTransferred int64
	lastSpeedCheck   time.Time
	windowDuration   time.Duration
}

func NewRateEnforcingWriter(w io.Writer, minRateMbps float64, windowDuration time.Duration) *RateEnforcingWriter {
	return &RateEnforcingWriter{
		w:              w,
		minRateMbps:    minRateMbps,
		windowDuration: windowDuration,
	}
}

func (rew *RateEnforcingWriter) Write(p []byte) (int, error) {
	if rew.writeError != nil {
		return 0, rew.writeError
	}

	now := time.Now()

	if !rew.lastSpeedCheck.IsZero() && now.Sub(rew.lastSpeedCheck) >= rew.windowDuration {
		elapsedTime := now.Sub(rew.lastSpeedCheck)
		transferSpeedMbps := float64(rew.bytesTransferred*8) / 1e6 / elapsedTime.Seconds()

		if transferSpeedMbps < rew.minRateMbps {
			rew.writeError = xerrors.Errorf("write rate over past %s is too slow: %f Mbps, expected at least %f Mbps", rew.windowDuration, transferSpeedMbps, rew.minRateMbps)
			rew.errorLowSpeed = true
			return 0, rew.writeError
		}

		// Reset transferred bytes and last speed check time
		rew.bytesTransferred = 0
		rew.lastSpeedCheck = now

		// Set write deadline
		if w, ok := rew.w.(interface{ SetWriteDeadline(time.Time) error }); ok {
			_ = w.SetWriteDeadline(now.Add(rew.windowDuration * 2))
		}
	} else if rew.lastSpeedCheck.IsZero() {
		rew.lastSpeedCheck = now
		// Set write deadline
		if w, ok := rew.w.(interface{ SetWriteDeadline(time.Time) error }); ok {
			_ = w.SetWriteDeadline(now.Add(rew.windowDuration * 2))
		}
	}

	n, err := rew.w.Write(p)
	rew.bytesTransferred += int64(n)
	rew.writeError = err
	return n, err
}

func (rew *RateEnforcingWriter) WriteError() error {
	return rew.writeError
}
