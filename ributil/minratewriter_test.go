package ributil

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestRateEnforcingWriter(t *testing.T) {
	t.Run("should write without error when rate is above minimum", func(t *testing.T) {
		var buf bytes.Buffer

		rc := NewRateCounters[int](MinAvgGlobalLogPeerRate(1024, 1000)).Get(0)
		rew := NewRateEnforcingWriter(&buf, rc, 50*time.Millisecond)
		defer rew.Done()

		data := make([]byte, 1024)
		time.Sleep(50 * time.Millisecond)
		n, err := rew.Write(data)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if n != len(data) {
			t.Fatalf("expected to write %d bytes, wrote %d", len(data), n)
		}
	})

	t.Run("should write with error when rate is below minimum", func(t *testing.T) {
		var buf deadlineWriter
		rc := NewRateCounters[int](MinAvgGlobalLogPeerRate(1024, 1000)).Get(0)
		rew := NewRateEnforcingWriter(&buf, rc, 50*time.Millisecond)
		defer rew.Done()

		data := make([]byte, 1024)
		_, err := rew.Write(data)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(60 * time.Millisecond) // Increase the sleep duration to make sure the rate is below the minimum
		n, err := rew.Write(data)
		t.Log(err)
		if !errors.Is(err, rew.writeError) {
			t.Fatalf("expected error, got: %v", err)
		}
		if n != 0 || buf.buf.Len() != 1024 {
			t.Fatalf("expected to write 0 bytes, wrote %d", n)
		}
	})

	t.Run("should set write deadline on the underlying writer", func(t *testing.T) {
		var buf deadlineWriter
		rc := NewRateCounters[int](MinAvgGlobalLogPeerRate(1024, 1000)).Get(0)
		rew := NewRateEnforcingWriter(&buf, rc, 50*time.Millisecond)
		defer rew.Done()

		data := make([]byte, 1024)
		_, err := rew.Write(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if buf.writeDeadline.IsZero() {
			t.Fatal("expected write deadline to be set")
		}
	})
}

type deadlineWriter struct {
	buf           bytes.Buffer
	writeDeadline time.Time
}

func (d *deadlineWriter) Write(p []byte) (n int, err error) {
	return d.buf.Write(p)
}

func (d *deadlineWriter) SetWriteDeadline(t time.Time) error {
	d.writeDeadline = t
	return nil
}

func TestRateEnforcingReader(t *testing.T) {
	t.Run("should read without error when rate is above minimum", func(t *testing.T) {
		data := make([]byte, 1024)
		buf := bytes.NewBuffer(data)

		rc := NewRateCounters[int](MinAvgGlobalLogPeerRate(1024, 1000)).Get(0)
		rer := NewRateEnforcingReader(buf, rc, 50*time.Millisecond)
		defer rer.Done()

		readData := make([]byte, 1024)
		time.Sleep(50 * time.Millisecond)
		n, err := rer.Read(readData)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if n != len(data) {
			t.Fatalf("expected to read %d bytes, read %d", len(data), n)
		}
	})

	t.Run("should read with error when rate is below minimum", func(t *testing.T) {
		data := make([]byte, 1024)
		buf := bytes.NewBuffer(data)

		rc := NewRateCounters[int](MinAvgGlobalLogPeerRate(1024, 1000)).Get(0)
		rer := NewRateEnforcingReader(buf, rc, 50*time.Millisecond)
		defer rer.Done()

		readData := make([]byte, 1024)
		_, err := rer.Read(readData)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(60 * time.Millisecond) // Increase the sleep duration to make sure the rate is below the minimum
		n, err := rer.Read(readData)
		t.Log(err)
		if !errors.Is(err, rer.readError) {
			t.Fatalf("expected error, got: %v", err)
		}
		if n != 0 {
			t.Fatalf("expected to read 0 bytes, read %d", n)
		}
	})

	t.Run("should support SetReadDeadline on the underlying reader", func(t *testing.T) {
		var buf deadlineReader
		buf.buf = bytes.NewBuffer(make([]byte, 2000))

		rc := NewRateCounters[int](MinAvgGlobalLogPeerRate(1024, 1000)).Get(0)
		rer := NewRateEnforcingReader(&buf, rc, 50*time.Millisecond)
		defer rer.Done()

		data := make([]byte, 1024)
		_, err := rer.Read(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if buf.readDeadline.IsZero() {
			t.Fatal("expected read deadline to be set")
		}
	})
}

type deadlineReader struct {
	buf          *bytes.Buffer
	readDeadline time.Time
}

func (d *deadlineReader) Read(p []byte) (n int, err error) {
	return d.buf.Read(p)
}

func (d *deadlineReader) SetReadDeadline(t time.Time) error {
	d.readDeadline = t
	return nil
}
