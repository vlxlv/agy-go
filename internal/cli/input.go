package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// inputReader polls the CLI-owned stdin reader without closing the caller's file.
// Only files and in-memory readers are accepted: an arbitrary io.Reader cannot be canceled.
type inputReader struct {
	ctx    context.Context
	source io.Reader
}

func (r inputReader) Read(p []byte) (int, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		switch source := r.source.(type) {
		case *os.File:
			fds := []unix.PollFd{{Fd: int32(source.Fd()), Events: unix.POLLIN}}
			n, err := unix.Poll(fds, 100)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if err != nil {
				return 0, err
			}
			if n == 0 {
				continue
			}
			if fds[0].Revents&unix.POLLNVAL != 0 {
				return 0, os.ErrClosed
			}
			if err := r.ctx.Err(); err != nil {
				return 0, err
			}
			return source.Read(p)
		case *bytes.Buffer, *bytes.Reader, *strings.Reader:
			return source.Read(p)
		default:
			return 0, errors.New("interactive input requires a file or in-memory reader")
		}
	}
}
