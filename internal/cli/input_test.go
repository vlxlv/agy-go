package cli

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestInputCancellationLeavesFileUsable(t *testing.T) {
	input, output, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	defer output.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := (inputReader{ctx: ctx, source: input}).Read(make([]byte, 1)); done <- err }()
	// Exercise cancellation while waiting for data, not just an already canceled context.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("input remained blocked")
	}
	if _, err := output.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if _, err := (inputReader{ctx: context.Background(), source: input}).Read(buf); err != nil || buf[0] != 'x' {
		t.Fatalf("file unusable: %q %v", buf, err)
	}
}
