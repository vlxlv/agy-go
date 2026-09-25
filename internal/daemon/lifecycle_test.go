package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vlxlv/agy-go/internal/config"
	"github.com/vlxlv/agy-go/internal/storage"
)

func TestRunForeground_LifecycleAndGracefulShutdown(t *testing.T) {
	tmpDir := t.TempDir()
	pidFile := filepath.Join(tmpDir, "foreground.pid")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readyChan := make(chan struct{})
	boundPortChan := make(chan int, 1)

	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	serverDone := make(chan struct{})
	serverErrChan := make(chan error, 1)
	go func() {
		defer close(serverDone)
		serverErrChan <- RunForeground(ctx, ServerOptions{
			Port:                 0,
			EphemeralPort:        true,
			PIDFile:              pidFile,
			Handler:              testHandler,
			ReadyChan:            readyChan,
			BoundPortChan:        boundPortChan,
			QuotaRefreshInterval: 1 * time.Hour,
			DisableSignals:       true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-serverDone
	})

	// Wait for server to bind and be ready
	select {
	case <-readyChan:
	case err := <-serverErrChan:
		t.Fatalf("RunForeground failed immediately: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for server ready")
	}

	boundPort := <-boundPortChan
	if boundPort <= 0 {
		t.Fatalf("invalid bound port %d", boundPort)
	}

	// 1. Verify port is listening
	if !IsPortListening(boundPort) {
		t.Fatalf("expected port %d to be listening", boundPort)
	}

	// 2. Verify PID file was written
	info, err := ReadPIDFile(pidFile)
	if err != nil {
		t.Fatalf("ReadPIDFile failed: %v", err)
	}
	if info.PID != os.Getpid() {
		t.Fatalf("expected PID %d, got %d", os.Getpid(), info.PID)
	}

	// 2b. Verify runtime stats file was written
	runtimeFile := filepath.Join(tmpDir, "agy-pool-runtime.json")
	stats, err := ReadRuntimeStats(runtimeFile)
	if err != nil {
		t.Fatalf("ReadRuntimeStats failed: %v", err)
	}
	if stats.PID != os.Getpid() {
		t.Fatalf("expected runtime stats PID %d, got %d", os.Getpid(), stats.PID)
	}
	if stats.Goroutines <= 0 || stats.AllocBytes == 0 {
		t.Fatalf("expected valid runtime stats, got: %+v", stats)
	}

	// 3. Verify concurrent start on same PID file fails
	concurrentErr := RunForeground(ctx, ServerOptions{
		Port:           0,
		EphemeralPort:  true,
		PIDFile:        pidFile,
		Handler:        testHandler,
		DisableSignals: true,
	})
	if !errors.Is(concurrentErr, ErrDaemonAlreadyRunning) {
		t.Fatalf("expected ErrDaemonAlreadyRunning, got %v", concurrentErr)
	}

	// 4. Trigger graceful shutdown
	cancel()

	select {
	case err := <-serverErrChan:
		if err != nil {
			t.Fatalf("RunForeground returned error on shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for graceful shutdown")
	}

	// 5. Verify listener is closed
	if IsPortListening(boundPort) {
		t.Fatalf("expected port %d to no longer be listening", boundPort)
	}

	// 6. Verify PID file and runtime stats file were cleaned up
	if _, err := os.Stat(pidFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected PID file to be removed on clean exit")
	}
	if _, err := os.Stat(runtimeFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected runtime stats file to be removed on clean exit")
	}
}

func TestIsDaemonOutdated(t *testing.T) {
	tmpDir := t.TempDir()
	pidFile := filepath.Join(tmpDir, "outdated.pid")

	origPIDProvider := PIDFileProvider
	PIDFileProvider = func() string { return pidFile }
	defer func() { PIDFileProvider = origPIDProvider }()

	// If daemon is not running / port not listening, IsDaemonOutdated returns false
	if IsDaemonOutdated(pidFile, 12345) {
		t.Fatalf("expected false when port not listening")
	}

	// Start a dummy HTTP listener on ephemeral port
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-serverDone
	})

	boundPortChan := make(chan int, 1)
	readyChan := make(chan struct{})

	go func() {
		defer close(serverDone)
		_ = RunForeground(ctx, ServerOptions{
			Port:           0,
			EphemeralPort:  true,
			PIDFile:        pidFile,
			ReadyChan:      readyChan,
			BoundPortChan:  boundPortChan,
			DisableSignals: true,
		})
	}()

	<-readyChan
	port := <-boundPortChan

	// Running current version: should not be outdated
	if IsDaemonOutdated(pidFile, port) {
		t.Fatalf("expected current daemon not to be outdated")
	}

	// Overwrite PID file with an older version string
	_ = WritePIDFile(pidFile, DaemonInfo{
		PID:         os.Getpid(),
		Version:     "0.0.1-old",
		ScriptMtime: time.Now().Unix(),
	})

	if !IsDaemonOutdated(pidFile, port) {
		t.Fatalf("expected daemon with older version to be marked outdated")
	}

	// Overwrite PID file with an older mtime (1 second in past)
	currExe := GetEntrypointPath()
	if st, err := os.Stat(currExe); err == nil {
		_ = WritePIDFile(pidFile, DaemonInfo{
			PID:         os.Getpid(),
			Version:     config.Version,
			ScriptMtime: st.ModTime().Unix() - 10,
		})
		if !IsDaemonOutdated(pidFile, port) {
			t.Fatalf("expected daemon with older script_mtime to be marked outdated")
		}
	}
}

func TestLifecycle_RequireSameInstanceRejectsForeignProcess(t *testing.T) {
	tmpDir := t.TempDir()
	resetSandbox := config.SetSyntheticSandboxRoot(tmpDir)
	t.Cleanup(resetSandbox)
	pidFile := filepath.Join(tmpDir, "agy-pool.pid")

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	portCh := make(chan int, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunForeground(ctx, ServerOptions{Port: 0, EphemeralPort: true, PIDFile: pidFile, ReadyChan: ready, BoundPortChan: portCh, DisableSignals: true})
	}()
	<-ready
	port := <-portCh
	t.Cleanup(func() {
		cancel()
		<-done
	})
	if err := WritePIDFile(pidFile, DaemonInfo{PID: os.Getpid(), DataDir: filepath.Join(tmpDir, "foreign"), Port: port, Version: config.Version}); err != nil {
		t.Fatal(err)
	}
	instance := &InstanceContext{DataDir: tmpDir, ListenHost: "127.0.0.1", ListenPort: port, Version: config.Version}
	if _, err := StartInstance(instance, LaunchOptions{PIDFile: pidFile, RequireSameInstance: true}); err == nil {
		t.Fatal("foreign process was accepted by start")
	}
	if _, err := RestartInstance(instance, LaunchOptions{PIDFile: pidFile, RequireSameInstance: true}); err == nil {
		t.Fatal("foreign process was accepted by restart")
	}
	if !IsPortListening(port) || !IsProcessAlive(os.Getpid()) {
		t.Fatal("foreign process was stopped or signaled")
	}
}

func TestStartInstance_NoRestartLeavesOutdatedDaemonRunning(t *testing.T) {
	tmpDir := t.TempDir()
	resetSandbox := config.SetSyntheticSandboxRoot(tmpDir)
	t.Cleanup(resetSandbox)
	pidFile := filepath.Join(tmpDir, "agy-pool.pid")

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	portCh := make(chan int, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunForeground(ctx, ServerOptions{Port: 0, EphemeralPort: true, PIDFile: pidFile, ReadyChan: ready, BoundPortChan: portCh, DisableSignals: true})
	}()
	<-ready
	port := <-portCh
	t.Cleanup(func() {
		cancel()
		<-done
	})

	instance := &InstanceContext{DataDir: tmpDir, ListenHost: "127.0.0.1", ListenPort: port, Version: config.Version}
	if err := WritePIDFile(pidFile, DaemonInfo{PID: os.Getpid(), DataDir: tmpDir, Port: port, Version: "old-version"}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := StartInstance(instance, LaunchOptions{PIDFile: pidFile, RequireSameInstance: true, NoRestart: true}); !errors.Is(err, ErrOutdatedDaemonRunning) {
		t.Fatalf("expected ErrOutdatedDaemonRunning, got %v", err)
	}
	after, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("PID file changed")
	}
	if !IsPortListening(port) || !IsProcessAlive(os.Getpid()) {
		t.Fatal("outdated daemon was stopped or signaled")
	}
}

func TestStopDaemon_Scenarios(t *testing.T) {
	tmpDir := t.TempDir()
	pidFile := filepath.Join(tmpDir, "stop.pid")

	// 1. Stop nonexistent PID file
	err := StopDaemon(pidFile, 12345, 100*time.Millisecond)
	if !errors.Is(err, ErrNotRunning) {
		t.Fatalf("expected ErrNotRunning, got %v", err)
	}

	// 2. Stop with dead PID: cleans up stale PID file
	deadPID := 4194300
	for IsProcessAlive(deadPID) {
		deadPID--
	}
	_ = WritePIDFile(pidFile, DaemonInfo{PID: deadPID})
	err = StopDaemon(pidFile, 12345, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("expected nil on stale dead PID cleanup, got %v", err)
	}
	if _, err := os.Stat(pidFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected stale PID file to be removed")
	}

	// 3. Stop with foreign PID (e.g. PID 1): refuses to kill, cleans up stale file
	if _, err := os.Stat("/proc/1/cmdline"); err == nil {
		_ = WritePIDFile(pidFile, DaemonInfo{PID: 1})
		err = StopDaemon(pidFile, 12345, 100*time.Millisecond)
		if !errors.Is(err, ErrPIDReused) {
			t.Fatalf("expected ErrPIDReused when PID file points to PID 1, got %v", err)
		}
		if _, err := os.Stat(pidFile); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected stale PID file to be cleaned up")
		}
	}
}

func TestHelperProcess_BackgroundDaemonSpawnAndStop(t *testing.T) {
	if os.Getenv("AGY_HELPER_PROCESS") == "1" {
		// Helper child process mode
		port := 0
		if pStr := os.Getenv(EnvDaemonPort); pStr != "" {
			_, _ = fmt.Sscanf(pStr, "%d", &port)
		}
		for i, arg := range os.Args {
			if arg == "--port" && i+1 < len(os.Args) {
				_, _ = fmt.Sscanf(os.Args[i+1], "%d", &port)
			}
		}
		pidFile := os.Getenv("AGY_TEST_PID_FILE")
		err := RunForeground(context.Background(), ServerOptions{
			Port:                 port,
			PIDFile:              pidFile,
			QuotaRefreshInterval: 1 * time.Hour,
			DisableSignals:       false,
		})
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}

	tmpDir := t.TempDir()
	pidFile := filepath.Join(tmpDir, "child.pid")
	logFile := filepath.Join(tmpDir, "child.log")

	// Pick an available ephemeral port
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get ephemeral port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	opts := LaunchOptions{
		Port:           port,
		PIDFile:        pidFile,
		LogFile:        logFile,
		EntrypointPath: os.Args[0], // Current test binary
		CustomArgs:     []string{"-test.run=TestHelperProcess_BackgroundDaemonSpawnAndStop"},
	}

	t.Setenv("AGY_HELPER_PROCESS", "1")
	t.Setenv("AGY_TEST_PID_FILE", pidFile)

	res, err := StartDaemon(opts)
	if err != nil {
		logContent, _ := os.ReadFile(logFile)
		t.Fatalf("StartDaemon failed: %v\nChild log:\n%s", err, string(logContent))
	}
	if res.PID <= 0 {
		t.Fatalf("invalid child PID %d", res.PID)
	}

	// Verify child is alive and port is listening
	if !IsProcessAlive(res.PID) {
		t.Fatalf("expected child PID %d to be alive", res.PID)
	}
	if !IsPortListening(port) {
		t.Fatalf("expected port %d to be listening", port)
	}

	// Verify log file was created with 0600 mode
	st, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("log file was not created: %v", err)
	}
	if perm := st.Mode().Perm(); perm != 0o600 {
		t.Fatalf("expected log file mode 0600, got %o", perm)
	}

	// Now stop the child daemon
	err = StopDaemon(pidFile, port, 8*time.Second)
	if err != nil {
		logContent, _ := os.ReadFile(logFile)
		t.Fatalf("StopDaemon failed: %v\nChild log:\n%s", err, string(logContent))
	}

	// Verify process is dead and port is closed
	if IsProcessAlive(res.PID) {
		t.Fatalf("expected child PID %d to be dead after StopDaemon", res.PID)
	}
	if IsPortListening(port) {
		t.Fatalf("expected port %d to no longer be listening", port)
	}
	if _, err := os.Stat(pidFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected PID file to be unlinked after StopDaemon")
	}
}

// TestDaemon_StartsQuotaRefresher verifies that daemon startup activates the background quota refresher.
func TestDaemon_StartsQuotaRefresher(t *testing.T) {
	tmpDir := t.TempDir()
	if err := config.ConfigureStateDir(tmpDir); err != nil {
		t.Fatalf("failed to configure state dir: %v", err)
	}
	t.Cleanup(config.ResetDataDir)

	// Seed pool with a stale account
	oldTime := time.Now().Unix() - 500
	frac := 0.5
	acc := &storage.Account{
		ID:    "acc-stale-refresh",
		Email: "stale@example.com",
		LastQuota: &storage.QuotaState{
			Gemini5H:  &storage.QuotaWindow{Fraction: &frac},
			UpdatedAt: &oldTime,
		},
	}
	if err := storage.SavePool(&storage.Pool{Accounts: []*storage.Account{acc}}); err != nil {
		t.Fatalf("failed to save pool: %v", err)
	}

	// Set a mock QuotaRefresher to observe invocations
	refreshed := make(chan string, 1)
	origRefresher := QuotaRefresher
	QuotaRefresher = func(a *storage.Account) {
		select {
		case refreshed <- a.ID:
		default:
		}
	}
	defer func() {
		QuotaRefresher = origRefresher
	}()

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-serverDone
	})

	pidFile := filepath.Join(tmpDir, "server.pid")
	readyChan := make(chan struct{})

	go func() {
		defer close(serverDone)
		_ = RunForeground(ctx, ServerOptions{
			Port:                 0,
			EphemeralPort:        true,
			PIDFile:              pidFile,
			ReadyChan:            readyChan,
			QuotaRefreshInterval: 20 * time.Millisecond,
			DisableSignals:       true,
		})
	}()

	select {
	case <-readyChan:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for server ready")
	}

	// Verify QuotaRefresher is invoked for the stale account
	select {
	case id := <-refreshed:
		if id != "acc-stale-refresh" {
			t.Errorf("expected refresh for acc-stale-refresh, got %s", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for background QuotaRefresher to be called by daemon")
	}
}

func TestStartFailureReapsSlowChild(t *testing.T) {
	if os.Getenv("AGY_SLOW_START_HELPER") == "1" {
		if err := os.WriteFile(os.Getenv("AGY_CHILD_PID_CAPTURE"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			os.Exit(2)
		}
		time.Sleep(time.Minute)
		os.Exit(0)
	}
	dir := t.TempDir()
	capture := filepath.Join(dir, "spawned.pid")
	t.Setenv("AGY_SLOW_START_HELPER", "1")
	t.Setenv("AGY_CHILD_PID_CAPTURE", capture)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	_, err = StartDaemon(LaunchOptions{StateDir: dir, Port: port, EntrypointPath: os.Args[0], CustomArgs: []string{"-test.run=^TestStartFailureReapsSlowChild$"}})
	if err == nil {
		t.Fatal("slow child unexpectedly became ready")
	}
	b, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	if IsProcessAlive(pid) {
		t.Fatalf("failed startup left child %d alive", pid)
	}
}

func TestShutdownCancelsActiveRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	ports := make(chan int, 1)
	entered := make(chan struct{})
	canceled := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- RunForeground(ctx, ServerOptions{
			EphemeralPort: true, PIDFile: filepath.Join(t.TempDir(), "server.pid"), DisableSignals: true,
			ReadyChan: ready, BoundPortChan: ports,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-r.Context().Done()
				close(canceled)
			}),
		})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("startup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("startup timeout")
	}
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/", <-ports))
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler timeout")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("request context not canceled")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("shutdown timeout")
	}
	<-clientDone
}
