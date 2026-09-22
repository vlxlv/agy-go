package auth

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vlxlv/agy-go/internal/config"
	"github.com/vlxlv/agy-go/internal/storage"
)

func TestDecodeCred(t *testing.T) {
	if DefaultClientID == "" {
		t.Fatal("DefaultClientID is empty")
	}
	if DefaultClientSecret == "" {
		t.Fatal("DefaultClientSecret is empty")
	}

	// Verify DecodeCred on synthetic test vectors
	testCases := []struct {
		name string
		raw  string
		key  byte
	}{
		{
			name: "synthetic client id",
			raw:  "synthetic-client-id-sample-for-testing",
			key:  0x5A,
		},
		{
			name: "synthetic client secret",
			raw:  "synthetic-client-secret-sample-for-testing",
			key:  0x5A,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := []byte(tc.raw)
			enc := make([]byte, len(b))
			for i, v := range b {
				enc[i] = v ^ tc.key
			}
			hexStr := hex.EncodeToString(enc)
			got := DecodeCred(hexStr, tc.key)
			if got != tc.raw {
				t.Fatalf("DecodeCred(%q, 0x%02X) = %q, want %q", hexStr, tc.key, got, tc.raw)
			}
		})
	}

	// Verify invalid hex returns empty string
	if got := DecodeCred("invalid-hex-string!", 0x5A); got != "" {
		t.Fatalf("DecodeCred with invalid hex returned %q, want empty string", got)
	}

	// Verify DefaultClientID and DefaultClientSecret consistency with hex constants
	if got := DecodeCred(defaultClientIDHex, 0x5A); got != DefaultClientID {
		t.Fatalf("DecodeCred(defaultClientIDHex) = %q, want DefaultClientID %q", got, DefaultClientID)
	}
	if got := DecodeCred(defaultClientSecretHex, 0x5A); got != DefaultClientSecret {
		t.Fatalf("DecodeCred(defaultClientSecretHex) = %q, want DefaultClientSecret %q", got, DefaultClientSecret)
	}
}

func TestDecodeJWTPayload(t *testing.T) {
	tests := []struct {
		name      string
		jwt       string
		wantEmail string
		wantEmpty bool
	}{
		{
			name:      "valid jwt",
			jwt:       "header.eyJlbWFpbCI6ICJ1c2VyQGV4YW1wbGUuY29tIiwgInN1YiI6ICIxMjM0NSJ9.sig",
			wantEmail: "user@example.com",
		},
		{
			name:      "single part",
			jwt:       "not-a-jwt",
			wantEmpty: true,
		},
		{
			name:      "invalid base64",
			jwt:       "header.!!!notb64!!!.sig",
			wantEmpty: true,
		},
		{
			name:      "invalid json",
			jwt:       "header.bm90LWpzb24.sig", // "not-json"
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := DecodeJWTPayload(tt.jwt)
			if tt.wantEmpty {
				if len(res) != 0 {
					t.Fatalf("expected empty claims, got %+v", res)
				}
				return
			}
			if email, _ := res["email"].(string); email != tt.wantEmail {
				t.Fatalf("email = %q, want %q", email, tt.wantEmail)
			}
		})
	}
}

func TestErrorClassifiers(t *testing.T) {
	t.Run("IsValidationError", func(t *testing.T) {
		if IsValidationError(200, []byte("validation_required")) {
			t.Fatal("200 should not be validation error")
		}
		if !IsValidationError(403, []byte(`{"error": "validation_required"}`)) {
			t.Fatal("403 with validation_required should match")
		}
		if !IsValidationError(403, []byte("Please Verify Your Account to continue")) {
			t.Fatal("403 with verify your account should match")
		}
		if IsValidationError(403, []byte("other 403 error")) {
			t.Fatal("unrelated 403 should not match")
		}
	})

	t.Run("ExtractValidationURL", func(t *testing.T) {
		bodyMetadata := []byte(`{
			"error": {
				"details": [
					{"metadata": {"validation_url": "https://accounts.google.com/verify?id=123"}}
				]
			}
		}`)
		if u := ExtractValidationURL(bodyMetadata); u != "https://accounts.google.com/verify?id=123" {
			t.Fatalf("got %q, want metadata url", u)
		}

		bodyLink := []byte(`{
			"error": {
				"details": [
					{"links": [{"description": "Please verify", "url": "https://google.com/verify"}]}
				]
			}
		}`)
		if u := ExtractValidationURL(bodyLink); u != "https://google.com/verify" {
			t.Fatalf("got %q, want link url", u)
		}

		if u := ExtractValidationURL([]byte("invalid json")); u != "" {
			t.Fatalf("expected empty url on invalid json, got %q", u)
		}
	})

	t.Run("IsAuthError", func(t *testing.T) {
		if !IsAuthError(401, []byte("Unauthorized")) {
			t.Fatal("401 must be auth error")
		}
		if !IsAuthError(403, []byte("invalid_grant")) {
			t.Fatal("403 invalid_grant must be auth error")
		}
		if !IsAuthError(403, []byte("Access_Token_Expired")) {
			t.Fatal("403 token expired must be auth error")
		}
		if IsAuthError(403, []byte("permission denied for resource")) {
			t.Fatal("generic 403 should not be auth error")
		}
		if IsAuthError(500, []byte("invalid_grant")) {
			t.Fatal("500 must not be auth error")
		}
	})
}

func setupIsolatedTestDir(t *testing.T) string {
	t.Helper()
	tmp := t.TempDir()
	if err := config.ConfigureStateDir(tmp); err != nil {
		t.Fatalf("failed to configure state dir: %v", err)
	}
	t.Cleanup(config.ResetDataDir)
	return tmp
}

func TestRefreshToken_Cached(t *testing.T) {
	setupIsolatedTestDir(t)

	now := time.Now()
	NowFunc = func() time.Time { return now }
	defer func() { NowFunc = time.Now }()

	expiry := float64(now.Unix() + 300) // 5 minutes remaining (> 120s)
	acc := &storage.Account{
		ID:          "acc_1",
		AccessToken: "cached-token-123",
		TokenExpiry: &expiry,
	}

	tok, err := RefreshToken(acc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok != "cached-token-123" {
		t.Fatalf("got %q, want cached-token-123", tok)
	}
}

func TestRefreshToken_MissingRefreshToken(t *testing.T) {
	setupIsolatedTestDir(t)

	acc := &storage.Account{
		ID: "acc_1",
	}

	_, err := RefreshToken(acc)
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected 'Missing refresh_token', got %v", err)
	}
}

func TestRefreshToken_UpstreamFailureDoesNotLeakBody(t *testing.T) {
	setupIsolatedTestDir(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","description":"raw-provider-body-marker access-secret"}`))
	}))
	defer server.Close()
	oldURL := TokenURL
	TokenURL = server.URL
	t.Cleanup(func() { TokenURL = oldURL })
	acc := &storage.Account{ID: "acc_1", RefreshToken: "refresh-secret"}
	pool := storage.NewEmptyPool()
	pool.Accounts = []*storage.Account{acc}
	if err := storage.SavePool(pool); err != nil {
		t.Fatal(err)
	}
	_, err := RefreshToken(acc)
	if err == nil || !strings.Contains(err.Error(), "HTTP 400") || strings.Contains(err.Error(), "raw-provider-body-marker") || strings.Contains(err.Error(), "access-secret") || strings.Contains(err.Error(), "refresh-secret") {
		t.Fatalf("unsafe refresh error: %v", err)
	}
}

func TestRefreshToken_UpstreamSuccess(t *testing.T) {
	setupIsolatedTestDir(t)

	var serverHits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&serverHits, 1)
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.FormValue("grant_type") != "refresh_token" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.FormValue("refresh_token") != "valid-rf" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		resp := map[string]any{
			"access_token":  "new-at-456",
			"refresh_token": "new-rf-789",
			"expires_in":    3600,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	origTokenURL := TokenURL
	TokenURL = server.URL
	defer func() { TokenURL = origTokenURL }()

	pool := storage.NewEmptyPool()
	acc := &storage.Account{
		ID:           "acc_1",
		Email:        "user@example.com",
		RefreshToken: "valid-rf",
	}
	pool.Accounts = append(pool.Accounts, acc)
	if err := storage.SavePool(pool); err != nil {
		t.Fatalf("failed to save pool: %v", err)
	}

	tok, err := RefreshToken(acc)
	if err != nil {
		t.Fatalf("RefreshToken failed: %v", err)
	}
	if tok != "new-at-456" {
		t.Fatalf("got token %q, want new-at-456", tok)
	}
	if acc.RefreshToken != "new-rf-789" {
		t.Fatalf("got rf %q, want new-rf-789", acc.RefreshToken)
	}

	// Verify persistence in disk pool
	loaded, err := storage.LoadPool()
	if err != nil {
		t.Fatalf("failed to load pool: %v", err)
	}
	stored := storage.FindAccount(loaded, acc)
	if stored == nil || stored.AccessToken != "new-at-456" || stored.RefreshToken != "new-rf-789" {
		t.Fatalf("stored account tokens not updated: %+v", stored)
	}
}

func TestRefreshToken_ConcurrentSingleFlight(t *testing.T) {
	setupIsolatedTestDir(t)

	var serverHits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&serverHits, 1)
		time.Sleep(50 * time.Millisecond) // Simulate network latency

		resp := map[string]any{
			"access_token":  "new-concurrent-at",
			"refresh_token": "valid-rf",
			"expires_in":    3600,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	origTokenURL := TokenURL
	TokenURL = server.URL
	defer func() { TokenURL = origTokenURL }()

	pool := storage.NewEmptyPool()
	acc := &storage.Account{
		ID:           "acc_1",
		Email:        "user@example.com",
		RefreshToken: "valid-rf",
	}
	pool.Accounts = append(pool.Accounts, acc)
	if err := storage.SavePool(pool); err != nil {
		t.Fatalf("failed to save pool: %v", err)
	}

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			// Each goroutine passes a copy pointing to acc_1
			localAcc := &storage.Account{
				ID:           "acc_1",
				RefreshToken: "valid-rf",
			}
			tok, err := RefreshToken(localAcc)
			if err != nil {
				t.Errorf("RefreshToken concurrent error: %v", err)
				return
			}
			if tok != "new-concurrent-at" {
				t.Errorf("got token %q, want new-concurrent-at", tok)
			}
		}()
	}

	wg.Wait()

	// Upstream should only have been contacted ONCE due to file lock and post-lock re-read!
	hits := atomic.LoadInt32(&serverHits)
	if hits != 1 {
		t.Fatalf("expected exactly 1 upstream request due to lock reuse, got %d", hits)
	}
}

func TestRefreshToken_RemovedAccountNotRecreated(t *testing.T) {
	setupIsolatedTestDir(t)

	refreshReached := make(chan struct{})
	allowRefreshResponse := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(refreshReached)
		<-allowRefreshResponse

		resp := map[string]any{
			"access_token":  "new-at-after-remove",
			"refresh_token": "new-rf-after-remove",
			"expires_in":    3600,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	origTokenURL := TokenURL
	TokenURL = server.URL
	defer func() { TokenURL = origTokenURL }()

	pool := storage.NewEmptyPool()
	acc := &storage.Account{
		ID:           "acc_victim",
		Email:        "victim@example.com",
		RefreshToken: "valid-rf",
	}
	pool.Accounts = append(pool.Accounts, acc)
	if err := storage.SavePool(pool); err != nil {
		t.Fatalf("failed to save pool: %v", err)
	}

	errChan := make(chan error, 1)
	go func() {
		localAcc := &storage.Account{
			ID:           "acc_victim",
			Email:        "victim@example.com",
			RefreshToken: "valid-rf",
		}
		_, err := RefreshToken(localAcc)
		errChan <- err
	}()

	// Wait until refresh is in-flight at upstream server
	<-refreshReached

	// Remove the account from storage while refresh request is in flight
	err := storage.PoolTransaction(func(p *storage.Pool) error {
		p.Accounts = make([]*storage.Account, 0)
		return nil
	})
	if err != nil {
		t.Fatalf("failed to delete account during refresh: %v", err)
	}

	// Verify account is gone from storage
	loaded, err := storage.LoadPool()
	if err != nil {
		t.Fatalf("failed to load pool: %v", err)
	}
	if len(loaded.Accounts) != 0 {
		t.Fatalf("account was not deleted before refresh completed")
	}

	// Unblock upstream response
	close(allowRefreshResponse)

	// Refresh should complete with an error (account removed)
	refreshErr := <-errChan
	if refreshErr == nil {
		t.Fatalf("expected RefreshToken to report error when persisting to removed account, got nil")
	}

	// CRITICAL CHECK: Account MUST NOT have been recreated in storage!
	reloaded, err := storage.LoadPool()
	if err != nil {
		t.Fatalf("failed to reload pool: %v", err)
	}
	if len(reloaded.Accounts) != 0 {
		t.Fatalf("SECURITY VIOLATION: removed account was recreated by late refresh response: %+v", reloaded.Accounts[0])
	}
}

func TestRefreshToken_ConcurrentActiveSwitch(t *testing.T) {
	setupIsolatedTestDir(t)

	refreshReached := make(chan struct{})
	allowRefreshResponse := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(refreshReached)
		<-allowRefreshResponse

		resp := map[string]any{
			"access_token": "new-at-acc1",
			"expires_in":   3600,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	origTokenURL := TokenURL
	TokenURL = server.URL
	defer func() { TokenURL = origTokenURL }()

	pool := storage.NewEmptyPool()
	acc1 := &storage.Account{ID: "acc_1", Email: "acc1@example.com", RefreshToken: "rf1"}
	acc2 := &storage.Account{ID: "acc_2", Email: "acc2@example.com", RefreshToken: "rf2"}
	pool.Accounts = append(pool.Accounts, acc1, acc2)
	pool.ActiveAccountID = &acc1.ID
	if err := storage.SavePool(pool); err != nil {
		t.Fatalf("failed to save pool: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		localAcc1 := &storage.Account{ID: "acc_1", RefreshToken: "rf1"}
		_, err := RefreshToken(localAcc1)
		done <- err
	}()

	<-refreshReached

	// Switch active account to acc_2 while acc_1 is refreshing
	err := storage.PoolTransaction(func(p *storage.Pool) error {
		p.ActiveAccountID = &acc2.ID
		return nil
	})
	if err != nil {
		t.Fatalf("failed to switch active account: %v", err)
	}

	close(allowRefreshResponse)
	if err := <-done; err != nil {
		t.Fatalf("RefreshToken failed: %v", err)
	}

	// Verify active account remained acc_2
	loaded, err := storage.LoadPool()
	if err != nil {
		t.Fatalf("failed to load pool: %v", err)
	}
	if loaded.ActiveAccountID == nil || *loaded.ActiveAccountID != "acc_2" {
		t.Fatalf("active account was overwritten by refresh race! got %v, want acc_2", loaded.ActiveAccountID)
	}
}

func TestRefreshToken_RotatedTokenPreserved(t *testing.T) {
	setupIsolatedTestDir(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"access_token":  "new-at-rotated",
			"refresh_token": "brand-new-rotated-rf-999",
			"expires_in":    3600,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	origTokenURL := TokenURL
	TokenURL = server.URL
	defer func() { TokenURL = origTokenURL }()

	pool := storage.NewEmptyPool()
	acc := &storage.Account{ID: "acc_1", Email: "user@example.com", RefreshToken: "initial-rf"}
	pool.Accounts = append(pool.Accounts, acc)
	if err := storage.SavePool(pool); err != nil {
		t.Fatalf("failed to save pool: %v", err)
	}

	tok, err := RefreshToken(acc)
	if err != nil {
		t.Fatalf("RefreshToken failed: %v", err)
	}
	if tok != "new-at-rotated" {
		t.Fatalf("unexpected token: %s", tok)
	}

	loaded, err := storage.LoadPool()
	if err != nil {
		t.Fatalf("failed to load pool: %v", err)
	}
	if loaded.Accounts[0].RefreshToken != "brand-new-rotated-rf-999" {
		t.Fatalf("rotated refresh token was not preserved: got %s", loaded.Accounts[0].RefreshToken)
	}
}

func TestRefreshToken_RejectionClassification(t *testing.T) {
	setupIsolatedTestDir(t)
	oldURL, oldClient := TokenURL, HTTPClient
	defer func() { TokenURL, HTTPClient = oldURL, oldClient }()
	for _, tc := range []struct {
		body     string
		status   int
		rejected bool
	}{
		{`{"error":"invalid_grant"}`, 400, true}, {`{"error":"invalid_client"}`, 400, false}, {`{"error":"temporarily_unavailable"}`, 503, false}, {`{"error":"invalid_grant"}`, 503, false},
	} {
		t.Run(tc.body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			TokenURL, HTTPClient = server.URL, server.Client()
			acc := &storage.Account{ID: "acc_1", RefreshToken: "test-refresh"}
			pool := storage.NewEmptyPool()
			pool.Accounts = []*storage.Account{acc}
			if err := storage.SavePool(pool); err != nil {
				t.Fatal(err)
			}
			_, err := RefreshToken(acc)
			if err == nil || errors.Is(err, ErrInvalidCredentials) != tc.rejected {
				t.Fatalf("classification: %v", err)
			}
		})
	}
}

func TestRefreshDoesNotOverwriteNewLogin(t *testing.T) {
	setupIsolatedTestDir(t)
	acc := &storage.Account{ID: "acc_1", RefreshToken: "old-refresh", AccessToken: "old-access"}
	pool := storage.NewEmptyPool()
	pool.Accounts = []*storage.Account{acc}
	if err := storage.SavePool(pool); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := storage.PoolTransaction(func(p *storage.Pool) error {
			p.Accounts[0].AccessToken = "login-access"
			p.Accounts[0].RefreshToken = "login-refresh"
			return nil
		}); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte(`{"access_token":"stale-refresh-result","refresh_token":"stale-rotation","expires_in":3600}`))
	}))
	defer server.Close()
	oldURL := TokenURL
	TokenURL = server.URL
	defer func() { TokenURL = oldURL }()
	if _, err := RefreshToken(acc); err == nil || errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected concurrent update error: %v", err)
	}
	got, err := storage.LoadPool()
	if err != nil {
		t.Fatal(err)
	}
	if got.Accounts[0].RefreshToken != "login-refresh" || got.Accounts[0].AccessToken != "login-access" {
		t.Fatal("new login overwritten")
	}
}
