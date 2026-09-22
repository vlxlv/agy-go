package accounts

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/vlxlv/agy-go/internal/auth"
	"github.com/vlxlv/agy-go/internal/storage"
)

func loginTestToken(email string, changes map[string]any) string {
	c := map[string]any{"email": email, "email_verified": true, "sub": "test-subject", "iss": "https://accounts.google.com", "aud": auth.GetClientID(), "exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix()}
	for key, value := range changes {
		c[key] = value
	}
	b, _ := json.Marshal(c)
	return "eyJhbGciOiJSUzI1NiJ9." + base64.RawURLEncoding.EncodeToString(b) + ".c2ln"
}

func TestLoginRejectsInvalidIdentityBeforeSaving(t *testing.T) {
	setupIsolatedTestDir(t)
	for key, value := range map[string]any{"email": "", "email_verified": false, "sub": "", "iss": "https://attacker.invalid", "aud": "other-client", "azp": "other-client", "exp": 1, "iat": time.Now().Add(time.Hour).Unix()} {
		t.Run(key, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"access_token": "access", "refresh_token": "refresh", "expires_in": 3600, "id_token": loginTestToken("user@example.com", map[string]any{key: value})})
			}))
			defer server.Close()
			_, err := Login(context.Background(), LoginOptions{TokenEndpoint: server.URL, BrowserOpener: func(string) error { return nil }, PromptFn: func(context.Context, string) (string, error) { return "code", nil }, PortRange: [2]int{0, 1}})
			if err == nil {
				t.Fatal("accepted invalid identity")
			}
			pool, err := storage.LoadPool()
			if err != nil || len(pool.Accounts) != 0 {
				t.Fatalf("pool changed: %+v %v", pool, err)
			}
		})
	}
}

func TestLoginCancelsAndJoinsPrompt(t *testing.T) {
	setupIsolatedTestDir(t)
	done := make(chan struct{})
	_, err := Login(context.Background(), LoginOptions{Timeout: 100 * time.Millisecond, BrowserOpener: func(string) error { return nil }, PortRange: [2]int{0, 1}, PromptFn: func(ctx context.Context, _ string) (string, error) {
		defer close(done)
		<-ctx.Done()
		return "", ctx.Err()
	}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("prompt still running after login returned")
	}
}

func TestCallbackStopsPromptBeforeTokenExchange(t *testing.T) {
	setupIsolatedTestDir(t)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-done:
		default:
			t.Error("prompt still reading during token exchange")
		}
		json.NewEncoder(w).Encode(map[string]any{"access_token": "access", "refresh_token": "refresh", "expires_in": 3600, "id_token": loginTestToken("user@example.com", nil)})
	}))
	defer server.Close()
	_, err := Login(context.Background(), LoginOptions{
		TokenEndpoint: server.URL, PortRange: [2]int{0, 1}, Timeout: time.Second,
		PromptFn: func(ctx context.Context, _ string) (string, error) {
			defer close(done)
			<-ctx.Done()
			return "", ctx.Err()
		},
		BrowserOpener: func(authURL string) error {
			u, err := url.Parse(authURL)
			if err != nil {
				return err
			}
			q := u.Query()
			resp, err := http.Get(q.Get("redirect_uri") + "?code=code&state=" + url.QueryEscape(q.Get("state")))
			if err == nil {
				resp.Body.Close()
			}
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}
