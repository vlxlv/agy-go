package accounts

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/mail"
	"strings"
	"time"

	"github.com/vlxlv/agy-go/internal/auth"
)

// tokenEndpointEmail is only for tokens received directly from Google's authenticated
// HTTPS token endpoint (OIDC Core 3.1.3.7). Never use it to authenticate a file or callback token.
func tokenEndpointEmail(token, clientID string, now time.Time) (string, error) {
	invalid := errors.New("invalid Google ID token claims")
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[2] == "" {
		return "", invalid
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", invalid
	}
	var h struct {
		Alg string `json:"alg"`
	}
	if json.Unmarshal(header, &h) != nil || h.Alg != "RS256" {
		return "", invalid
	}
	c := auth.DecodeJWTPayload(token)
	issuer, _ := c["iss"].(string)
	subject, _ := c["sub"].(string)
	email, _ := c["email"].(string)
	address, emailErr := mail.ParseAddress(email)
	if emailErr != nil || address.Address != email {
		return "", invalid
	}
	verified, _ := c["email_verified"].(bool)
	expires, _ := c["exp"].(float64)
	issued, _ := c["iat"].(float64)
	audience, _ := c["aud"].(string)
	if audiences, ok := c["aud"].([]any); ok && len(audiences) == 1 {
		audience, _ = audiences[0].(string)
	}
	if (issuer != "https://accounts.google.com" && issuer != "accounts.google.com") ||
		subject == "" || strings.TrimSpace(email) == "" || !verified || audience != clientID ||
		expires <= float64(now.Unix()) || issued <= 0 || issued > float64(now.Unix()+60) {
		return "", invalid
	}
	if party, exists := c["azp"]; exists && party != clientID {
		return "", invalid
	}
	return email, nil
}
