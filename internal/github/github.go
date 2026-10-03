// Package github is a minimal GitHub App REST client (stdlib only):
// JWT minting, installation token caching, and the handful of endpoints the
// controller needs.
package github

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const apiBase = "https://api.github.com"

// Client is a GitHub App API client.
type Client struct {
	appID          int64
	installationID int64
	key            *rsa.PrivateKey
	http           *http.Client

	mu       sync.Mutex
	token    string
	tokenExp time.Time
}

// NewClient reads the App's PEM key and returns a ready client.
func NewClient(appID, installationID int64, keyPath string) (*Client, error) {
	pemBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read app key: %w", err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in %s", keyPath)
	}
	var key *rsa.PrivateKey
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		key = k
	} else if k8, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if k, ok := k8.(*rsa.PrivateKey); ok {
			key = k
		}
	}
	if key == nil {
		return nil, fmt.Errorf("parse app key %s: unsupported format (want RSA PKCS#1 or PKCS#8)", keyPath)
	}
	return &Client{
		appID:          appID,
		installationID: installationID,
		key:            key,
		http:           &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// jwt mints a short-lived GitHub App JWT (RS256).
func (g *Client) jwt() (string, error) {
	now := time.Now()
	b64 := func(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	claims, _ := json.Marshal(map[string]int64{
		"iat": now.Add(-60 * time.Second).Unix(),
		"exp": now.Add(9 * time.Minute).Unix(),
		"iss": g.appID,
	})
	input := b64(header) + "." + b64(claims)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, g.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return input + "." + b64(sig), nil
}

func (g *Client) installationToken(ctx context.Context) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.token != "" && time.Until(g.tokenExp) > 5*time.Minute {
		return g.token, nil
	}
	jwt, err := g.jwt()
	if err != nil {
		return "", err
	}
	var out struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	err = g.doRaw(ctx, http.MethodPost,
		fmt.Sprintf("/app/installations/%d/access_tokens", g.installationID),
		nil, "Bearer "+jwt, &out)
	if err != nil {
		return "", fmt.Errorf("create installation token: %w", err)
	}
	g.token, g.tokenExp = out.Token, out.ExpiresAt
	return g.token, nil
}

// do performs an authenticated REST call and JSON-decodes the response into out (may be nil).
func (g *Client) do(ctx context.Context, method, path string, body any, out any) error {
	tok, err := g.installationToken(ctx)
	if err != nil {
		return err
	}
	return g.doRaw(ctx, method, path, body, "Bearer "+tok, out)
}

func (g *Client) doRaw(ctx context.Context, method, path string, body any, authHeader string, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, apiBase+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 300 {
		return &apiError{Status: resp.StatusCode, Body: string(data)}
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode %s %s: %w", method, path, err)
		}
	}
	return nil
}

type apiError struct {
	Status int
	Body   string
}

func (e *apiError) Error() string { return fmt.Sprintf("github api: status %d: %s", e.Status, e.Body) }

// ---- API models ----

// RunnerInfo is a registered runner (subset of fields).
type RunnerInfo struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"` // "online" or "offline"
	Busy   bool   `json:"busy"`
}

// ---- API calls ----

// ListRunners returns the runners registered at the given scope (first page
// only, up to 100). Repo scope: per-repo runners (runner groups are org-only).
func (g *Client) ListRunners(ctx context.Context, scope, owner, repo string) ([]RunnerInfo, error) {
	var path string
	if scope == "repo" {
		path = fmt.Sprintf("/repos/%s/%s/actions/runners", owner, repo)
	} else {
		path = "/orgs/" + owner + "/actions/runners"
	}
	q := url.Values{"per_page": {"100"}}
	var out struct {
		Runners []RunnerInfo `json:"runners"`
	}
	if err := g.do(ctx, http.MethodGet, path+"?"+q.Encode(), nil, &out); err != nil {
		return nil, err
	}
	return out.Runners, nil
}

// DeleteRunnerByName removes the runner registration matching name.
// Returns true when a runner was found and removed. A runner absent from the
// list, or a 404 on delete (e.g. it already self-deregistered after its
// job), is success with false.
func (g *Client) DeleteRunnerByName(ctx context.Context, scope, owner, repo, name string) (bool, error) {
	runners, err := g.ListRunners(ctx, scope, owner, repo)
	if err != nil {
		return false, fmt.Errorf("list runners: %w", err)
	}
	for _, r := range runners {
		if r.Name != name {
			continue
		}
		var path string
		if scope == "repo" {
			path = fmt.Sprintf("/repos/%s/%s/actions/runners/%d", owner, repo, r.ID)
		} else {
			path = fmt.Sprintf("/orgs/%s/actions/runners/%d", owner, r.ID)
		}
		if err := g.do(ctx, http.MethodDelete, path, nil, nil); err != nil {
			var ae *apiError
			if errors.As(err, &ae) && ae.Status == http.StatusNotFound {
				return true, nil // was registered; gone now
			}
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// DetectScope returns "org" or "repo" based on the owner's account type; the
// controller requires org scope (runner scale sets are an org concept).
func (g *Client) DetectScope(ctx context.Context, owner string) (string, error) {
	var out struct {
		Type string `json:"type"` // "Organization" or "User"
	}
	if err := g.do(ctx, http.MethodGet, "/users/"+owner, nil, &out); err != nil {
		return "", err
	}
	if out.Type == "Organization" {
		return "org", nil
	}
	return "repo", nil
}

// DecodeJITConfig decodes an encoded JIT configuration into the runner's
// on-disk config files (path -> content). Keys are validated to be dotfile
// names only (no path traversal).
func DecodeJITConfig(encoded string) (map[string][]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode jit config: %w", err)
	}
	var bundle map[string]string
	if err := json.Unmarshal(raw, &bundle); err != nil {
		return nil, fmt.Errorf("parse jit config: %w", err)
	}
	files := make(map[string][]byte, len(bundle))
	for path, b64 := range bundle {
		// Keys must be dotfile names only - never a path traversal vector.
		if !strings.HasPrefix(path, ".") || strings.ContainsAny(path, "/\\") || strings.Contains(path, "..") {
			return nil, fmt.Errorf("jit config: unsafe file key %q", path)
		}
		content, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("jit config: decode %s: %w", path, err)
		}
		files[path] = content
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("jit config: empty bundle")
	}
	return files, nil
}

// GetRunnerRegistrationToken obtains a runner registration token (org scope).
// Used to establish the actions-service admin connection for broker mode.
func (g *Client) GetRunnerRegistrationToken(ctx context.Context, org string) (string, error) {
	var out struct {
		Token string `json:"token"`
	}
	if err := g.do(ctx, http.MethodPost, "/orgs/"+org+"/actions/runners/registration-token", nil, &out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", fmt.Errorf("github api: empty runner registration token")
	}
	return out.Token, nil
}

// ActionsServiceConnection holds the actions-service (broker) base URL and
// admin token. Internal GitHub API, as used by ARC.
type ActionsServiceConnection struct {
	URL       string
	Token     string
	ExpiresAt time.Time
}

// GetActionsServiceConnection exchanges a runner registration token for an
// actions-service admin connection. Note the RemoteAuth scheme.
func (g *Client) GetActionsServiceConnection(ctx context.Context, org, registrationToken string) (*ActionsServiceConnection, error) {
	body := map[string]string{
		"url":          "https://github.com/" + org,
		"runner_event": "register",
	}
	var out struct {
		URL   *string `json:"url,omitempty"`
		Token *string `json:"token,omitempty"`
	}
	if err := g.doRaw(ctx, http.MethodPost, "/actions/runner-registration", body, "RemoteAuth "+registrationToken, &out); err != nil {
		return nil, err
	}
	if out.URL == nil || out.Token == nil || *out.URL == "" || *out.Token == "" {
		return nil, fmt.Errorf("actions service connection: missing url or token")
	}
	exp, err := jwtExpiresAt(*out.Token)
	if err != nil {
		return nil, fmt.Errorf("actions service connection: %w", err)
	}
	return &ActionsServiceConnection{URL: *out.URL, Token: *out.Token, ExpiresAt: exp}, nil
}

// jwtExpiresAt parses the exp claim of a JWT without verifying the signature.
func jwtExpiresAt(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return time.Time{}, fmt.Errorf("malformed jwt")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("malformed jwt payload: %w", err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, fmt.Errorf("malformed jwt claims: %w", err)
	}
	if claims.Exp == 0 {
		return time.Time{}, fmt.Errorf("jwt has no exp claim")
	}
	return time.Unix(claims.Exp, 0), nil
}
