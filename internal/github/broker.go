// broker.go implements the GitHub actions-service ("broker") client used for
// runner scale set message sessions: an internal, undocumented long-poll API.
// Protocol reference: github.com/actions/scaleset.
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const scaleSetEndpoint = "_apis/runtime/runnerscalesets"

// errTokenExpired marks an expired message-queue access token (HTTP 401);
// triggers a session refresh and one retry.
var errTokenExpired = errors.New("message queue token expired")

// BrokerClient talks to the actions service. Long-poll requests block
// server-side, so the HTTP client intentionally has no timeout (contexts
// control cancellation).
type BrokerClient struct {
	gh     *Client
	org    string
	client *http.Client

	mu       sync.Mutex
	token    string
	tokenExp time.Time
	baseURL  string

	sessionMu sync.Mutex
	session   *brokerSession
}

type brokerSession struct {
	ID                      string              `json:"sessionId"`
	MessageQueueURL         string              `json:"messageQueueUrl"`
	MessageQueueAccessToken string              `json:"messageQueueAccessToken"`
	Statistics              *ScaleSetStatistics `json:"statistics,omitempty"`
}

// ScaleSetStatistics reports the scale set's server-side job/runner counts.
type ScaleSetStatistics struct {
	TotalAvailableJobs     int `json:"totalAvailableJobs"`
	TotalAcquiredJobs      int `json:"totalAcquiredJobs"`
	TotalAssignedJobs      int `json:"totalAssignedJobs"`
	TotalRunningJobs       int `json:"totalRunningJobs"`
	TotalRegisteredRunners int `json:"totalRegisteredRunners"`
	TotalBusyRunners       int `json:"totalBusyRunners"`
	TotalIdleRunners       int `json:"totalIdleRunners"`
}

// NewBrokerClient establishes the actions-service admin connection for org:
// registration token -> admin connection exchange.
func (g *Client) NewBrokerClient(ctx context.Context, org string) (*BrokerClient, error) {
	bc := &BrokerClient{gh: g, org: org, client: &http.Client{}}
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if err := bc.refreshTokenLocked(ctx); err != nil {
		return nil, fmt.Errorf("broker: initial admin connection: %w", err)
	}
	return bc, nil
}

// caller must hold b.mu
func (b *BrokerClient) refreshTokenLocked(ctx context.Context) error {
	regToken, err := b.gh.GetRunnerRegistrationToken(ctx, b.org)
	if err != nil {
		return fmt.Errorf("broker: registration token: %w", err)
	}
	conn, err := b.gh.GetActionsServiceConnection(ctx, b.org, regToken)
	if err != nil {
		return fmt.Errorf("broker: admin connection: %w", err)
	}
	b.token, b.tokenExp, b.baseURL = conn.Token, conn.ExpiresAt, conn.URL
	return nil
}

func (b *BrokerClient) adminToken(ctx context.Context) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.token == "" || time.Now().Add(60*time.Second).After(b.tokenExp) {
		if err := b.refreshTokenLocked(ctx); err != nil {
			return "", err
		}
	}
	return b.token, nil
}

// doActionsService performs an actions-service request; on 401 it refreshes
// the admin token and retries once.
func (b *BrokerClient) doActionsService(ctx context.Context, method, path string, body io.Reader, expected int, out any) error {
	for attempt := 0; attempt < 2; attempt++ {
		token, err := b.adminToken(ctx)
		if err != nil {
			return err
		}
		u := strings.TrimRight(b.baseURL, "/") + "/" + strings.TrimLeft(path, "/")
		if strings.Contains(u, "?") {
			u += "&api-version=6.0-preview"
		} else {
			u += "?api-version=6.0-preview"
		}
		req, err := http.NewRequestWithContext(ctx, method, u, body)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := b.client.Do(req)
		if err != nil {
			return err
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 {
			continue // expired admin token: refresh and retry once
		}
		if resp.StatusCode != expected {
			return fmt.Errorf("actions service: %s %s: unexpected status %s: %s", method, path, resp.Status, strings.TrimSpace(string(data)))
		}
		if out != nil && len(data) > 0 {
			if err := json.Unmarshal(data, out); err != nil {
				return fmt.Errorf("actions service: decode %s %s: %w", method, path, err)
			}
		}
		return nil
	}
	return fmt.Errorf("actions service: %s %s: unauthorized after token refresh", method, path)
}

// ---- runner scale sets ----

type scaleSetLabel struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type runnerSetting struct {
	DisableUpdate bool `json:"disableUpdate,omitempty"`
}

// scaleSet mirrors the actions-service scale set entity. The PATCH update
// must round-trip the full fetched object (a partial body is accepted with
// 200 but silently not applied), so every server field is preserved here.
type scaleSet struct {
	ID                 int             `json:"id,omitempty"`
	Name               string          `json:"name,omitempty"`
	RunnerGroupID      int             `json:"runnerGroupId,omitempty"`
	RunnerGroupName    string          `json:"runnerGroupName,omitempty"`
	Labels             []scaleSetLabel `json:"labels,omitempty"`
	RunnerSetting      runnerSetting   `json:"RunnerSetting"` // exact capital-R key, always serialized
	CreatedOn          time.Time       `json:"createdOn"`
	RunnerJitConfigURL string          `json:"runnerJitConfigUrl,omitempty"`
}

// GetOrCreateScaleSet returns the ID of the named scale set in the given
// runner group, creating it (with the given labels) when absent. When the
// scale set exists, its labels are reconciled with the desired set (PATCH on
// difference). Scale sets receive JobAvailable messages even with zero
// runners (scale-from-zero).
func (b *BrokerClient) GetOrCreateScaleSet(ctx context.Context, groupID int, name string, labels []string) (int, error) {
	q := url.Values{"runnerGroupId": {strconv.Itoa(groupID)}, "name": {name}}
	var list struct {
		Count int        `json:"count"`
		Value []scaleSet `json:"value"`
	}
	if err := b.doActionsService(ctx, http.MethodGet, scaleSetEndpoint+"?"+q.Encode(), nil, http.StatusOK, &list); err != nil {
		return 0, fmt.Errorf("broker: get scale set %q: %w", name, err)
	}
	ls := make([]scaleSetLabel, 0, len(labels))
	for _, l := range labels {
		ls = append(ls, scaleSetLabel{Type: "System", Name: l})
	}
	switch list.Count {
	case 1:
		existing := list.Value[0]
		if sameLabels(existing.Labels, ls) {
			return existing.ID, nil
		}
		// Labels are immutable after creation: the actions service accepts a
		// PATCH but never applies label changes (verified: 200 with the old
		// labels echoed back). The only way to change them is delete+recreate.
		path := fmt.Sprintf("%s/%d", scaleSetEndpoint, existing.ID)
		if err := b.doActionsService(ctx, http.MethodDelete, path, nil, http.StatusNoContent, nil); err != nil {
			slog.Warn("scale set labels differ but delete failed - keeping old labels; delete the scale set manually and restart",
				"name", name, "old", labelNames(existing.Labels), "new", labels, "error", err)
			return existing.ID, nil
		}
		slog.Info("scale set deleted for label change", "name", name, "old", labelNames(existing.Labels), "new", labels)
		created, err := b.createScaleSet(ctx, groupID, name, ls)
		if err != nil {
			return 0, err
		}
		return created.ID, nil
	case 0:
		created, err := b.createScaleSet(ctx, groupID, name, ls)
		if err != nil {
			return 0, err
		}
		return created.ID, nil
	default:
		return 0, fmt.Errorf("broker: multiple runner scale sets named %q", name)
	}
}

// DeleteScaleSetByName resolves the named scale set in the runner group and
// deletes it. The error carries the full server response (status + body),
// e.g. 422 when runners are still registered.
func (b *BrokerClient) DeleteScaleSetByName(ctx context.Context, groupID int, name string) error {
	q := url.Values{"runnerGroupId": {strconv.Itoa(groupID)}, "name": {name}}
	var list struct {
		Count int        `json:"count"`
		Value []scaleSet `json:"value"`
	}
	if err := b.doActionsService(ctx, http.MethodGet, scaleSetEndpoint+"?"+q.Encode(), nil, http.StatusOK, &list); err != nil {
		return fmt.Errorf("broker: get scale set %q: %w", name, err)
	}
	switch list.Count {
	case 1:
		path := fmt.Sprintf("%s/%d", scaleSetEndpoint, list.Value[0].ID)
		if err := b.doActionsService(ctx, http.MethodDelete, path, nil, http.StatusNoContent, nil); err != nil {
			return fmt.Errorf("broker: delete scale set %q: %w", name, err)
		}
		return nil
	case 0:
		return fmt.Errorf("broker: scale set %q not found", name)
	default:
		return fmt.Errorf("broker: multiple runner scale sets named %q", name)
	}
}

// createScaleSet creates the scale set with runner self-update disabled
// (runners are ephemeral - a mid-job update is pointless).
func (b *BrokerClient) createScaleSet(ctx context.Context, groupID int, name string, labels []scaleSetLabel) (*scaleSet, error) {
	body, _ := json.Marshal(scaleSet{
		Name:          name,
		RunnerGroupID: groupID,
		Labels:        labels,
		RunnerSetting: runnerSetting{DisableUpdate: true},
	})
	var created scaleSet
	if err := b.doActionsService(ctx, http.MethodPost, scaleSetEndpoint, bytes.NewReader(body), http.StatusOK, &created); err != nil {
		return nil, fmt.Errorf("broker: create scale set %q: %w", name, err)
	}
	return &created, nil
}

// sameLabels reports whether two label lists hold the same names, order and
// duplicates aside.
func sameLabels(a, b []scaleSetLabel) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, l := range a {
		seen[l.Name]++
	}
	for _, l := range b {
		seen[l.Name]--
		if seen[l.Name] < 0 {
			return false
		}
	}
	return true
}

func labelNames(ls []scaleSetLabel) []string {
	names := make([]string, len(ls))
	for i, l := range ls {
		names[i] = l.Name
	}
	return names
}

// ---- message sessions ----

// CreateSession opens the long-poll message session for the scale set.
// Returns the scale set's current server-side statistics.
func (b *BrokerClient) CreateSession(ctx context.Context, scaleSetID int) (*ScaleSetStatistics, error) {
	body, _ := json.Marshal(map[string]string{"ownerName": b.org})
	var created brokerSession
	path := fmt.Sprintf("/%s/%d/sessions", scaleSetEndpoint, scaleSetID)
	if err := b.doActionsService(ctx, http.MethodPost, path, bytes.NewReader(body), http.StatusOK, &created); err != nil {
		return nil, fmt.Errorf("broker: create session: %w", err)
	}
	b.sessionMu.Lock()
	b.session = &created
	b.sessionMu.Unlock()
	return created.Statistics, nil
}

func (b *BrokerClient) currentSession() *brokerSession {
	b.sessionMu.Lock()
	defer b.sessionMu.Unlock()
	return b.session
}

func (b *BrokerClient) refreshSession(ctx context.Context, scaleSetID int) error {
	sess := b.currentSession()
	if sess == nil {
		return fmt.Errorf("broker: no session to refresh")
	}
	var refreshed brokerSession
	path := fmt.Sprintf("/%s/%d/sessions/%s", scaleSetEndpoint, scaleSetID, sess.ID)
	if err := b.doActionsService(ctx, http.MethodPatch, path, nil, http.StatusOK, &refreshed); err != nil {
		return fmt.Errorf("broker: refresh session: %w", err)
	}
	b.sessionMu.Lock()
	b.session = &refreshed
	b.sessionMu.Unlock()
	return nil
}

// DeleteSession closes the current session (call on shutdown).
func (b *BrokerClient) DeleteSession(ctx context.Context, scaleSetID int) error {
	sess := b.currentSession()
	if sess == nil {
		return nil
	}
	path := fmt.Sprintf("/%s/%d/sessions/%s", scaleSetEndpoint, scaleSetID, sess.ID)
	return b.doActionsService(ctx, http.MethodDelete, path, nil, http.StatusNoContent, nil)
}

// ---- the message queue ----

// JobMessageBase carries the fields common to all job messages (subset - the
// wire format carries more, e.g. the broker job UUID and requestLabels, which
// we do not consume).
type JobMessageBase struct {
	RunnerRequestID int64  `json:"runnerRequestId"`
	RepositoryName  string `json:"repositoryName"`
	OwnerName       string `json:"ownerName"`
	JobDisplayName  string `json:"jobDisplayName"`
	WorkflowRunID   int64  `json:"workflowRunId"`
}

type JobAvailableMessage struct{ JobMessageBase }

type JobAssignedMessage struct{ JobMessageBase }

type JobStartedMessage struct {
	JobMessageBase
	RunnerName string `json:"runnerName"`
}

type JobCompletedMessage struct {
	JobMessageBase
	Result     string `json:"result"`
	RunnerName string `json:"runnerName"`
}

// JobMessages is one dequeued batch (a single queue message can carry
// multiple job events).
type JobMessages struct {
	MessageID  int
	Statistics *ScaleSetStatistics
	Available  []JobAvailableMessage
	Assigned   []JobAssignedMessage
	Started    []JobStartedMessage
	Completed  []JobCompletedMessage
}

func parseJobMessages(r io.Reader) (*JobMessages, error) {
	var wrapper struct {
		MessageID   int                 `json:"messageId"`
		MessageType string              `json:"messageType"`
		Body        string              `json:"body"`
		Statistics  *ScaleSetStatistics `json:"statistics"`
	}
	if err := json.NewDecoder(r).Decode(&wrapper); err != nil {
		return nil, fmt.Errorf("broker: decode message wrapper: %w", err)
	}
	if wrapper.MessageType != "RunnerScaleSetJobMessages" {
		return nil, fmt.Errorf("broker: unsupported message type: %s", wrapper.MessageType)
	}
	out := &JobMessages{MessageID: wrapper.MessageID, Statistics: wrapper.Statistics}
	if wrapper.Body == "" {
		return out, nil
	}
	var batch []json.RawMessage
	if err := json.Unmarshal([]byte(wrapper.Body), &batch); err != nil {
		return nil, fmt.Errorf("broker: decode message batch: %w", err)
	}
	for _, raw := range batch {
		var mt struct {
			MessageType string `json:"messageType"`
		}
		if err := json.Unmarshal(raw, &mt); err != nil {
			return nil, fmt.Errorf("broker: decode message type: %w", err)
		}
		switch mt.MessageType {
		case "JobAvailable":
			var m JobAvailableMessage
			if err := json.Unmarshal(raw, &m); err != nil {
				return nil, err
			}
			out.Available = append(out.Available, m)
		case "JobAssigned":
			var m JobAssignedMessage
			if err := json.Unmarshal(raw, &m); err != nil {
				return nil, err
			}
			out.Assigned = append(out.Assigned, m)
		case "JobStarted":
			var m JobStartedMessage
			if err := json.Unmarshal(raw, &m); err != nil {
				return nil, err
			}
			out.Started = append(out.Started, m)
		case "JobCompleted":
			var m JobCompletedMessage
			if err := json.Unmarshal(raw, &m); err != nil {
				return nil, err
			}
			out.Completed = append(out.Completed, m)
		}
	}
	return out, nil
}

// GetMessage long-polls the message queue. Returns (nil, nil) when the poll
// window elapsed without messages. On an expired session token it refreshes
// the session and retries once.
func (b *BrokerClient) GetMessage(ctx context.Context, scaleSetID, lastMessageID, maxCapacity int) (*JobMessages, error) {
	msg, err := b.getMessage(ctx, lastMessageID, maxCapacity)
	if errors.Is(err, errTokenExpired) {
		if rerr := b.refreshSession(ctx, scaleSetID); rerr != nil {
			return nil, rerr
		}
		return b.getMessage(ctx, lastMessageID, maxCapacity)
	}
	return msg, err
}

func (b *BrokerClient) getMessage(ctx context.Context, lastMessageID, maxCapacity int) (*JobMessages, error) {
	sess := b.currentSession()
	if sess == nil {
		return nil, fmt.Errorf("broker: no message session")
	}
	u, err := url.Parse(sess.MessageQueueURL)
	if err != nil {
		return nil, fmt.Errorf("broker: parse message queue url: %w", err)
	}
	if lastMessageID > 0 {
		q := u.Query()
		q.Set("lastMessageId", strconv.Itoa(lastMessageID))
		u.RawQuery = q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json; api-version=6.0-preview")
	req.Header.Set("Authorization", "Bearer "+sess.MessageQueueAccessToken)
	req.Header.Set("X-ScaleSetMaxCapacity", strconv.Itoa(maxCapacity))
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusAccepted: // long-poll window elapsed, no messages
		return nil, nil
	case http.StatusOK:
		return parseJobMessages(resp.Body)
	case http.StatusUnauthorized:
		return nil, errTokenExpired
	default:
		return nil, fmt.Errorf("broker: get message: unexpected status %s", resp.Status)
	}
}

// AcquireJobs acquires job requests on behalf of the scale set; per the
// listener contract, a JobAvailable that is never acquired stays unassigned
// forever. Authenticates with the message-queue token, not the admin token.
// Returns the request IDs that were actually acquired.
func (b *BrokerClient) AcquireJobs(ctx context.Context, scaleSetID int, requestIDs []int64) ([]int64, error) {
	ids, err := b.acquireJobs(ctx, scaleSetID, requestIDs)
	if errors.Is(err, errTokenExpired) {
		if rerr := b.refreshSession(ctx, scaleSetID); rerr != nil {
			return nil, rerr
		}
		return b.acquireJobs(ctx, scaleSetID, requestIDs)
	}
	return ids, err
}

func (b *BrokerClient) acquireJobs(ctx context.Context, scaleSetID int, requestIDs []int64) ([]int64, error) {
	sess := b.currentSession()
	if sess == nil {
		return nil, fmt.Errorf("broker: no message session")
	}
	body, err := json.Marshal(requestIDs)
	if err != nil {
		return nil, err
	}
	u := strings.TrimRight(b.baseURL, "/") + "/" + scaleSetEndpoint + "/" + strconv.Itoa(scaleSetID) + "/acquirejobs?api-version=6.0-preview"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sess.MessageQueueAccessToken)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var out struct {
			Value []int64 `json:"value"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, fmt.Errorf("broker: acquire jobs: decode: %w", err)
		}
		return out.Value, nil
	case http.StatusUnauthorized:
		return nil, errTokenExpired
	default:
		return nil, fmt.Errorf("broker: acquire jobs: unexpected status %s", resp.Status)
	}
}

// GenerateScaleSetJITConfig generates a JIT configuration that registers the
// runner INTO the scale set (required for scale-set-routed jobs). Note the
// actions-service response uses camelCase (encodedJITConfig), unlike the REST
// API's encoded_jit_config.
func (b *BrokerClient) GenerateScaleSetJITConfig(ctx context.Context, scaleSetID int, name, workDir string) (string, error) {
	body, _ := json.Marshal(map[string]string{"name": name, "workFolder": workDir})
	var out struct {
		EncodedJITConfig string `json:"encodedJITConfig"`
	}
	path := fmt.Sprintf("/%s/%d/generatejitconfig", scaleSetEndpoint, scaleSetID)
	if err := b.doActionsService(ctx, http.MethodPost, path, bytes.NewReader(body), http.StatusOK, &out); err != nil {
		return "", err
	}
	if out.EncodedJITConfig == "" {
		return "", fmt.Errorf("broker: empty encoded jit config")
	}
	return out.EncodedJITConfig, nil
}

// DeleteMessage acks a processed queue message; without the ack the message
// is redelivered. Refreshes the session and retries once on token expiry.
func (b *BrokerClient) DeleteMessage(ctx context.Context, scaleSetID, messageID int) error {
	err := b.deleteMessage(ctx, messageID)
	if errors.Is(err, errTokenExpired) {
		if rerr := b.refreshSession(ctx, scaleSetID); rerr != nil {
			return rerr
		}
		return b.deleteMessage(ctx, messageID)
	}
	return err
}

func (b *BrokerClient) deleteMessage(ctx context.Context, messageID int) error {
	sess := b.currentSession()
	if sess == nil {
		return fmt.Errorf("broker: no message session")
	}
	u, err := url.Parse(sess.MessageQueueURL)
	if err != nil {
		return fmt.Errorf("broker: parse message queue url: %w", err)
	}
	u.Path = fmt.Sprintf("%s/%d", u.Path, messageID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+sess.MessageQueueAccessToken)
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusUnauthorized:
		return errTokenExpired
	default:
		return fmt.Errorf("broker: delete message: unexpected status %s", resp.Status)
	}
}
