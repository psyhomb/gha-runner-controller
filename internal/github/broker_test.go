package github

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func brokerFixture(t *testing.T, inner []map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	wrapper := map[string]any{
		"messageId":   42,
		"messageType": "RunnerScaleSetJobMessages",
		"body":        string(body),
	}
	data, err := json.Marshal(wrapper)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestParseJobMessages(t *testing.T) {
	data := brokerFixture(t, []map[string]any{
		{
			"messageType":     "JobAvailable",
			"runnerRequestId": 123,
			"repositoryName":  "repo",
			"ownerName":       "org",
			"jobId":           "uuid-1",
			"jobDisplayName":  "smoke-test",
			"workflowRunId":   456,
			"requestLabels":   []string{"self-hosted", "macOS", "ARM64"},
		},
		{
			"messageType": "JobCompleted",
			"jobId":       "uuid-1",
			"runnerName":  "eph-1",
			"result":      "succeeded",
		},
	})

	msg, err := parseJobMessages(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("parseJobMessages: %v", err)
	}
	if msg.MessageID != 42 {
		t.Errorf("MessageID = %d, want 42", msg.MessageID)
	}
	if len(msg.Available) != 1 {
		t.Fatalf("Available = %d, want 1", len(msg.Available))
	}
	avail := msg.Available[0]
	if avail.JobDisplayName != "smoke-test" || avail.WorkflowRunID != 456 || avail.RunnerRequestID != 123 {
		t.Errorf("unexpected JobAvailable payload: %+v", avail)
	}
	if len(msg.Completed) != 1 || msg.Completed[0].RunnerName != "eph-1" || msg.Completed[0].Result != "succeeded" {
		t.Errorf("unexpected JobCompleted payload: %+v", msg.Completed)
	}
}

func TestParseJobMessagesRejectsUnknownWrapper(t *testing.T) {
	data, _ := json.Marshal(map[string]any{
		"messageId":   1,
		"messageType": "SomethingElse",
		"body":        "[]",
	})
	if _, err := parseJobMessages(bytes.NewReader(data)); err == nil {
		t.Error("expected error for unsupported message type")
	}
}

func TestSameLabels(t *testing.T) {
	mk := func(names ...string) []scaleSetLabel {
		ls := make([]scaleSetLabel, len(names))
		for i, n := range names {
			ls[i] = scaleSetLabel{Type: "System", Name: n}
		}
		return ls
	}
	if !sameLabels(mk("a", "b"), mk("b", "a")) {
		t.Error("reordered labels should compare equal")
	}
	if sameLabels(mk("a"), mk("a", "b")) {
		t.Error("different lengths should not compare equal")
	}
	if sameLabels(mk("a", "b"), mk("a", "c")) {
		t.Error("different names should not compare equal")
	}
}

func TestGetOrCreateScaleSetReconcilesLabels(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stored       []string
		deleteStatus int
		wantDelete   bool
		wantCreate   bool
		wantID       int
	}{
		{"labels changed - recreate", []string{"self-hosted", "tahoe"}, http.StatusNoContent, true, true, 9},
		{"labels unchanged (reordered)", []string{"macOS", "self-hosted"}, http.StatusNoContent, false, false, 7},
		{"delete fails - keep old set", []string{"self-hosted", "tahoe"}, http.StatusUnprocessableEntity, true, false, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var deleted, created bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.Method {
				case http.MethodGet:
					stored := make([]scaleSetLabel, len(tc.stored))
					for i, n := range tc.stored {
						stored[i] = scaleSetLabel{Type: "System", Name: n}
					}
					json.NewEncoder(w).Encode(map[string]any{
						"count": 1,
						"value": []scaleSet{{ID: 7, Name: "ss", RunnerGroupID: 1, Labels: stored}},
					})
				case http.MethodDelete:
					deleted = true
					w.WriteHeader(tc.deleteStatus)
				case http.MethodPost:
					created = true
					var body scaleSet
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("POST body decode: %v", err)
					}
					if !body.RunnerSetting.DisableUpdate {
						t.Error("create body should disable runner self-update")
					}
					if len(body.Labels) != 2 || body.Labels[0].Type != "System" {
						t.Errorf("create labels = %+v, want 2 System labels", body.Labels)
					}
					body.ID = 9
					json.NewEncoder(w).Encode(body)
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL)
				}
			}))
			defer srv.Close()
			b := &BrokerClient{client: srv.Client(), baseURL: srv.URL, token: "t", tokenExp: time.Now().Add(time.Hour)}
			id, err := b.GetOrCreateScaleSet(context.Background(), 1, "ss", []string{"self-hosted", "macOS"})
			if err != nil {
				t.Fatal(err)
			}
			if id != tc.wantID {
				t.Errorf("scale set ID = %d, want %d", id, tc.wantID)
			}
			if deleted != tc.wantDelete {
				t.Errorf("DELETE called = %v, want %v", deleted, tc.wantDelete)
			}
			if created != tc.wantCreate {
				t.Errorf("POST called = %v, want %v", created, tc.wantCreate)
			}
		})
	}
}

func TestDeleteScaleSetByName(t *testing.T) {
	for _, tc := range []struct {
		name         string
		count        int
		deleteStatus int
		wantDelete   bool
		wantErr      string
	}{
		{"deleted", 1, http.StatusNoContent, true, ""},
		{"not found", 0, http.StatusNoContent, false, "not found"},
		{"in use - 422", 1, http.StatusUnprocessableEntity, true, "422 Unprocessable Entity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var deleted bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.Method {
				case http.MethodGet:
					sets := []scaleSet{}
					if tc.count > 0 {
						sets = append(sets, scaleSet{ID: 7, Name: "ss", RunnerGroupID: 1})
					}
					json.NewEncoder(w).Encode(map[string]any{"count": tc.count, "value": sets})
				case http.MethodDelete:
					deleted = true
					w.WriteHeader(tc.deleteStatus)
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL)
				}
			}))
			defer srv.Close()
			b := &BrokerClient{client: srv.Client(), baseURL: srv.URL, token: "t", tokenExp: time.Now().Add(time.Hour)}
			err := b.DeleteScaleSetByName(context.Background(), 1, "ss")
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
			if deleted != tc.wantDelete {
				t.Errorf("DELETE called = %v, want %v", deleted, tc.wantDelete)
			}
		})
	}
}
