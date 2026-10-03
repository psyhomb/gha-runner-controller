package github

import (
	"bytes"
	"encoding/json"
	"testing"
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
