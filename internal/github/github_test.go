package github

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func encodeBundle(t *testing.T, files map[string]string) string {
	t.Helper()
	raw, err := json.Marshal(files)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func TestDecodeJITConfig(t *testing.T) {
	valid := encodeBundle(t, map[string]string{
		".runner":       base64.StdEncoding.EncodeToString([]byte(`{"agentName":"test"}`)),
		".credentials":  base64.StdEncoding.EncodeToString([]byte(`{"scheme":"OAuth"}`)),
		".nested.pem-x": base64.StdEncoding.EncodeToString([]byte("key")),
	})

	files, err := DecodeJITConfig(valid)
	if err != nil {
		t.Fatalf("DecodeJITConfig: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("got %d files, want 3", len(files))
	}
	if got := string(files[".runner"]); got != `{"agentName":"test"}` {
		t.Errorf(".runner content = %q", got)
	}
	if got := string(files[".nested.pem-x"]); got != "key" {
		t.Errorf(".nested.pem-x content = %q", got)
	}
}

func TestDecodeJITConfigRejectsUnsafeKeys(t *testing.T) {
	for _, key := range []string{"../evil", "a/b", "runner", ".a/../b", `/abs`} {
		bundle := encodeBundle(t, map[string]string{
			key: base64.StdEncoding.EncodeToString([]byte("x")),
		})
		if _, err := DecodeJITConfig(bundle); err == nil {
			t.Errorf("key %q: expected error, got none", key)
		}
	}
}

func TestDecodeJITConfigRejectsGarbage(t *testing.T) {
	if _, err := DecodeJITConfig("not-base64!!!"); err == nil {
		t.Error("expected error for invalid base64")
	}
	empty := encodeBundle(t, map[string]string{})
	if _, err := DecodeJITConfig(empty); err == nil {
		t.Error("expected error for empty bundle")
	}
}
