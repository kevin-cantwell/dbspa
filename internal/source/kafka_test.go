package source

import (
	"testing"
)

func TestEncodeKafkaKeyUTF8(t *testing.T) {
	key := []byte("user-123")
	got := EncodeKafkaKey(key)
	if got != "user-123" {
		t.Errorf("got %q, want %q", got, "user-123")
	}
}

func TestEncodeKafkaKeyNonUTF8(t *testing.T) {
	// 0xFE 0xFF are not valid UTF-8 sequences
	key := []byte{0xFE, 0xFF, 0x80}
	got := EncodeKafkaKey(key)
	if got[:4] != "b64:" {
		t.Errorf("expected b64: prefix for invalid UTF-8, got %q", got)
	}
}

func TestEncodeKafkaKeyNil(t *testing.T) {
	got := EncodeKafkaKey(nil)
	if got != "" {
		t.Errorf("got %q, want empty string", got)
	}
}

func TestEncodeKafkaKeyEmpty(t *testing.T) {
	got := EncodeKafkaKey([]byte{})
	if got != "" {
		t.Errorf("got %q, want empty string", got)
	}
}
