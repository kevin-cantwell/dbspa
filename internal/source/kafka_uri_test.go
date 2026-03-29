package source

import (
	"testing"
)

func TestParseKafkaURIBasic(t *testing.T) {
	cfg, err := ParseKafkaURI("kafka://localhost:9092/my-topic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Broker != "localhost:9092" {
		t.Errorf("broker: got %q, want %q", cfg.Broker, "localhost:9092")
	}
	if cfg.Topic != "my-topic" {
		t.Errorf("topic: got %q, want %q", cfg.Topic, "my-topic")
	}
	if cfg.Offset != "latest" {
		t.Errorf("offset: got %q, want %q", cfg.Offset, "latest")
	}
}

func TestParseKafkaURIDefaultPort(t *testing.T) {
	cfg, err := ParseKafkaURI("kafka://broker-host/events")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Broker != "broker-host:9092" {
		t.Errorf("broker: got %q, want %q", cfg.Broker, "broker-host:9092")
	}
}

func TestParseKafkaURIWithAuth(t *testing.T) {
	cfg, err := ParseKafkaURI("kafka://myuser:mypass@broker:9093/topic1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SASLUser != "myuser" {
		t.Errorf("user: got %q, want %q", cfg.SASLUser, "myuser")
	}
	if cfg.SASLPass != "mypass" {
		t.Errorf("pass: got %q, want %q", cfg.SASLPass, "mypass")
	}
	if cfg.SASLMech != "PLAIN" {
		t.Errorf("mech: got %q, want %q", cfg.SASLMech, "PLAIN")
	}
}

func TestParseKafkaURIWithParams(t *testing.T) {
	cfg, err := ParseKafkaURI("kafka://host:9092/topic?offset=earliest&partition=0,2,4&group=my-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Offset != "earliest" {
		t.Errorf("offset: got %q, want %q", cfg.Offset, "earliest")
	}
	if len(cfg.Partitions) != 3 {
		t.Fatalf("partitions: got %d, want 3", len(cfg.Partitions))
	}
	want := []int32{0, 2, 4}
	for i, p := range cfg.Partitions {
		if p != want[i] {
			t.Errorf("partition[%d]: got %d, want %d", i, p, want[i])
		}
	}
	if cfg.Group != "my-group" {
		t.Errorf("group: got %q, want %q", cfg.Group, "my-group")
	}
}

func TestParseKafkaURIWithSASLParams(t *testing.T) {
	cfg, err := ParseKafkaURI("kafka://host/topic?sasl_mechanism=SCRAM-SHA-256&sasl_username=u&sasl_password=p")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SASLMech != "SCRAM-SHA-256" {
		t.Errorf("mech: got %q", cfg.SASLMech)
	}
	if cfg.SASLUser != "u" {
		t.Errorf("user: got %q", cfg.SASLUser)
	}
}

func TestParseKafkaURIMissingTopic(t *testing.T) {
	_, err := ParseKafkaURI("kafka://host:9092/")
	if err == nil {
		t.Fatal("expected error for missing topic")
	}
}

func TestParseKafkaURIMissingHost(t *testing.T) {
	_, err := ParseKafkaURI("kafka:///topic")
	if err == nil {
		t.Fatal("expected error for missing host")
	}
}

func TestParseKafkaURIWrongScheme(t *testing.T) {
	_, err := ParseKafkaURI("http://host/topic")
	if err == nil {
		t.Fatal("expected error for wrong scheme")
	}
}

func TestParseKafkaURITopicWithDots(t *testing.T) {
	cfg, err := ParseKafkaURI("kafka://broker:9092/orders.cdc.events")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Topic != "orders.cdc.events" {
		t.Errorf("topic: got %q, want %q", cfg.Topic, "orders.cdc.events")
	}
}
