package source

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// KafkaConfig holds parsed Kafka connection parameters.
type KafkaConfig struct {
	Broker    string   // host:port
	Topic     string
	Offset    string   // "earliest", "latest", ISO timestamp, or integer offset
	Partitions []int32 // nil means all
	Group     string   // consumer group ID; empty = ephemeral
	Registry  string   // Confluent Schema Registry URL (e.g., "http://schema-registry:8081")
	SASLUser  string
	SASLPass  string
	SASLMech  string   // "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"
	TLSCert   string
	TLSKey    string
	TLSCA     string
}

// ParseKafkaURI parses a kafka:// URI into a KafkaConfig.
// Format: kafka://[user:pass@]host[:port]/topic[?params]
func ParseKafkaURI(raw string) (*KafkaConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid kafka URI: %w", err)
	}
	if u.Scheme != "kafka" {
		return nil, fmt.Errorf("expected kafka:// scheme, got %q", u.Scheme)
	}

	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("kafka URI missing host")
	}
	port := u.Port()
	if port == "" {
		port = "9092"
	}

	topic := strings.TrimPrefix(u.Path, "/")
	if topic == "" {
		return nil, fmt.Errorf("kafka URI missing topic (path)")
	}

	cfg := &KafkaConfig{
		Broker: host + ":" + port,
		Topic:  topic,
		Offset: "latest", // default per spec
	}

	// Credentials from userinfo
	if u.User != nil {
		cfg.SASLUser = u.User.Username()
		cfg.SASLPass, _ = u.User.Password()
		if cfg.SASLMech == "" {
			cfg.SASLMech = "PLAIN"
		}
	}

	// Query params
	q := u.Query()
	if v := q.Get("offset"); v != "" {
		cfg.Offset = v
	}
	if v := q.Get("partition"); v != "" {
		parts := strings.Split(v, ",")
		for _, ps := range parts {
			n, err := strconv.ParseInt(strings.TrimSpace(ps), 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid partition %q: %w", ps, err)
			}
			cfg.Partitions = append(cfg.Partitions, int32(n))
		}
	}
	if v := q.Get("group"); v != "" {
		cfg.Group = v
	}
	if v := q.Get("sasl_mechanism"); v != "" {
		cfg.SASLMech = v
	}
	if v := q.Get("sasl_username"); v != "" {
		cfg.SASLUser = v
	}
	if v := q.Get("sasl_password"); v != "" {
		cfg.SASLPass = v
	}
	if v := q.Get("tls_cert"); v != "" {
		cfg.TLSCert = v
	}
	if v := q.Get("tls_key"); v != "" {
		cfg.TLSKey = v
	}
	if v := q.Get("tls_ca"); v != "" {
		cfg.TLSCA = v
	}
	if v := q.Get("registry"); v != "" {
		cfg.Registry = v
	}

	return cfg, nil
}
