# Install

## From source (Go 1.25+)

```bash
go install github.com/kevin-cantwell/dbspa/cmd/dbspa@latest
```

## Build from source

```bash
git clone https://github.com/kevin-cantwell/dbspa.git
cd dbspa
make build
# Produces: ./dbspa and ./dbspa-gen (data generator)
```

## Pre-built binaries

Download from the [releases page](https://github.com/kevin-cantwell/dbspa/releases). Binaries are available for:

| OS | Architecture |
|---|---|
| Linux | amd64, arm64 |
| macOS | amd64 (Intel), arm64 (Apple Silicon) |

```bash
# Example: macOS arm64
curl -Lo dbspa https://github.com/kevin-cantwell/dbspa/releases/latest/download/dbspa-darwin-arm64
chmod +x dbspa
mv dbspa /usr/local/bin/
```

## Verify installation

```bash
dbspa version
```

## Dependencies

DBSPA is a single static binary with zero runtime dependencies. No server, no daemon, no cluster.

For Kafka sources, you need a reachable Kafka broker. For Avro format with schema registry, the registry must be accessible over HTTP.
