# Install

## From source (Go 1.25+)

```bash
go install github.com/kevin-cantwell/folddb/cmd/folddb@latest
```

## Build from source

```bash
git clone https://github.com/kevin-cantwell/folddb.git
cd folddb
make build
# Produces: ./folddb and ./folddb-gen (data generator)
```

## Pre-built binaries

Download from the [releases page](https://github.com/kevin-cantwell/folddb/releases). Binaries are available for:

| OS | Architecture |
|---|---|
| Linux | amd64, arm64 |
| macOS | amd64 (Intel), arm64 (Apple Silicon) |

```bash
# Example: macOS arm64
curl -Lo folddb https://github.com/kevin-cantwell/folddb/releases/latest/download/folddb-darwin-arm64
chmod +x folddb
mv folddb /usr/local/bin/
```

## Verify installation

```bash
folddb version
```

## Dependencies

FoldDB is a single static binary with zero runtime dependencies. No server, no daemon, no cluster.

For Kafka sources, you need a reachable Kafka broker. For Avro format with schema registry, the registry must be accessible over HTTP.
