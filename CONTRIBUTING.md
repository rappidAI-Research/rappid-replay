# Contributing

Replay is experimental software. Read the [README](README.md),
[architecture decisions](docs/adr/README.md) and [adapter SDK](docs/adapter-sdk.md)
before changing recording, restore or rerun behavior.

Use Go 1.27 or newer, as required by `go.mod`. From the repository root:

```sh
go mod download
go test ./...
go vet ./...
go build -o bin/rappid ./cmd/rappid
```

Keep changes focused, describe their effect on recorded evidence and run the
affected tests. CI also checks supported desktop platforms. Do not claim
recorded playback, controlled external I/O, a UI or an intelligence layer as
implemented until the corresponding implementation and evidence exist.

Use synthetic fixtures without credentials or private recordings. Live reruns
execute commands and can affect external systems; they need explicit execution
authorization. Report security issues through [SECURITY.md](SECURITY.md).
