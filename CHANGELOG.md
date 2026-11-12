# Changelog

## Unreleased

- Update `github.com/klauspost/compress` from v1.19.2 to v1.20.2 to address [GO-2026-6865](https://pkg.go.dev/vuln/GO-2026-6865), a panic in FSE/Huffman frame processing. Checksums are recorded in `go.sum`; the existing codec, restore, race and fuzz checks remain the compatibility gates.
- Add source-build, contribution and private security-reporting documentation.

Replay has no published tagged release. These entries describe development on `main`.
