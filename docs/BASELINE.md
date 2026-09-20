# Baseline Provenance

This document records the provenance and verification status of the initial code imported into `agy-go`.

## Source Repository

- **Origin Repository:** [https://github.com/vlxlv/agy-pool-go](https://github.com/vlxlv/agy-pool-go)
- **Source Baseline Commit:** `8f6081a188eedb4d39698e3f53200e52f949d0d0`

## Baseline Verification

Prior to performing identity migration, the imported worktree was verified from `~/work/agy-go`:

- `gofmt` check: **PASS** (zero formatting discrepancies)
- `go vet ./...`: **PASS** (zero issues)
- `go test ./... -count=1`: **PASS** (all package suites passed)
- `go test -race ./... -count=1`: **PASS** (all package suites passed with race detection)
- Build (`./cmd/agy-pool`): **PASS**

## Import Scope & Integrity

- The source worktree was imported with zero source and test differences relative to the baseline commit before mechanical project-identity changes.
- Generated build artifacts (`bin/` and `dist/`) and source version control metadata (`.git/`) were intentionally not imported.
- This document records historical provenance only; `agy-go` maintains no runtime or build dependency on `agy-pool-go`.
