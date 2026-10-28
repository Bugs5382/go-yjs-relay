# AGENTS.md - go-yjs-relay

Guide for AI agents working in this repository. Pair with `CLAUDE.md` (the working agreement and
hook-enforced rules). Keep this file current when the build, layout, or public API changes.

## What this is

A product-neutral Yjs relay for Go: per-room y-websocket sync, presence fan-out, debounced snapshots, and flush and freeze around a publish.

The relay runs no Y.Doc: updates fan out to peers verbatim and clients converge on their own.
Durability comes from a client snapshot frame (content export plus compacted Yjs state) that each
room debounces and hands to a caller-supplied `SnapshotSink`. Joins are checked by a caller-supplied
`TokenVerifier`. Keep the library product-neutral: no JWT, database, broker or RPC code here.

## Using go-yjs-relay

- Entry points: `yjsrelay.New(sink, opts...)` returns the `*Hub`; `yjsrelay.NewHandler(hub, verifier)`
  is the `http.Handler` clients connect to.
- Caller interfaces: `TokenVerifier` (returns a `Join`: room, `Identity`, room attrs) and
  `SnapshotSink` (`Load`, `Save`). Optional: `SessionObserver`.
- Publish flow: `Hub.Flush` before a publish (blocks until the sink answers; a `*FlushError` means do
  not publish), `Hub.Freeze` after it. `Hub.Lookup` and `Hub.RoomCount` never open a room.
- Errors: `ErrRejected`, `ErrUnavailable`, `ErrClosed`, `*RejectError` (a sink's client-visible
  refusal), `*FlushError`. go-apperr codes are the caller's, through `WithCodes`.
- The wire format lives in `protocol/`; its fixtures in `protocol/yprotocols_test.go` came from the
  real JavaScript stack and must not be regenerated from this package.

## Layout

- `*.go` (root) - hub, room, client, handler, the caller interfaces, options and errors
- `protocol/` - y-websocket and extension frame encoders and decoders
- `internal/debounce/` - the snapshot debouncer
- `*_test.go` - black-box tests over a real socket (`helpers_test.go` has the fakes);
  `resync_internal_test.go` covers the few paths a socket cannot drive deterministically

## Build, test, lint

- Build: `task build`
- Test: `task test` (hermetic; no services needed)
- Lint: `task lint`
- License headers: `task license` (check) and `task license:fix`

## Logging

Follow the logging rules in `CLAUDE.md`. In short:

- Log generously: entry and exit of significant operations, decisions and branches, retries, state
  changes, external calls (target, duration, outcome), and every error with its context.
- Levels: `trace` for step-by-step detail, `debug` for flow, `info` for lifecycle, `warn` and
  `error` for problems. The environment filters the volume, so err on the side of too much.
- Environments: local dev `trace` with `LOG_FORMAT=console` (never JSON), dev cluster `debug`,
  qa/staging `info`, production `error`. Every cluster environment logs JSON. Set levels through
  `LOG_LEVEL` and `LOG_FORMAT`, never in code; local settings live in the run target or
  `.env.example`.
- Never log secrets, tokens, or personal data, not even at `trace`. Log an opaque or keyed ID.

## Conventions and gotchas

- See `CLAUDE.md` for the branch/commit/PR rules; they are enforced by the git hooks in
  `.claude/hooks` (run `bash .claude/hooks/install.sh` once per clone).
- Open every PR as a draft. CI skips drafts, so run the full checks locally, push once they pass,
  and mark the PR ready when the work is finished; see CLAUDE.md "CI and Actions minutes".
- Never decode or merge Yjs on the server, and never save per update: state changes only on a
  snapshot. A zero-length update or Sync Step-2 payload is invalid Yjs; use
  `protocol.EmptyDocumentUpdate`.
- Never log tokens or display names. User ids are logged as opaque ids.
