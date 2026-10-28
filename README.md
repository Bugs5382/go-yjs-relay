# go-yjs-relay 🐹

> 🧭 A product-neutral Yjs relay for Go: per-room y-websocket sync, presence fan-out, debounced snapshots, and flush and freeze around a publish.

Collaborative editors built on [Yjs](https://yjs.dev) need a server that fans updates out to every
peer in a document's room. `go-yjs-relay` is that server as a library. It speaks the stock
y-websocket protocol, so `y-websocket`'s `WebsocketProvider` connects to it unchanged, and it runs
no `Y.Doc` of its own: updates pass through verbatim and the clients converge, which is what a CRDT
is for. 🔌 **You bring the two parts that are yours:** who may join a room, and where checkpoints go.

## ✨ Highlights

- 🔄 **y-websocket sync:** one room per document, Sync Step-1/Step-2 and updates, with a fresh joiner
  pulling the newest edits from the peers already there.
- 👥 **Presence you can trust:** awareness fans out to the room, and the relay rewrites every
  `user` block with the verified identity, so nobody can show up as someone else. Colours are stable
  per user.
- 💾 **Debounced snapshots:** clients send a checkpoint frame (content plus compacted Yjs state); the
  room saves the newest one after a quiet period, and again when the last editor leaves.
- 🧊 **Flush and freeze:** `Flush` forces the newest checkpoint into storage before you publish;
  `Freeze` turns the room read-only afterwards and tells every editor.
- 📣 **Outcomes reach the editor:** accepted and rejected checkpoints arrive as control frames, so an
  edit that did not persist is never silent.
- 🩹 **Self-repairing fan-out:** a client whose buffer overflows is resynced instead of drifting away
  for good.
- 🧱 **No product wiring:** no JWT, database, broker or RPC code. Logging through `go-log`, error
  codes through `go-apperr` when you supply them.

## 📦 Install

```bash
go get github.com/Bugs5382/go-yjs-relay
```

## 🚀 Usage

Implement a `TokenVerifier` (turns a join token into a room and a verified identity) and a
`SnapshotSink` (loads a room's stored state, saves checkpoints), then serve the handler:

```go
verifier := yjsrelay.TokenVerifierFunc(func(ctx context.Context, token string) (yjsrelay.Join, error) {
    claims, err := myTokens.Verify(ctx, token) // your signer, your claims
    if err != nil {
        return yjsrelay.Join{}, err
    }
    return yjsrelay.Join{
        Room:      claims.DocumentID,
        Identity:  yjsrelay.Identity{UserID: claims.UserID, DisplayName: claims.Name},
        RoomAttrs: map[string]string{"tenant": claims.Tenant},
    }, nil
})

hub := yjsrelay.New(sink, yjsrelay.WithLogger(log.NewLogger("collab")))
defer hub.Close(context.Background())

mux := http.NewServeMux()
mux.Handle("GET /rooms/{room}", yjsrelay.NewHandler(hub, verifier))
```

The client connects to `/rooms/<doc>?token=<token>` (or sends `Authorization: Bearer <token>`).
A token is only good for the room it names. Browser `Origin` headers are refused unless you list
them with `WithAllowedOrigins`; the intended setup is a server-side proxy in front of the relay.

Around a publish:

```go
// Before: the sink must hold the newest content, or the version is cut from stale content.
if _, err := hub.Flush(ctx, docID); err != nil {
    return err // a *yjsrelay.FlushError: rejected or unavailable
}
// ... publish ...
// After: every editor drops to read-only.
hub.Freeze(ctx, docID, "v7")
```

## 💾 Snapshots and the sink

The client sends a snapshot frame (`protocol.MsgSnapshot`, type 100) carrying its content export (for
example Lexical JSON) and its compacted `Y.encodeStateAsUpdate(doc)`. The relay attributes it to the
connection's verified identity, never to anything in the frame, and saves the newest one through
your sink:

```go
type SnapshotSink interface {
    Load(ctx context.Context, room string) ([]byte, error)
    Save(ctx context.Context, snap yjsrelay.Snapshot) error
}
```

🛡️ **What `Save` returns decides what editors see:**

- `nil`: a `snapshot.accepted` control frame.
- `&yjsrelay.RejectError{Reason, Detail}`: a `snapshot.rejected` frame carrying your reason and
  detail.
- an error wrapping `yjsrelay.ErrUnavailable` (or a context error): rejected as `unavailable`, and
  the checkpoint is kept for the next flush.
- any other error: rejected as `sink_error`, with its text kept server-side.

The room keeps the newest state in memory even when the sink refuses the content, because the peers
already hold those edits. `ValidateLexicalJSON` is a ready-made envelope check a sink can run first.
A `nil` sink runs a pure relay.

## 🏷️ Error codes

The library never picks go-apperr codes for you. Pass yours and `Flush` and `Close` attach them:

```go
hub := yjsrelay.New(sink, yjsrelay.WithCodes(yjsrelay.Codes{Rejected: 4221, Unavailable: 5031}))
```

`errors.Is(err, yjsrelay.ErrRejected)`, `errors.Is(err, yjsrelay.ErrUnavailable)` and
`errors.As(err, &flushErr)` keep working either way.

## 🔧 Options

- 🪵 **`WithLogger`:** a `go-log` logger; the default discards everything. Tokens and display names
  are never logged.
- ⏱️ **`WithDebounce`:** quiet period before a save (default 5s).
- 📏 **`WithReadLimit`:** largest inbound frame (default 2 MiB). A proxy in front must allow at least
  this much, or it closes the biggest snapshots first.
- 👀 **`WithSessionObserver`:** hear every join and leave, for example for an audit trail.
- 🎨 **`WithPalette`:** presence colours. Presence also carries `colorIndex`, so a client can map it
  onto a palette of its own.
- 🚪 **`WithAllowedOrigins` and `WithRoomFunc`:** handler options for the Origin allowlist and for
  reading the room id from a request.

## 📐 Scaling

All room state lives in process memory. Two editors of one document on different instances get two
rooms that never see each other. Run one instance, or route each room id to one instance (a
consistent hash at the ingress).

## 🧩 Packages

- `yjsrelay`: the hub, the handler and the two interfaces.
- `yjsrelay/protocol`: the wire encoders and decoders, pinned against bytes the real JavaScript
  `yjs`/`y-protocols` stack produced. Use it in Go clients and tests.

## 🛠 Develop

```bash
task build    # go build ./...
task test     # go test ./...
task lint     # gofmt check + golangci-lint + yamllint
task license  # check MIT headers (golic)
```

## ⚖️ License

MIT (c) 2026 Shane
