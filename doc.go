// Package yjsrelay is a product-neutral Yjs room relay: per-room y-websocket
// sync and awareness fan-out, server-authoritative presence, debounced
// snapshots, and flush and freeze around a publish.
//
// The relay runs no Y.Doc. Updates fan out to peers verbatim and the clients
// converge on their own. Durability comes from a snapshot frame the client
// sends (see package protocol): the editor's content export plus its
// compacted Yjs state. The room debounces those and hands the newest to a
// SnapshotSink you supply.
//
// You supply two things:
//
//   - a TokenVerifier, which turns a join token into a Join: the room, the
//     verified Identity and any room attributes;
//   - a SnapshotSink, which loads a room's stored state when it opens and
//     saves checkpoints.
//
// Then serve the Handler and use the Hub around a publish:
//
//	hub := yjsrelay.New(sink, yjsrelay.WithLogger(logger))
//	mux.Handle("GET /rooms/{room}", yjsrelay.NewHandler(hub, verifier))
//
//	// before publishing: the sink must hold the newest content
//	if _, err := hub.Flush(ctx, docID); err != nil { ... }
//	// after publishing: editors drop to read-only
//	hub.Freeze(ctx, docID, "v7")
//
// All state is in process memory: run one instance per document. Package
// protocol holds the wire encoders and decoders, for Go clients and tests.
package yjsrelay

/*
MIT License

Copyright (c) 2026 Shane

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
