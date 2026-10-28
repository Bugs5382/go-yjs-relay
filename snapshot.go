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

import "context"

// SnapshotSink is where a room's checkpoints go. It is the caller's code: a
// database row, an object store, an upstream service that validates the
// content, or several of those.
type SnapshotSink interface {
	// Load returns the room's last stored state, or nil when there is none.
	// It runs once when a room opens; an error refuses the join.
	Load(ctx context.Context, room string) ([]byte, error)

	// Save stores a checkpoint. A nil error tells the editors it is durable.
	// Return a *RejectError to refuse it with a reason the editors may see,
	// or an error matching ErrUnavailable (or a context error) when the
	// failure says nothing about the content, so the relay keeps the
	// checkpoint for a retry. Any other error refuses it without showing the
	// editors its text.
	Save(ctx context.Context, snap Snapshot) error
}

// Snapshot is one client checkpoint, sent by the debounce, by Flush, by
// Freeze, or when the last client leaves.
type Snapshot struct {
	Room      string
	RoomAttrs map[string]string
	// Content is the editor's export (for example Lexical JSON). It is the
	// authoritative copy of the document; the relay passes it through
	// untouched. It may be empty when a client sent only state.
	Content []byte
	// State is the client's compacted Y.encodeStateAsUpdate(doc), stored so
	// a room can rehydrate after a restart. The relay never decodes it. It
	// may be empty when a client sent only content.
	State []byte
	// Author is the verified identity of the connection that sent it.
	Author Identity
}

// pendingSnapshot is a checkpoint waiting for the debounce.
type pendingSnapshot struct {
	content []byte
	state   []byte
	author  Identity
}
