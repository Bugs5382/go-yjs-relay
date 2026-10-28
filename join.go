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

// TokenVerifier checks the join token a client presents, as a ?token= query
// parameter or an Authorization: Bearer header. It is the caller's code:
// signed JWTs, opaque session lookups or anything else. The relay never
// parses the token itself and never logs it.
type TokenVerifier interface {
	// Verify returns who is joining which room, or an error to refuse the
	// connection with 401. Join.Room must name the room the token was issued
	// for; the handler refuses a token used against any other room.
	Verify(ctx context.Context, token string) (Join, error)
}

// TokenVerifierFunc adapts a function to TokenVerifier.
type TokenVerifierFunc func(ctx context.Context, token string) (Join, error)

// Verify calls f.
func (f TokenVerifierFunc) Verify(ctx context.Context, token string) (Join, error) {
	return f(ctx, token)
}

// Join is a verified join token.
type Join struct {
	// Room is the room the token admits its holder to. Required.
	Room string
	// Identity is the verified user. Identity.UserID is required.
	Identity Identity
	// RoomAttrs describe the room (for example its tenant or the document
	// version it validates against). The first joiner's attrs are kept for
	// the room's life and handed to the sink with every snapshot.
	RoomAttrs map[string]string
}

// Identity is a verified user. The relay is authoritative for it: presence
// carries it in place of anything the client asserts, and snapshots are
// attributed to it.
type Identity struct {
	// UserID is a stable, opaque user id. It is logged; keep personal data
	// out of it.
	UserID string
	// DisplayName is the presence label. Empty falls back to UserID. It is
	// never logged.
	DisplayName string
	// Attrs carry anything else the sink or observer needs about the user,
	// for example the admin acting on their behalf.
	Attrs map[string]string
}

// Session is one client's membership of a room.
type Session struct {
	Room      string
	RoomAttrs map[string]string
	Identity  Identity
}

// SessionObserver hears clients join and leave rooms, for example to write
// an audit trail. Calls run on their own goroutine, so a slow observer never
// stalls a room.
type SessionObserver interface {
	SessionJoined(ctx context.Context, s Session)
	SessionLeft(ctx context.Context, s Session)
}
