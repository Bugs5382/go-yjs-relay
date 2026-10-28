package protocol_test

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

import (
	"encoding/hex"
	"testing"

	"github.com/Bugs5382/go-yjs-relay/protocol"
)

// These fixtures are the wire-compatibility contract with the real Yjs stack.
// Each was produced by the JavaScript packages a browser client runs (yjs
// 13.6.32, y-protocols 1.0.7, lib0 0.2.117), not by this package. A Go-to-Go
// round trip cannot catch a framing bug: a self-consistent encoder and decoder
// pass it while staying unreadable to every real client.
//
// Fixture doc: a Y.Doc with clientID 1193046 whose Y.Text "root" is
// "hello collab" (state vector: client 1193046 at clock 12).
const (
	// Y.encodeStateVector(doc)
	realStateVector = "01d6e8480c"
	// Y.encodeStateVector(new Y.Doc())
	realEmptyStateVector = "00"
	// Y.encodeStateAsUpdate(doc)
	realFullUpdate = "0101d6e84800040104726f6f740c68656c6c6f20636f6c6c616200"

	// messageSync + syncProtocol.writeSyncStep1(encoder, doc)
	realSyncStep1 = "00000501d6e8480c"
	// messageSync + syncProtocol.writeSyncStep1(encoder, new Y.Doc())
	realSyncStep1Empty = "00000100"
	// messageSync + syncProtocol.writeSyncStep2(encoder, doc, emptyStateVector)
	realSyncStep2 = "00011b0101d6e84800040104726f6f740c68656c6c6f20636f6c6c616200"
	// messageSync + syncProtocol.writeUpdate(encoder, update) for a one-char insert
	realSyncUpdate      = "00020e01010700040104726f6f74017800"
	realSyncUpdateInner = "01010700040104726f6f74017800"

	// messageAwareness + writeVarUint8Array(encodeAwarenessUpdate(awareness, [clientID]))
	// for a peer asserting user {uid: spoofed-uid, name: Impostor, color: #000000}
	// plus a cursor, at awareness clock 2.
	realAwareness      = "016701d6e84802617b2275736572223a7b22756964223a2273706f6f6665642d756964222c226e616d65223a22496d706f73746f72222c22636f6c6f72223a2223303030303030227d2c22637572736f72223a7b22616e63686f72223a312c2268656164223a347d7d"
	realAwarenessInner = "01d6e84802617b2275736572223a7b22756964223a2273706f6f6665642d756964222c226e616d65223a22496d706f73746f72222c22636f6c6f72223a2223303030303030227d2c22637572736f72223a7b22616e63686f72223a312c2268656164223a347d7d"

	// messageQueryAwareness: a bare message type, no payload.
	realQueryAwareness = "03"

	realFixtureClientID       = 1193046
	realFixtureAwarenessClock = 2
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad fixture hex %q: %v", s, err)
	}
	return b
}

// TestEncodeMatchesRealYProtocolsBytes requires each encoder to produce
// byte-for-byte the frame the real y-protocols encoder produced.
func TestEncodeMatchesRealYProtocolsBytes(t *testing.T) {
	cases := []struct {
		name string
		got  []byte
		want string
	}{
		{"sync step 1 (state vector)", protocol.EncodeSync1(mustHex(t, realStateVector)), realSyncStep1},
		{"sync step 1 (empty state vector)", protocol.EncodeSync1(protocol.EmptyStateVector()), realSyncStep1Empty},
		{"sync step 2 (full update)", protocol.EncodeSync2(mustHex(t, realFullUpdate)), realSyncStep2},
		{"sync update", protocol.EncodeUpdate(mustHex(t, realSyncUpdateInner)), realSyncUpdate},
		{"awareness", protocol.EncodeAwareness(mustHex(t, realAwarenessInner)), realAwareness},
		{"query awareness", protocol.EncodeQueryAwareness(), realQueryAwareness},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hex.EncodeToString(tc.got); got != tc.want {
				t.Fatalf("frame is not what y-protocols emits\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

func TestEmptyStateVectorMatchesYjs(t *testing.T) {
	if got := hex.EncodeToString(protocol.EmptyStateVector()); got != realEmptyStateVector {
		t.Fatalf("EmptyStateVector: got %s want %s", got, realEmptyStateVector)
	}
}

// TestDecodeRealYProtocolsSyncFrames requires real frames to decode with no
// length prefix leaking into the payload.
func TestDecodeRealYProtocolsSyncFrames(t *testing.T) {
	cases := []struct {
		name         string
		frame        string
		wantSyncType uint32
		wantData     string
	}{
		{"step 1", realSyncStep1, protocol.SyncStep1, realStateVector},
		{"step 1 empty", realSyncStep1Empty, protocol.SyncStep1, realEmptyStateVector},
		{"step 2", realSyncStep2, protocol.SyncStep2, realFullUpdate},
		{"update", realSyncUpdate, protocol.SyncUpdate, realSyncUpdateInner},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msgType, payload, err := protocol.DecodeMessage(mustHex(t, tc.frame))
			if err != nil {
				t.Fatalf("DecodeMessage: %v", err)
			}
			if msgType != protocol.MsgSync {
				t.Fatalf("msgType: got %d want %d", msgType, protocol.MsgSync)
			}
			syncType, data, err := protocol.DecodeSyncMessage(payload)
			if err != nil {
				t.Fatalf("DecodeSyncMessage: %v", err)
			}
			if syncType != tc.wantSyncType {
				t.Fatalf("syncType: got %d want %d", syncType, tc.wantSyncType)
			}
			if got := hex.EncodeToString(data); got != tc.wantData {
				t.Fatalf("payload leaked framing bytes\n got: %s\nwant: %s", got, tc.wantData)
			}
		})
	}
}

func TestDecodeRealYProtocolsAwareness(t *testing.T) {
	msgType, payload, err := protocol.DecodeMessage(mustHex(t, realAwareness))
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if msgType != protocol.MsgAwareness {
		t.Fatalf("msgType: got %d want %d", msgType, protocol.MsgAwareness)
	}
	body, err := protocol.DecodeAwarenessMessage(payload)
	if err != nil {
		t.Fatalf("DecodeAwarenessMessage: %v", err)
	}
	if got := hex.EncodeToString(body); got != realAwarenessInner {
		t.Fatalf("awareness body\n got: %s\nwant: %s", got, realAwarenessInner)
	}
	entries, err := protocol.DecodeAwarenessUpdate(body)
	if err != nil {
		t.Fatalf("DecodeAwarenessUpdate: %v", err)
	}
	if len(entries) != 1 || entries[0].ClientID != realFixtureClientID || entries[0].Clock != realFixtureAwarenessClock {
		t.Fatalf("entries: got %+v", entries)
	}
	if got := hex.EncodeToString(protocol.EncodeAwarenessUpdate(entries)); got != realAwarenessInner {
		t.Fatalf("re-encoded awareness update\n got: %s\nwant: %s", got, realAwarenessInner)
	}
}

func TestDecodeRealQueryAwareness(t *testing.T) {
	msgType, payload, err := protocol.DecodeMessage(mustHex(t, realQueryAwareness))
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if msgType != protocol.MsgQueryAwareness {
		t.Fatalf("msgType: got %d want %d", msgType, protocol.MsgQueryAwareness)
	}
	if len(payload) != 0 {
		t.Fatalf("MsgQueryAwareness should carry no payload, got %x", payload)
	}
}

// TestSync1PayloadIsAStateVectorNotADocument: Yjs does not reject a document
// blob offered as a state vector, it misreads it. Y.decodeStateVector on the
// full-update fixture yields {client 1: clock 1193046}, so a relay that sent
// it would claim to hold 1.19M ops and the peer would send nothing.
func TestSync1PayloadIsAStateVectorNotADocument(t *testing.T) {
	frame := hex.EncodeToString(protocol.EncodeSync1(protocol.EmptyStateVector()))
	if frame != realSyncStep1Empty {
		t.Fatalf("connect Sync1 frame: got %s want %s", frame, realSyncStep1Empty)
	}
	if docFrame := hex.EncodeToString(protocol.EncodeSync1(mustHex(t, realFullUpdate))); docFrame == frame {
		t.Fatal("document blob and empty state vector produced the same Sync1 frame")
	}
}

// TestEmptyDocumentUpdateMatchesYjs: a zero-length payload is framed correctly
// but is not valid Yjs (Y.applyUpdate throws "Unexpected end of array").
func TestEmptyDocumentUpdateMatchesYjs(t *testing.T) {
	if got := hex.EncodeToString(protocol.EmptyDocumentUpdate()); got != "0000" {
		t.Fatalf("EmptyDocumentUpdate: got %s want 0000", got)
	}
	if got := hex.EncodeToString(protocol.EncodeSync2(protocol.EmptyDocumentUpdate())); got != "0001020000" {
		t.Fatalf("empty-doc Sync2 frame: got %s want 0001020000", got)
	}
}
