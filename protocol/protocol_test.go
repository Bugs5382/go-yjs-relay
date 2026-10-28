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
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Bugs5382/go-yjs-relay/protocol"
)

func TestEncodeDecodeSync1(t *testing.T) {
	stateVector := []byte{0x01, 0x02, 0x03}
	msgType, payload, err := protocol.DecodeMessage(protocol.EncodeSync1(stateVector))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msgType != protocol.MsgSync {
		t.Fatalf("expected MsgSync, got %d", msgType)
	}
	syncType, data, err := protocol.DecodeSyncMessage(payload)
	if err != nil {
		t.Fatalf("decode sync: %v", err)
	}
	if syncType != protocol.SyncStep1 {
		t.Fatalf("expected SyncStep1, got %d", syncType)
	}
	if !bytes.Equal(data, stateVector) {
		t.Fatalf("state vector mismatch: got %v want %v", data, stateVector)
	}
}

func TestEncodeDecodeUpdate(t *testing.T) {
	update := []byte{0xAB, 0xCD}
	msgType, payload, err := protocol.DecodeMessage(protocol.EncodeUpdate(update))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msgType != protocol.MsgSync {
		t.Fatalf("expected MsgSync, got %d", msgType)
	}
	syncType, data, err := protocol.DecodeSyncMessage(payload)
	if err != nil {
		t.Fatalf("decode sync: %v", err)
	}
	if syncType != protocol.SyncUpdate {
		t.Fatalf("expected SyncUpdate, got %d", syncType)
	}
	if !bytes.Equal(data, update) {
		t.Fatalf("update mismatch: got %v want %v", data, update)
	}
}

func TestEncodeAwareness(t *testing.T) {
	payload := []byte{0x10, 0x20}
	msgType, framed, err := protocol.DecodeMessage(protocol.EncodeAwareness(payload))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msgType != protocol.MsgAwareness {
		t.Fatalf("expected MsgAwareness, got %d", msgType)
	}
	// The awareness payload is a varUint8Array; reading DecodeMessage's
	// remainder directly would hide a missing length prefix.
	data, err := protocol.DecodeAwarenessMessage(framed)
	if err != nil {
		t.Fatalf("decode awareness: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("awareness data mismatch")
	}
}

func TestEncodeQueryAwarenessIsABareType(t *testing.T) {
	msgType, payload, err := protocol.DecodeMessage(protocol.EncodeQueryAwareness())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msgType != protocol.MsgQueryAwareness || len(payload) != 0 {
		t.Fatalf("got type %d payload %x", msgType, payload)
	}
}

func TestDecodeRejectsMalformedFrames(t *testing.T) {
	cases := map[string]func() error{
		"empty message": func() error { _, _, err := protocol.DecodeMessage(nil); return err },
		"varint overflow": func() error {
			_, _, err := protocol.DecodeMessage([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01})
			return err
		},
		"sync length past end": func() error {
			_, _, err := protocol.DecodeSyncMessage([]byte{protocol.SyncUpdate, 0x09, 0x01})
			return err
		},
		"awareness length past end": func() error {
			_, err := protocol.DecodeAwarenessMessage([]byte{0x05, 0x01})
			return err
		},
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			err := fn()
			if !errors.Is(err, protocol.ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

// TestSnapshotFrameRoundTrip pins the MsgSnapshot layout: the message type,
// then the content and the compacted Yjs state as two length-prefixed fields.
func TestSnapshotFrameRoundTrip(t *testing.T) {
	content := []byte(`{"root":{"type":"root","children":[]}}`)
	state := []byte{0x01, 0x02, 0xFF, 0x00}

	msgType, payload, err := protocol.DecodeMessage(protocol.EncodeSnapshot(content, state))
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if msgType != protocol.MsgSnapshot {
		t.Fatalf("msg type: got %d want %d", msgType, protocol.MsgSnapshot)
	}
	gotContent, gotState, err := protocol.DecodeSnapshotMessage(payload)
	if err != nil {
		t.Fatalf("DecodeSnapshotMessage: %v", err)
	}
	if !bytes.Equal(gotContent, content) {
		t.Errorf("content: got %q want %q", gotContent, content)
	}
	if !bytes.Equal(gotState, state) {
		t.Errorf("state: got %x want %x", gotState, state)
	}
}

// TestSnapshotFrameStateIsOptional keeps the frame forward and backward
// compatible: content alone is understood, and unknown trailing fields are
// ignored rather than fatal.
func TestSnapshotFrameStateIsOptional(t *testing.T) {
	content := []byte(`{"root":{}}`)
	// A content-only frame: the type, then one length-prefixed field.
	jsonOnly := append([]byte{protocol.MsgSnapshot, byte(len(content))}, content...)
	_, payload, err := protocol.DecodeMessage(jsonOnly)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	gotContent, gotState, err := protocol.DecodeSnapshotMessage(payload)
	if err != nil {
		t.Fatalf("content-only snapshot: %v", err)
	}
	if !bytes.Equal(gotContent, content) || len(gotState) != 0 {
		t.Fatalf("content-only snapshot: got (%q, %x)", gotContent, gotState)
	}

	withExtra := append(protocol.EncodeSnapshot(content, []byte{0x09}), 0x06, 'f', 'u', 't', 'u', 'r', 'e')
	_, payload, err = protocol.DecodeMessage(withExtra)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	gotContent, gotState, err = protocol.DecodeSnapshotMessage(payload)
	if err != nil {
		t.Fatalf("snapshot with trailing field: %v", err)
	}
	if !bytes.Equal(gotContent, content) || !bytes.Equal(gotState, []byte{0x09}) {
		t.Fatalf("snapshot with trailing field: got (%q, %x)", gotContent, gotState)
	}
}

// TestControlFrameRoundTrip pins the MsgControl layout and payload shape.
func TestControlFrameRoundTrip(t *testing.T) {
	want := protocol.ControlMessage{
		Type:   protocol.ControlSnapshotRejected,
		Room:   "doc-1",
		Reason: "rejected",
		Detail: "section count mismatch",
	}
	frame, err := protocol.EncodeControl(want)
	if err != nil {
		t.Fatalf("EncodeControl: %v", err)
	}
	msgType, payload, err := protocol.DecodeMessage(frame)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if msgType != protocol.MsgControl {
		t.Fatalf("msg type: got %d want %d", msgType, protocol.MsgControl)
	}
	got, err := protocol.DecodeControlMessage(payload)
	if err != nil {
		t.Fatalf("DecodeControlMessage: %v", err)
	}
	if got != want {
		t.Fatalf("control message: got %+v want %+v", got, want)
	}

	// The payload is plain JSON so a browser can read it with JSON.parse.
	body := payload[1:]
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("control payload is not JSON: %v", err)
	}
	if raw["type"] != protocol.ControlSnapshotRejected || raw["room"] != "doc-1" {
		t.Fatalf("control payload: got %v", raw)
	}
}

func TestAwarenessUpdateRoundTrip(t *testing.T) {
	entries := []protocol.AwarenessEntry{
		{ClientID: 3, Clock: 1, State: []byte(`{"a":1}`)},
		{ClientID: 300, Clock: 9, State: []byte(protocol.AwarenessStateCleared)},
	}
	got, err := protocol.DecodeAwarenessUpdate(protocol.EncodeAwarenessUpdate(entries))
	if err != nil {
		t.Fatalf("DecodeAwarenessUpdate: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("entries: got %d want 2", len(got))
	}
	for i := range entries {
		if got[i].ClientID != entries[i].ClientID || got[i].Clock != entries[i].Clock || !bytes.Equal(got[i].State, entries[i].State) {
			t.Fatalf("entry %d: got %+v want %+v", i, got[i], entries[i])
		}
	}
	if got[0].Cleared() || !got[1].Cleared() {
		t.Fatalf("Cleared: got %v/%v want false/true", got[0].Cleared(), got[1].Cleared())
	}
}

func TestDecodeAwarenessUpdateRejectsTruncatedBody(t *testing.T) {
	// One client announced, but the state field is cut short.
	if _, err := protocol.DecodeAwarenessUpdate([]byte{0x01, 0x07, 0x01, 0x09, '{'}); !errors.Is(err, protocol.ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed", err)
	}
}
