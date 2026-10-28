// Package protocol encodes and decodes the y-websocket wire frames the relay
// speaks, plus two extension frames of its own.
//
// References:
//   - message envelope: https://github.com/yjs/y-websocket
//   - sync sub-protocol: https://github.com/yjs/y-protocols/blob/master/sync.js
//   - awareness: https://github.com/yjs/y-protocols/blob/master/awareness.js
//
// Every frame is a varUint message type followed by its payload. A MsgSync
// payload is a varUint sync type and a varUint8Array of data; a MsgAwareness
// payload is a varUint8Array holding the awareness update.
//
// The varUint8Array framing (a varUint byte length, then the bytes) is not
// optional. y-protocols writes it with lib0's writeVarUint8Array, and a stock
// Yjs client throws "Unexpected end of array" on a sync frame without it, or
// silently drops an update frame. Every encoder here writes the prefix and
// every decoder strips it.
//
// # Extension frames
//
// MsgSnapshot (client to server) and MsgControl (server to client) ride the
// same envelope and field framing, so a browser writes them with plain lib0
// encoders.
//
// MsgSnapshot is a client checkpoint:
//
//	[varUint 100]
//	[varUint8Array content]  // the editor's export, for example Lexical JSON
//	[varUint8Array state]    // Y.encodeStateAsUpdate(doc), may be empty
//
// Both fields are length-prefixed, so either may be empty, and a decoder
// ignores trailing fields it does not know. The frame carries no author: the
// relay attributes it to the connection's verified identity.
//
// MsgControl tells editors what they cannot see in the document stream:
//
//	[varUint 101]
//	[varUint8Array controlJSON]  // a JSON-encoded ControlMessage
package protocol

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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

// ErrMalformed matches every decode failure.
var ErrMalformed = errors.New("protocol: malformed frame")

// Top-level message types (y-websocket constants).
const (
	MsgSync           = 0
	MsgAwareness      = 1
	MsgAuth           = 2
	MsgQueryAwareness = 3
)

// Extension message types. y-websocket allocates 0 to 3; 100 and up leaves
// upstream room to add types without a collision.
const (
	// MsgSnapshot is a client checkpoint of content plus compacted state.
	MsgSnapshot = 100
	// MsgControl is a server control frame carrying a ControlMessage.
	MsgControl = 101
)

// Sync sub-message types.
const (
	SyncStep1  = 0
	SyncStep2  = 1
	SyncUpdate = 2
)

// ControlMessage is the JSON payload of a MsgControl frame. Type is always
// set; the other fields depend on it:
//
//   - ControlSnapshotAccepted: the sink stored the checkpoint.
//   - ControlSnapshotRejected: the checkpoint did not persist. Reason is a
//     stable token and Detail a human-readable cause. The room keeps running,
//     so the editor must surface the failure: its edits are not durable.
//   - ControlRoomFrozen: the document is now read-only (for example, it was
//     published as Version). The room refuses further edits and snapshots.
type ControlMessage struct {
	Type    string `json:"type"`
	Room    string `json:"room"`
	Reason  string `json:"reason,omitempty"`
	Detail  string `json:"detail,omitempty"`
	Version string `json:"version,omitempty"`
}

// ControlMessage Type values.
const (
	ControlSnapshotAccepted = "snapshot.accepted"
	ControlSnapshotRejected = "snapshot.rejected"
	ControlRoomFrozen       = "room.frozen"
)

// EmptyStateVector is the encoding of a state vector with no clients, what
// Y.encodeStateVector(new Y.Doc()) produces. A relay that keeps no Y.Doc can
// only truthfully offer this in Sync Step-1 ("send me everything"); a stored
// document offered in its place is misread as a set of client clocks, and the
// peer withholds updates the room needs.
func EmptyStateVector() []byte { return []byte{0x00} }

// EmptyDocumentUpdate is the update for an empty document, what
// Y.encodeStateAsUpdate(new Y.Doc()) produces. A zero-length update is not
// valid Yjs: Y.applyUpdate throws on it.
func EmptyDocumentUpdate() []byte { return []byte{0x00, 0x00} }

// DecodeMessage splits a frame into its message type and payload.
func DecodeMessage(msg []byte) (msgType uint32, payload []byte, err error) {
	msgType, payload, err = decodeVarUint(msg)
	if err != nil {
		return 0, nil, fmt.Errorf("decode message type: %w", err)
	}
	return msgType, payload, nil
}

// DecodeSyncMessage splits a MsgSync payload into its sync type and data,
// stripping the length prefix around the data.
func DecodeSyncMessage(payload []byte) (syncType uint32, data []byte, err error) {
	syncType, rest, err := decodeVarUint(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("decode sync type: %w", err)
	}
	data, _, err = decodeVarUint8Array(rest)
	if err != nil {
		return 0, nil, fmt.Errorf("decode sync payload: %w", err)
	}
	return syncType, data, nil
}

// DecodeAwarenessMessage strips the length prefix from a MsgAwareness payload,
// yielding the raw awareness update.
func DecodeAwarenessMessage(payload []byte) ([]byte, error) {
	body, _, err := decodeVarUint8Array(payload)
	if err != nil {
		return nil, fmt.Errorf("decode awareness payload: %w", err)
	}
	return body, nil
}

// EncodeSync1 builds a Sync Step-1 frame carrying a state vector.
func EncodeSync1(stateVector []byte) []byte {
	return concat(encodeVarUint(MsgSync), encodeVarUint(SyncStep1), encodeVarUint8Array(stateVector))
}

// EncodeSync2 builds a Sync Step-2 frame carrying an update.
func EncodeSync2(update []byte) []byte {
	return concat(encodeVarUint(MsgSync), encodeVarUint(SyncStep2), encodeVarUint8Array(update))
}

// EncodeUpdate builds a sync update frame.
func EncodeUpdate(update []byte) []byte {
	return concat(encodeVarUint(MsgSync), encodeVarUint(SyncUpdate), encodeVarUint8Array(update))
}

// EncodeAwareness wraps an awareness update in a MsgAwareness frame.
func EncodeAwareness(update []byte) []byte {
	return concat(encodeVarUint(MsgAwareness), encodeVarUint8Array(update))
}

// EncodeQueryAwareness builds the payload-free frame a client sends to ask
// for the room's presence.
func EncodeQueryAwareness() []byte { return encodeVarUint(MsgQueryAwareness) }

// EncodeSnapshot builds a MsgSnapshot frame. Browsers build it with lib0;
// this is the canonical Go encoder for tests and Go clients.
func EncodeSnapshot(content, state []byte) []byte {
	return concat(encodeVarUint(MsgSnapshot), encodeVarUint8Array(content), encodeVarUint8Array(state))
}

// DecodeSnapshotMessage parses a MsgSnapshot payload. A payload that ends
// after the content yields an empty state; trailing fields are ignored.
func DecodeSnapshotMessage(payload []byte) (content, state []byte, err error) {
	content, rest, err := decodeVarUint8Array(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("decode snapshot content: %w", err)
	}
	if len(rest) == 0 {
		return content, nil, nil
	}
	state, _, err = decodeVarUint8Array(rest)
	if err != nil {
		return nil, nil, fmt.Errorf("decode snapshot state: %w", err)
	}
	return content, state, nil
}

// EncodeControl builds a MsgControl frame carrying msg as JSON.
func EncodeControl(msg ControlMessage) ([]byte, error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode control message: %w", err)
	}
	return concat(encodeVarUint(MsgControl), encodeVarUint8Array(body)), nil
}

// DecodeControlMessage parses a MsgControl payload, for Go clients and tests.
func DecodeControlMessage(payload []byte) (ControlMessage, error) {
	body, _, err := decodeVarUint8Array(payload)
	if err != nil {
		return ControlMessage{}, fmt.Errorf("decode control payload: %w", err)
	}
	var msg ControlMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		return ControlMessage{}, fmt.Errorf("%w: control message: %w", ErrMalformed, err)
	}
	return msg, nil
}

func encodeVarUint(n uint32) []byte {
	buf := make([]byte, binary.MaxVarintLen32)
	return buf[:binary.PutUvarint(buf, uint64(n))]
}

// decodeVarUint reads a varint from the front of b. The wire value is
// untrusted and the protocol only encodes uint32, so anything larger is
// refused rather than wrapped.
func decodeVarUint(b []byte) (uint32, []byte, error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, fmt.Errorf("%w: invalid varint", ErrMalformed)
	}
	if v > math.MaxUint32 {
		return 0, nil, fmt.Errorf("%w: varint %d overflows uint32", ErrMalformed, v)
	}
	return uint32(v), b[n:], nil
}

// encodeVarUint8Array writes b with a varUint length prefix. lib0's
// writeVarString has the same layout, so it also encodes awareness states.
func encodeVarUint8Array(b []byte) []byte {
	return append(encodeVarUint(clampUint32(len(b))), b...)
}

func decodeVarUint8Array(b []byte) (data, rest []byte, err error) {
	n, rest, err := decodeVarUint(b)
	if err != nil {
		return nil, nil, err
	}
	if uint64(n) > uint64(len(rest)) {
		return nil, nil, fmt.Errorf("%w: length %d exceeds %d remaining bytes", ErrMalformed, n, len(rest))
	}
	return rest[:n], rest[n:], nil
}

func concat(parts ...[]byte) []byte {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	out := make([]byte, 0, total)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func clampUint32(n int) uint32 {
	switch {
	case n < 0:
		return 0
	case uint64(n) > math.MaxUint32:
		return math.MaxUint32
	default:
		return uint32(n) // #nosec G115 -- bounded by the guards above
	}
}
