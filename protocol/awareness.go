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

import "fmt"

// AwarenessStateCleared is the state a Yjs client writes when it drops its
// presence: y-protocols encodes states as JSON, and a cleared one is null.
const AwarenessStateCleared = "null"

// AwarenessEntry is one client's entry in an awareness update. ClientID is
// the Yjs client id (one user can hold several, for example two tabs), Clock
// is the entry's awareness clock, and State is its raw JSON.
type AwarenessEntry struct {
	ClientID uint32
	Clock    uint32
	State    []byte
}

// Cleared reports whether the entry drops the client's presence.
func (e AwarenessEntry) Cleared() bool { return string(e.State) == AwarenessStateCleared }

// DecodeAwarenessUpdate parses an awareness update body, the value
// DecodeAwarenessMessage returns:
//
//	varUint(numClients)
//	repeat numClients: varUint(clientID) varUint(clock) varString(stateJSON)
func DecodeAwarenessUpdate(body []byte) ([]AwarenessEntry, error) {
	numClients, rest, err := decodeVarUint(body)
	if err != nil {
		return nil, fmt.Errorf("awareness: decode client count: %w", err)
	}
	entries := make([]AwarenessEntry, 0, min(int(numClients), 64))
	for range numClients {
		clientID, r2, err := decodeVarUint(rest)
		if err != nil {
			return nil, fmt.Errorf("awareness: decode client id: %w", err)
		}
		clock, r3, err := decodeVarUint(r2)
		if err != nil {
			return nil, fmt.Errorf("awareness: decode clock: %w", err)
		}
		state, r4, err := decodeVarUint8Array(r3)
		if err != nil {
			return nil, fmt.Errorf("awareness: decode state: %w", err)
		}
		rest = r4
		entries = append(entries, AwarenessEntry{ClientID: clientID, Clock: clock, State: state})
	}
	return entries, nil
}

// EncodeAwarenessUpdate serializes entries into an awareness update body,
// ready for EncodeAwareness.
func EncodeAwarenessUpdate(entries []AwarenessEntry) []byte {
	out := encodeVarUint(clampUint32(len(entries)))
	for _, e := range entries {
		out = append(out, encodeVarUint(e.ClientID)...)
		out = append(out, encodeVarUint(e.Clock)...)
		out = append(out, encodeVarUint8Array(e.State)...)
	}
	return out
}
