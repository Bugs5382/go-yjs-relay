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

import (
	"cmp"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"slices"

	"github.com/Bugs5382/go-yjs-relay/protocol"
)

// awarenessUser is the identity block the relay writes into every presence
// state's "user" key, replacing whatever the client put there.
type awarenessUser struct {
	UID        string `json:"uid"`
	Name       string `json:"name"`
	Color      string `json:"color"`
	ColorIndex int    `json:"colorIndex"`
}

func newAwarenessUser(id Identity, palette []string) awarenessUser {
	name := id.DisplayName
	if name == "" {
		name = id.UserID
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id.UserID))
	idx := int(h.Sum32() % uint32(len(palette))) // #nosec G115 -- a palette is a handful of colours
	return awarenessUser{UID: id.UserID, Name: name, Color: palette[idx], ColorIndex: idx}
}

// bindAwarenessIdentity rewrites the "user" key of every entry in an
// awareness update with the connection's verified identity, keeping every
// other key (cursor, selection, focus) byte for byte. Cleared entries pass
// through untouched. It returns the re-encoded update and the entries, so the
// room can record them without decoding twice.
func bindAwarenessIdentity(body []byte, user awarenessUser) ([]byte, []protocol.AwarenessEntry, error) {
	entries, err := protocol.DecodeAwarenessUpdate(body)
	if err != nil {
		return nil, nil, err
	}
	userJSON, err := json.Marshal(user)
	if err != nil {
		return nil, nil, fmt.Errorf("awareness: encode user: %w", err)
	}
	for i, e := range entries {
		if e.Cleared() {
			continue
		}
		fields := map[string]json.RawMessage{}
		if err := json.Unmarshal(e.State, &fields); err != nil {
			return nil, nil, fmt.Errorf("awareness: decode state: %w", err)
		}
		fields["user"] = userJSON
		state, err := json.Marshal(fields)
		if err != nil {
			return nil, nil, fmt.Errorf("awareness: encode state: %w", err)
		}
		entries[i].State = state
	}
	return protocol.EncodeAwarenessUpdate(entries), entries, nil
}

// sortAwarenessEntries orders entries by client id, so map iteration order
// never leaks into the wire.
func sortAwarenessEntries(entries []protocol.AwarenessEntry) {
	slices.SortFunc(entries, func(a, b protocol.AwarenessEntry) int { return cmp.Compare(a.ClientID, b.ClientID) })
}
