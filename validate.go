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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ValidateLexicalJSON is a cheap envelope check on a Lexical EditorState
// export, for a sink to run before it forwards content anywhere. It refuses
// empty input, malformed JSON, trailing data, a top-level value that is not
// an object, and a missing or non-object "root". It never looks at node
// types: deep validation belongs to whoever owns the content.
func ValidateLexicalJSON(content []byte) error {
	if len(content) == 0 {
		return errors.New("empty content")
	}
	dec := json.NewDecoder(bytes.NewReader(content))
	var doc map[string]json.RawMessage
	if err := dec.Decode(&doc); err != nil {
		return fmt.Errorf("top-level value must be a JSON object: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after the top-level object")
	}
	root, ok := doc["root"]
	if !ok {
		return errors.New("missing required field: root")
	}
	if raw := bytes.TrimSpace(root); len(raw) == 0 || raw[0] != '{' {
		return errors.New("root must be a JSON object")
	}
	return nil
}
