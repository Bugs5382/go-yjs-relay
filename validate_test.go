package yjsrelay_test

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
	"testing"

	yjsrelay "github.com/Bugs5382/go-yjs-relay"
)

// ValidateLexicalJSON is a cheap envelope check a sink can run before it
// forwards content anywhere. Deep validation stays with the content owner.
func TestValidateLexicalJSON(t *testing.T) {
	valid := []string{
		`{"root":{"type":"root","children":[]}}`,
		`{"root":{"type":"root","children":[{"type":"section","key":"s1","children":[]}]}}`,
	}
	for _, v := range valid {
		if err := yjsrelay.ValidateLexicalJSON([]byte(v)); err != nil {
			t.Errorf("%s: expected valid, got %v", v, err)
		}
	}

	invalid := map[string]string{
		"empty":             ``,
		"not json":          `{not json}`,
		"trailing garbage":  `{"root":{"type":"root","children":[]}}xxx`,
		"missing root":      `{"other":"field"}`,
		"root string":       `{"root":"not-an-object"}`,
		"root number":       `{"root":42}`,
		"root array":        `{"root":[1,2,3]}`,
		"root null":         `{"root":null}`,
		"top-level array":   `[1,2,3]`,
		"top-level scalar":  `"hello"`,
		"second document":   `{"root":{}} {"root":{}}`,
		"broken root value": `{"root":{"a":}}`,
	}
	for name, v := range invalid {
		if err := yjsrelay.ValidateLexicalJSON([]byte(v)); err == nil {
			t.Errorf("%s: expected an error for %q", name, v)
		}
	}
}
