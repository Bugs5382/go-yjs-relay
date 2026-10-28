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
	"net/http"
	"slices"
	"time"

	log "github.com/Bugs5382/go-log"
)

const (
	// DefaultDebounce is how long a room waits after the last snapshot frame
	// before it saves.
	DefaultDebounce = 5 * time.Second
	// DefaultReadLimit caps one inbound frame. It has to fit the largest
	// legitimate frame, a snapshot carrying the full content and the full
	// compacted state together. A proxy in front of the relay must allow
	// frames at least this large, or it closes big snapshots first.
	DefaultReadLimit int64 = 2 << 20
)

// DefaultPalette is the presence colours a user is assigned from. The relay
// picks one from the user id, so a user keeps their colour across sessions
// and cannot pick another user's. Presence carries the index too, so a client
// can map it onto a palette of its own (for example per theme).
var DefaultPalette = []string{
	"#D93D42",
	"#C2570C",
	"#9A6B00",
	"#2B8A3E",
	"#0B7A75",
	"#2563EB",
	"#7C3AED",
	"#C026D3",
}

type config struct {
	logger    log.Logger
	debounce  time.Duration
	readLimit int64
	codes     Codes
	observer  SessionObserver
	palette   []string
}

func defaultConfig() config {
	return config{
		logger:    log.Nop(),
		debounce:  DefaultDebounce,
		readLimit: DefaultReadLimit,
		palette:   slices.Clone(DefaultPalette),
	}
}

// Option configures a Hub.
type Option func(*config)

// WithLogger sets the logger. The default discards everything.
func WithLogger(l log.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithDebounce sets how long a room waits after the last snapshot frame
// before it saves. The default is DefaultDebounce.
func WithDebounce(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.debounce = d
		}
	}
}

// WithReadLimit caps one inbound frame in bytes; a larger frame closes the
// connection. The default is DefaultReadLimit.
func WithReadLimit(n int64) Option {
	return func(c *config) {
		if n > 0 {
			c.readLimit = n
		}
	}
}

// WithCodes attaches the caller's go-apperr codes to the errors Flush and
// Close return.
func WithCodes(codes Codes) Option {
	return func(c *config) { c.codes = codes }
}

// WithSessionObserver reports every join and leave to o.
func WithSessionObserver(o SessionObserver) Option {
	return func(c *config) { c.observer = o }
}

// WithPalette replaces DefaultPalette. An empty palette is ignored.
func WithPalette(colors []string) Option {
	return func(c *config) {
		if len(colors) > 0 {
			c.palette = slices.Clone(colors)
		}
	}
}

type handlerConfig struct {
	allowedOrigins []string
	roomFunc       func(*http.Request) string
}

// HandlerOption configures a Handler.
type HandlerOption func(*handlerConfig)

// WithAllowedOrigins sets the browser Origin allowlist, compared
// case-insensitively against the whole Origin header (scheme, host and
// port). With none set, every request carrying an Origin header is refused
// with 403: the intended setup is a server-side proxy in front of the relay,
// which sends no Origin. A request without an Origin header is not a browser
// and is always allowed through to the token check.
func WithAllowedOrigins(origins ...string) HandlerOption {
	return func(c *handlerConfig) { c.allowedOrigins = slices.Clone(origins) }
}

// WithRoomFunc sets how the handler reads the room id from a request. The
// default uses the {room} path value when the route has one, and the last
// path segment otherwise.
func WithRoomFunc(f func(*http.Request) string) HandlerOption {
	return func(c *handlerConfig) {
		if f != nil {
			c.roomFunc = f
		}
	}
}
