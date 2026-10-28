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
	"context"
	"errors"
	"maps"
	"sync"
	"time"

	log "github.com/Bugs5382/go-log"
)

// Hub owns the live rooms, one per document id.
//
// All of its state is in process memory. Two editors of one document who
// reach different processes get two rooms that never see each other, and
// both race to save. Run one replica per document: a single instance, or
// routing that pins a room id to one instance (a consistent hash at the
// ingress).
type Hub struct {
	sink   SnapshotSink
	cfg    config
	logger log.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	rooms  map[string]*room
	closed bool
}

// New returns a Hub that loads and saves room state through sink. A nil sink
// runs a pure relay: rooms start empty and checkpoints are dropped.
func New(sink SnapshotSink, opts ...Option) *Hub {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		sink:   sink,
		cfg:    cfg,
		logger: cfg.logger,
		ctx:    ctx,
		cancel: cancel,
		rooms:  make(map[string]*room),
	}
	h.logger.Info("relay hub started",
		log.F("debounce", cfg.debounce.String()), log.F("read_limit", cfg.readLimit), log.F("pure_relay", sink == nil))
	return h
}

// join returns the room for id, opening it (and loading its state from the
// sink) when it is not live. The room counts the caller as joining until
// the client registers or abandons the join, so the room cannot be dropped
// in between.
func (h *Hub) join(ctx context.Context, id string, attrs map[string]string) (*room, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	if r, ok := h.rooms[id]; ok {
		r.joining.Add(1)
		log.Trace(h.logger, "joining live room", log.F("room", id))
		return r, nil
	}
	var state []byte
	if h.sink != nil {
		start := time.Now()
		var err error
		state, err = h.sink.Load(ctx, id)
		if err != nil {
			h.logger.Error(err, "sink load failed", log.F("room", id), log.F("duration", time.Since(start).String()))
			return nil, err
		}
		h.logger.Debug("sink load", log.F("room", id), log.F("bytes", len(state)), log.F("duration", time.Since(start).String()))
	}
	r := newRoom(id, attrs, h)
	r.state = state
	r.joining.Add(1)
	h.rooms[id] = r
	go r.run()
	h.logger.Info("room opened", log.F("room", id), log.F("rooms", len(h.rooms)))
	return r, nil
}

// removeRoom drops r once it is empty. It is conditional because a client can
// join while the room's final flush runs; dropping the room then would strand
// that client, and the next joiner would open a second room for the document.
func (h *Hub) removeRoom(r *room) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[r.id] != r || r.clientCount.Load() > 0 || r.joining.Load() > 0 {
		h.logger.Debug("room kept: a client joined during its final flush", log.F("room", r.id))
		return
	}
	delete(h.rooms, r.id)
	r.debouncer.Stop()
	if r.hasPending() {
		h.logger.Warn("room closed with an unsaved checkpoint", log.F("room", r.id))
	}
	r.cancel()
	h.logger.Info("room closed", log.F("room", r.id), log.F("rooms", len(h.rooms)))
}

func (h *Hub) room(id string) *room {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rooms[id]
}

// FlushResult reports what Flush found.
type FlushResult struct {
	// RoomFound is false when nobody is editing the document. That is not an
	// error: the sink already holds the newest content.
	RoomFound bool
	// ContentFlushed is true when the call saved a pending checkpoint.
	ContentFlushed bool
}

// Flush saves the room's pending checkpoint now and returns once the sink
// has answered, waiting out a debounced save already in flight. Call it
// before publishing a document, so the published version is never older than
// what the editors hold.
//
// A non-nil error is a *FlushError: the sink does not hold the room's newest
// checkpoint, including a rejection no newer checkpoint has replaced yet, and
// the publish should not go ahead. Flush never opens a room.
func (h *Hub) Flush(ctx context.Context, id string) (FlushResult, error) {
	r := h.room(id)
	if r == nil {
		log.Trace(h.logger, "flush: no live room", log.F("room", id))
		return FlushResult{}, nil
	}
	flushed, err := r.flush(ctx)
	if err != nil {
		h.logger.Warn("flush: sink does not hold the newest checkpoint", log.F("room", id), log.F("error", err.Error()))
		return FlushResult{RoomFound: true, ContentFlushed: flushed}, h.cfg.codes.wrap(err)
	}
	h.logger.Debug("flushed", log.F("room", id), log.F("content_flushed", flushed))
	return FlushResult{RoomFound: true, ContentFlushed: flushed}, nil
}

// Freeze makes the room read-only, saves anything still pending, and sends
// every editor a room.frozen control frame carrying version, so editors stop
// typing into a document that can no longer change. Call it after the
// publish succeeded, so a failed publish never leaves a live room stuck
// read-only; Flush is the call that belongs before. It reports whether a
// room was live.
func (h *Hub) Freeze(ctx context.Context, id, version string) bool {
	r := h.room(id)
	if r == nil {
		h.logger.Debug("freeze: no live room", log.F("room", id))
		return false
	}
	r.freeze(ctx, version)
	h.logger.Info("room frozen", log.F("room", id), log.F("version", version), log.F("editors", r.clientCount.Load()))
	return true
}

// RoomInfo describes a live room.
type RoomInfo struct {
	ID string
	// Attrs are the first joiner's Join.RoomAttrs.
	Attrs map[string]string
	// Editors is the number of connected clients.
	Editors int
	// Frozen is true after Freeze.
	Frozen bool
}

// Lookup describes the live room for id. It never opens one, so "nobody is
// editing" stays distinguishable from "a room exists".
func (h *Hub) Lookup(id string) (RoomInfo, bool) {
	r := h.room(id)
	if r == nil {
		return RoomInfo{}, false
	}
	return RoomInfo{
		ID:      r.id,
		Attrs:   maps.Clone(r.attrs),
		Editors: int(r.clientCount.Load()),
		Frozen:  r.isFrozen(),
	}, true
}

// RoomCount returns the number of live rooms.
func (h *Hub) RoomCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.rooms)
}

// Close saves every room's pending checkpoint, then disconnects every client
// and stops the rooms. New joins fail with ErrClosed. The error joins every
// room's flush error. Close is safe to call more than once.
func (h *Hub) Close(ctx context.Context) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	rooms := make([]*room, 0, len(h.rooms))
	for _, r := range h.rooms {
		rooms = append(rooms, r)
	}
	h.mu.Unlock()

	h.logger.Info("relay hub closing", log.F("rooms", len(rooms)))
	var errs []error
	for _, r := range rooms {
		if _, err := r.flush(ctx); err != nil {
			h.logger.Warn("close: final flush failed", log.F("room", r.id), log.F("error", err.Error()))
			errs = append(errs, h.cfg.codes.wrap(err))
		}
	}
	h.cancel()
	h.logger.Info("relay hub closed")
	return errors.Join(errs...)
}
