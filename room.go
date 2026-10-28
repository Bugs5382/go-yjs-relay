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
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Bugs5382/go-yjs-relay/internal/debounce"
	"github.com/Bugs5382/go-yjs-relay/protocol"
)

// room is one document's session: its clients, its presence map and the last
// client-compacted Yjs state.
//
// The room runs no Y.Doc. It never decodes, applies or merges Yjs: updates
// fan out to peers verbatim and convergence is the clients' job, which is
// what a CRDT buys. Its state is whatever full state a client last handed
// over in a snapshot frame, stored verbatim and served back on rehydration.
// That is safe because the authoritative copy is the content riding the same
// frame, which the sink owns; the Yjs state is a resume convenience.
// Concatenating updates instead would not even be valid Yjs: a rehydrated
// client decodes only the first update and drops the rest.
type room struct {
	id     string
	attrs  map[string]string
	hub    *Hub
	logger log.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu sync.RWMutex
	// state changes only on snapshot, never per update.
	state []byte
	// pending is the checkpoint waiting for the debounce.
	pending *pendingSnapshot
	frozen  bool

	// flushMu serializes saves, so a Flush that finds nothing pending still
	// waits for a debounced save already in flight. It also guards lastErr.
	flushMu sync.Mutex
	// lastErr is the sink's refusal of the newest checkpoint sent, nil once a
	// newer one is accepted.
	lastErr *FlushError

	register   chan *client
	unregister chan *client
	broadcast  chan []byte
	// awareness and control carry fully encoded frames fanned out as they
	// are. They must never go through broadcast, which wraps its payload in
	// a sync update frame.
	awareness chan []byte
	control   chan []byte

	// awarenessMu guards awarenessStates, kept apart from mu so presence
	// traffic never contends with the document lock. A y-websocket client
	// queries presence on connect; without this map a joiner would think it
	// is alone until a peer next moved. Keyed by Yjs client id.
	awarenessMu     sync.Mutex
	awarenessStates map[uint32]protocol.AwarenessEntry

	// clients is touched only by run.
	clients     map[*client]struct{}
	clientCount atomic.Int64
	// joining counts joins accepted by the hub but not yet registered.
	joining atomic.Int64

	debouncer *debounce.Debouncer
}

func newRoom(id string, attrs map[string]string, h *Hub) *room {
	ctx, cancel := context.WithCancel(h.ctx)
	return &room{
		id:              id,
		attrs:           maps.Clone(attrs),
		hub:             h,
		logger:          h.logger.With(log.F("room", id)),
		ctx:             ctx,
		cancel:          cancel,
		register:        make(chan *client, 8),
		unregister:      make(chan *client, 8),
		broadcast:       make(chan []byte, 64),
		awareness:       make(chan []byte, 64),
		control:         make(chan []byte, 16),
		awarenessStates: make(map[uint32]protocol.AwarenessEntry),
		clients:         make(map[*client]struct{}),
		debouncer:       debounce.New(h.cfg.debounce),
	}
}

// documentUpdate is the room's state as a payload safe for a Sync Step-2:
// the stored state, or the empty-document update when there is none, since a
// zero-length update is not valid Yjs.
func (r *room) documentUpdate() []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.state) == 0 {
		return protocol.EmptyDocumentUpdate()
	}
	return bytes.Clone(r.state)
}

// applyUpdate relays an update to every client. It never touches the state
// or the sink: the relay cannot merge updates, and saving per update made
// storage grow without bound.
func (r *room) applyUpdate(update []byte) {
	if len(update) == 0 {
		// Every peer's Y.applyUpdate would throw on it.
		log.Trace(r.logger, "dropped zero-length update")
		return
	}
	if r.isFrozen() {
		r.logger.Debug("dropped update for a frozen room")
		return
	}
	select {
	case r.broadcast <- update:
	case <-r.ctx.Done():
	}
}

// recordSnapshot keeps a client checkpoint and arms the debounced save. Only
// a snapshot arms it: an update alone has nothing to save.
func (r *room) recordSnapshot(content, state []byte, author Identity) {
	if len(content) == 0 && len(state) == 0 {
		log.Trace(r.logger, "ignored empty snapshot", log.F("user_id", author.UserID))
		return
	}
	r.mu.Lock()
	if r.frozen {
		r.mu.Unlock()
		r.logger.Debug("refused snapshot for a frozen room", log.F("user_id", author.UserID))
		return
	}
	r.pending = &pendingSnapshot{content: bytes.Clone(content), state: bytes.Clone(state), author: author}
	r.mu.Unlock()
	r.logger.Debug("snapshot recorded", log.F("user_id", author.UserID),
		log.F("content_bytes", len(content)), log.F("state_bytes", len(state)))
	r.debouncer.Trigger(func() { r.flushQuiet(r.ctx, "debounce") })
}

func (r *room) hasPending() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pending != nil
}

// flush cancels the debounce and saves now. The error is non-nil whenever the
// sink does not hold the newest checkpoint, including a standing rejection.
func (r *room) flush(ctx context.Context) (bool, error) {
	r.debouncer.Stop()
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	flushed := r.flushPendingLocked(ctx)
	if r.lastErr != nil {
		return flushed, r.lastErr
	}
	return flushed, nil
}

// flushQuiet saves for the debounce and the last-client-out path, where the
// outcome reaches editors over the control channel instead of a caller.
func (r *room) flushQuiet(ctx context.Context, trigger string) {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	if r.flushPendingLocked(ctx) {
		log.Trace(r.logger, "checkpoint flushed", log.F("trigger", trigger))
	}
}

// flushPendingLocked takes the pending checkpoint, adopts its state, saves it
// and tells the clients what the sink decided. The caller holds flushMu.
func (r *room) flushPendingLocked(ctx context.Context) bool {
	r.mu.Lock()
	p := r.pending
	r.pending = nil
	if p != nil && len(p.state) > 0 {
		// Adopted even if the sink then refuses the content: the peers'
		// documents already hold these changes, so the room has to be able
		// to rehydrate to what everyone is editing.
		r.state = p.state
	}
	r.mu.Unlock()
	if p == nil {
		return false
	}
	sink := r.hub.sink
	if sink == nil {
		log.Trace(r.logger, "pure relay: checkpoint dropped")
		return true
	}

	start := time.Now()
	err := sink.Save(ctx, Snapshot{
		Room:      r.id,
		RoomAttrs: maps.Clone(r.attrs),
		Content:   p.content,
		State:     p.state,
		Author:    p.author,
	})
	took := time.Since(start).String()
	if err == nil {
		r.lastErr = nil
		r.logger.Debug("sink save accepted", log.F("user_id", p.author.UserID), log.F("duration", took))
		r.broadcastControl(protocol.ControlMessage{Type: protocol.ControlSnapshotAccepted, Room: r.id})
		return true
	}

	fe := newFlushError(r.id, err)
	r.lastErr = fe
	if fe.Transient() {
		r.logger.Warn("sink save failed; keeping the checkpoint for a retry",
			log.F("error", err.Error()), log.F("duration", took))
		r.requeue(p)
	} else {
		r.logger.Warn("sink save rejected", log.F("reason", fe.Reason), log.F("error", err.Error()), log.F("duration", took))
	}
	r.broadcastControl(protocol.ControlMessage{
		Type:   protocol.ControlSnapshotRejected,
		Room:   r.id,
		Reason: fe.Reason,
		Detail: fe.Detail,
	})
	return true
}

// requeue puts a checkpoint that failed transiently back, unless a newer one
// arrived meanwhile or the room froze.
func (r *room) requeue(p *pendingSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil && !r.frozen {
		r.pending = p
	}
}

// freeze latches read-only before it flushes, so nothing new arrives during
// the flush, then tells every editor.
func (r *room) freeze(ctx context.Context, version string) {
	r.mu.Lock()
	r.frozen = true
	r.mu.Unlock()
	if _, err := r.flush(ctx); err != nil {
		r.logger.Warn("freeze: flush did not store the newest checkpoint", log.F("error", err.Error()))
	}
	r.broadcastControl(protocol.ControlMessage{Type: protocol.ControlRoomFrozen, Room: r.id, Version: version})
}

func (r *room) isFrozen() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.frozen
}

func (r *room) run() {
	for {
		select {
		case <-r.ctx.Done():
			log.Trace(r.logger, "room loop stopped")
			return
		case c := <-r.register:
			// Ask the peers already here to re-advertise. The room holds only
			// the last compacted state, so anything typed since lives only in
			// the peers; each answers with a Sync Step-2 that reaches the
			// joiner. Without it a mid-session joiner starts from stale content.
			for peer := range r.clients {
				peer.queueSync(protocol.EncodeSync1(protocol.EmptyStateVector()))
			}
			r.clients[c] = struct{}{}
			r.clientCount.Store(int64(len(r.clients)))
			r.joining.Add(-1)
			r.logger.Debug("client joined", log.F("user_id", c.id.UserID), log.F("editors", len(r.clients)))
			r.observe(c, true)
		case c := <-r.unregister:
			if _, ok := r.clients[c]; !ok {
				continue
			}
			delete(r.clients, c)
			r.clientCount.Store(int64(len(r.clients)))
			close(c.send)
			r.logger.Debug("client left", log.F("user_id", c.id.UserID), log.F("editors", len(r.clients)))
			r.observe(c, false)
			if len(r.clients) == 0 {
				// Last one out: save before the hub forgets the room, or every
				// edit since the last debounce is lost. Off the loop so a slow
				// sink cannot stall it, and the room is dropped only after.
				go func() {
					r.flushQuiet(r.ctx, "last client left")
					r.hub.removeRoom(r)
				}()
			}
		case update := <-r.broadcast:
			msg := protocol.EncodeUpdate(update)
			log.Trace(r.logger, "fan out update", log.F("bytes", len(update)), log.F("editors", len(r.clients)))
			for c := range r.clients {
				// queueSync, never a silent drop: a lost document frame
				// desyncs that client for good.
				c.queueSync(msg)
			}
		case frame := <-r.awareness:
			for c := range r.clients {
				c.queueAwareness(frame)
			}
		case frame := <-r.control:
			for c := range r.clients {
				c.queueControl(frame)
			}
		}
	}
}

func (r *room) observe(c *client, joined bool) {
	o := r.hub.cfg.observer
	if o == nil {
		return
	}
	s := Session{Room: r.id, RoomAttrs: maps.Clone(r.attrs), Identity: c.id}
	ctx := context.WithoutCancel(r.ctx)
	go func() {
		if joined {
			o.SessionJoined(ctx, s)
		} else {
			o.SessionLeft(ctx, s)
		}
	}()
}

// broadcastAwareness queues a presence frame for fan-out. A full buffer
// drops it, which is safe: presence is transient and re-advertised.
func (r *room) broadcastAwareness(frame []byte) {
	select {
	case r.awareness <- frame:
	default:
		log.Trace(r.logger, "awareness buffer full; dropped presence frame")
	}
}

// broadcastControl queues a control frame for fan-out. Unlike presence it is
// never re-sent, so a drop is logged: a lost snapshot.rejected leaves an
// editor believing its edits are safe.
func (r *room) broadcastControl(msg protocol.ControlMessage) {
	frame, err := protocol.EncodeControl(msg)
	if err != nil {
		r.logger.Error(err, "encode control frame", log.F("type", msg.Type))
		return
	}
	select {
	case r.control <- frame:
	default:
		r.logger.Warn("control buffer full; dropped control frame", log.F("type", msg.Type))
	}
}

// recordAwareness folds identity-bound entries into the presence map: a stale
// clock is ignored and a cleared entry removes the peer, as y-protocols'
// applyAwarenessUpdate does.
func (r *room) recordAwareness(entries []protocol.AwarenessEntry) {
	r.awarenessMu.Lock()
	defer r.awarenessMu.Unlock()
	for _, e := range entries {
		if prev, ok := r.awarenessStates[e.ClientID]; ok && e.Clock < prev.Clock {
			continue
		}
		if e.Cleared() {
			delete(r.awarenessStates, e.ClientID)
			continue
		}
		r.awarenessStates[e.ClientID] = e
	}
}

// awarenessState is the reply to a presence query: a frame describing every
// peer present, or nil when nobody is.
func (r *room) awarenessState() []byte {
	r.awarenessMu.Lock()
	entries := make([]protocol.AwarenessEntry, 0, len(r.awarenessStates))
	for _, e := range r.awarenessStates {
		entries = append(entries, e)
	}
	r.awarenessMu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	sortAwarenessEntries(entries)
	return protocol.EncodeAwareness(protocol.EncodeAwarenessUpdate(entries))
}

// releaseAwareness drops a departing peer's client ids and fans out cleared
// entries for them, so peers remove the cursor at once instead of waiting out
// the client-side timeout.
func (r *room) releaseAwareness(clientIDs map[uint32]struct{}) {
	if len(clientIDs) == 0 {
		return
	}
	r.awarenessMu.Lock()
	cleared := make([]protocol.AwarenessEntry, 0, len(clientIDs))
	for id := range clientIDs {
		prev, ok := r.awarenessStates[id]
		if !ok {
			continue
		}
		delete(r.awarenessStates, id)
		// The clock has to advance or peers discard the clear as stale.
		cleared = append(cleared, protocol.AwarenessEntry{
			ClientID: id,
			Clock:    prev.Clock + 1,
			State:    []byte(protocol.AwarenessStateCleared),
		})
	}
	r.awarenessMu.Unlock()
	if len(cleared) == 0 {
		return
	}
	sortAwarenessEntries(cleared)
	r.broadcastAwareness(protocol.EncodeAwareness(protocol.EncodeAwarenessUpdate(cleared)))
}

func (r *room) add(c *client) {
	select {
	case r.register <- c:
	case <-r.ctx.Done():
	}
}

func (r *room) remove(c *client) {
	select {
	case r.unregister <- c:
	case <-r.ctx.Done():
	}
}
