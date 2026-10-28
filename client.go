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
	"sync/atomic"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Bugs5382/go-yjs-relay/protocol"
	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	sendBuffer = 128
)

// client is one connected socket.
type client struct {
	room *room
	conn *websocket.Conn
	send chan []byte
	id   Identity
	user awarenessUser

	// resync is set when a document frame had to be dropped because the send
	// buffer was full. Yjs has no gap detection, so the peer would stay
	// behind for good; the write pump repairs it once the buffer drains.
	resync atomic.Bool

	// awarenessIDs are the Yjs client ids this socket announced. Only the
	// read pump touches it. On disconnect the room clears them.
	awarenessIDs map[uint32]struct{}
}

func newClient(r *room, conn *websocket.Conn, id Identity) *client {
	c := &client{
		room:         r,
		conn:         conn,
		send:         make(chan []byte, sendBuffer),
		id:           id,
		user:         newAwarenessUser(id, r.hub.cfg.palette),
		awarenessIDs: make(map[uint32]struct{}),
	}
	// Open the handshake the way y-websocket's server does, with a Sync
	// Step-1. It carries the empty state vector: the only honest one for a
	// relay that keeps no Y.Doc. The writer is always the write pump.
	c.send <- protocol.EncodeSync1(protocol.EmptyStateVector())
	r.add(c)
	go c.writePump()
	go c.readPump()
	return c
}

func (c *client) readPump() {
	r := c.room
	defer func() {
		// Clear presence before unregistering, so peers see the cursor go now.
		r.releaseAwareness(c.awarenessIDs)
		r.remove(c)
		_ = c.conn.Close()
	}()
	c.conn.SetReadLimit(r.hub.cfg.readLimit)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	// Closing the conn unblocks ReadMessage when the room stops.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-r.ctx.Done():
			_ = c.conn.Close()
		case <-done:
		}
	}()

	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure, websocket.CloseAbnormalClosure) {
				r.logger.Warn("read failed", log.F("error", err.Error()), log.F("user_id", c.id.UserID))
			} else {
				log.Trace(r.logger, "read loop ended", log.F("error", err.Error()), log.F("user_id", c.id.UserID))
			}
			return
		}
		c.handleMessage(msg)
	}
}

func (c *client) handleMessage(msg []byte) {
	r := c.room
	msgType, payload, err := protocol.DecodeMessage(msg)
	if err != nil {
		r.logger.Warn("decode message", log.F("error", err.Error()), log.F("user_id", c.id.UserID))
		return
	}
	log.Trace(r.logger, "frame received", log.F("msg_type", msgType), log.F("bytes", len(msg)), log.F("user_id", c.id.UserID))
	switch msgType {
	case protocol.MsgSync:
		syncType, data, err := protocol.DecodeSyncMessage(payload)
		if err != nil {
			r.logger.Warn("decode sync message", log.F("error", err.Error()), log.F("user_id", c.id.UserID))
			return
		}
		switch syncType {
		case protocol.SyncStep1:
			// The relay cannot compute a diff, so it answers with its whole
			// state.
			c.queueSync(protocol.EncodeSync2(r.documentUpdate()))
		case protocol.SyncStep2, protocol.SyncUpdate:
			r.applyUpdate(data)
		}
	case protocol.MsgAwareness:
		body, err := protocol.DecodeAwarenessMessage(payload)
		if err != nil {
			r.logger.Warn("decode awareness message", log.F("error", err.Error()), log.F("user_id", c.id.UserID))
			return
		}
		// The relay is authoritative for the user block, so a client cannot
		// show up as someone else.
		bound, entries, err := bindAwarenessIdentity(body, c.user)
		if err != nil {
			r.logger.Warn("bind awareness identity", log.F("error", err.Error()), log.F("user_id", c.id.UserID))
			return
		}
		c.trackAwareness(entries)
		r.recordAwareness(entries)
		r.broadcastAwareness(protocol.EncodeAwareness(bound))
	case protocol.MsgSnapshot:
		content, state, err := protocol.DecodeSnapshotMessage(payload)
		if err != nil {
			r.logger.Warn("decode snapshot message", log.F("error", err.Error()), log.F("user_id", c.id.UserID))
			return
		}
		// Attributed to this connection's verified identity, never to
		// anything the frame could assert.
		r.recordSnapshot(content, state, c.id)
	case protocol.MsgQueryAwareness:
		if frame := r.awarenessState(); frame != nil {
			c.queueAwareness(frame)
		}
	default:
		// MsgAuth and MsgControl travel server to client only.
		r.logger.Debug("unhandled message type", log.F("msg_type", msgType), log.F("user_id", c.id.UserID))
	}
}

func (c *client) trackAwareness(entries []protocol.AwarenessEntry) {
	for _, e := range entries {
		if e.Cleared() {
			delete(c.awarenessIDs, e.ClientID)
			continue
		}
		c.awarenessIDs[e.ClientID] = struct{}{}
	}
}

// queueSync hands a document frame to the write pump. A full buffer flags
// the client for resync rather than losing the frame silently.
func (c *client) queueSync(msg []byte) {
	select {
	case c.send <- msg:
	default:
		c.resync.Store(true)
		c.room.logger.Warn("send buffer full; dropped sync frame, scheduling resync", log.F("user_id", c.id.UserID))
	}
}

// queueAwareness hands a presence frame to the write pump. A drop is safe
// and does not trigger a resync: presence is re-advertised and times out.
func (c *client) queueAwareness(frame []byte) {
	select {
	case c.send <- frame:
	default:
		log.Trace(c.room.logger, "send buffer full; dropped presence frame", log.F("user_id", c.id.UserID))
	}
}

// queueControl hands a control frame to the write pump. Nothing re-sends a
// control frame and a document resync would not replace it, so a drop is
// logged loudly.
func (c *client) queueControl(frame []byte) {
	select {
	case c.send <- frame:
	default:
		c.room.logger.Warn("send buffer full; dropped control frame", log.F("user_id", c.id.UserID))
	}
}

func (c *client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() { ticker.Stop(); _ = c.conn.Close() }()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.write(websocket.BinaryMessage, msg); err != nil {
				return
			}
			if err := c.repairIfStale(); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.write(websocket.PingMessage, nil); err != nil {
				return
			}
			// Also here, so a client that fell behind and went quiet is still
			// repaired, at worst one ping period later.
			if err := c.repairIfStale(); err != nil {
				return
			}
		case <-c.room.ctx.Done():
			return
		}
	}
}

func (c *client) write(messageType int, msg []byte) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteMessage(messageType, msg)
}

// repairIfStale re-sends the room's state to a client that had a document
// frame dropped. It writes directly because the channel is what overflowed,
// and the write pump is the only writer. Yjs applies updates idempotently, so
// re-sending state the peer already has is harmless.
func (c *client) repairIfStale() error {
	if !c.resync.CompareAndSwap(true, false) {
		return nil
	}
	c.room.logger.Info("resyncing client after a dropped sync frame", log.F("user_id", c.id.UserID))
	return c.write(websocket.BinaryMessage, protocol.EncodeSync2(c.room.documentUpdate()))
}
