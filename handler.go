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
	"errors"
	"net/http"
	"strings"

	log "github.com/Bugs5382/go-log"
	"github.com/gorilla/websocket"
)

const bearerChallenge = `Bearer error="invalid_token"` // #nosec G101 -- an auth challenge header, not a credential

// Handler upgrades HTTP requests to relay sockets after the TokenVerifier
// accepts the join token.
type Handler struct {
	hub      *Hub
	verifier TokenVerifier
	cfg      handlerConfig
	upgrader websocket.Upgrader
}

// NewHandler returns an http.Handler that admits clients to hub's rooms. The
// room id comes from the request (see WithRoomFunc), the token from a
// ?token= query parameter or an Authorization: Bearer header, and the token
// has to be for that room.
func NewHandler(hub *Hub, verifier TokenVerifier, opts ...HandlerOption) *Handler {
	h := &Handler{hub: hub, verifier: verifier, cfg: handlerConfig{roomFunc: defaultRoom}}
	for _, o := range opts {
		o(&h.cfg)
	}
	h.upgrader = websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     h.checkOrigin,
	}
	return h
}

// defaultRoom reads the {room} path value, or the last path segment.
func defaultRoom(r *http.Request) string {
	if v := r.PathValue("room"); v != "" {
		return v
	}
	p := strings.TrimRight(r.URL.Path, "/")
	return p[strings.LastIndex(p, "/")+1:]
}

// checkOrigin allows requests with no Origin header (a server-side proxy, or
// a non-browser client; the token is the credential either way) and browser
// origins on the allowlist. Allowing every origin would let any web page
// spend a token leaked from a victim's browser.
func (h *Handler) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	for _, allowed := range h.cfg.allowedOrigins {
		if strings.EqualFold(allowed, origin) {
			return true
		}
	}
	h.hub.logger.Warn("upgrade refused: origin not allowed", log.F("origin", origin))
	return false
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := h.hub.logger
	token := extractToken(r)
	if token == "" {
		logger.Debug("join refused: no token", log.F("path", r.URL.Path))
		unauthorized(w, "missing token")
		return
	}
	join, err := h.verifier.Verify(r.Context(), token)
	if err != nil {
		logger.Warn("join refused: token not verified", log.F("error", err.Error()))
		unauthorized(w, "invalid token")
		return
	}
	if join.Room == "" || join.Identity.UserID == "" {
		logger.Warn("join refused: verifier returned no room or user")
		unauthorized(w, "invalid token")
		return
	}
	// A token for one room must not open another.
	if room := h.cfg.roomFunc(r); room != join.Room {
		logger.Warn("join refused: token is for another room",
			log.F("room", room), log.F("token_room", join.Room), log.F("user_id", join.Identity.UserID))
		unauthorized(w, "token/room mismatch")
		return
	}

	rm, err := h.hub.join(r.Context(), join.Room, join.RoomAttrs)
	if err != nil {
		status := http.StatusServiceUnavailable
		if !errors.Is(err, ErrClosed) {
			logger.Error(err, "join failed", log.F("room", join.Room))
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// The upgrader has already written the error response.
		rm.joining.Add(-1)
		h.hub.removeRoom(rm)
		logger.Debug("upgrade failed", log.F("room", join.Room), log.F("error", err.Error()))
		return
	}
	newClient(rm, conn, join.Identity)
	logger.Debug("client connected", log.F("room", join.Room), log.F("user_id", join.Identity.UserID))
}

func extractToken(r *http.Request) string {
	if t := r.URL.Query().Get("token"); t != "" {
		return t
	}
	if after, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		return after
	}
	return ""
}

func unauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", bearerChallenge)
	http.Error(w, msg, http.StatusUnauthorized)
}
