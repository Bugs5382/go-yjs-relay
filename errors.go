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
	"fmt"

	"github.com/Bugs5382/go-apperr"
)

var (
	// ErrRejected matches a checkpoint the sink refused.
	ErrRejected = errors.New("yjsrelay: snapshot rejected")
	// ErrUnavailable matches a checkpoint the sink could not take right now.
	// A sink wraps it to mark a failure as transient.
	ErrUnavailable = errors.New("yjsrelay: snapshot sink unavailable")
	// ErrClosed is returned once the hub is closed.
	ErrClosed = errors.New("yjsrelay: hub closed")
)

// Reasons the relay puts on a rejected checkpoint when the sink's error is
// not a *RejectError.
const (
	// ReasonUnavailable marks a transient failure; the checkpoint is kept.
	ReasonUnavailable = "unavailable"
	// ReasonSinkError marks any other sink error. Its text stays server-side.
	ReasonSinkError = "sink_error"
)

// RejectError is how a sink refuses a checkpoint with a reason the editors
// may see. Reason is a stable token and Detail a human-readable cause; both
// travel to every editor in the room in a snapshot.rejected control frame.
type RejectError struct {
	Reason string
	Detail string
}

func (e *RejectError) Error() string {
	if e.Detail == "" {
		return "yjsrelay: snapshot rejected: " + e.Reason
	}
	return "yjsrelay: snapshot rejected: " + e.Reason + ": " + e.Detail
}

// Is makes a *RejectError match ErrRejected.
func (e *RejectError) Is(target error) bool { return target == ErrRejected }

// FlushError reports that the sink does not hold the room's newest
// checkpoint. It matches ErrUnavailable when the failure was transient and
// ErrRejected otherwise, and it unwraps to the sink's own error.
type FlushError struct {
	Room   string
	Reason string
	// Detail is the RejectError's detail, empty for any other sink error.
	Detail string
	// Err is the sink's error.
	Err error
}

func (e *FlushError) Error() string {
	return fmt.Sprintf("yjsrelay: room %q checkpoint not stored (%s): %v", e.Room, e.Reason, e.Err)
}

// Unwrap returns the matching sentinel and the sink's error.
func (e *FlushError) Unwrap() []error {
	if e.Transient() {
		return []error{ErrUnavailable, e.Err}
	}
	return []error{ErrRejected, e.Err}
}

// Transient reports whether resending the same checkpoint may succeed.
func (e *FlushError) Transient() bool { return isTransient(e.Err) }

func isTransient(err error) bool {
	return errors.Is(err, ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func newFlushError(room string, err error) *FlushError {
	fe := &FlushError{Room: room, Err: err}
	var re *RejectError
	switch {
	case errors.As(err, &re):
		fe.Reason, fe.Detail = re.Reason, re.Detail
	case isTransient(err):
		fe.Reason = ReasonUnavailable
	default:
		fe.Reason = ReasonSinkError
	}
	return fe
}

// Codes are the caller's go-apperr codes for the errors Flush and Close
// return. A zero field leaves that error uncoded; the library never picks a
// code of its own.
type Codes struct {
	Rejected    int
	Unavailable int
}

func (c Codes) wrap(err error) error {
	if err == nil {
		return nil
	}
	code := c.Rejected
	if errors.Is(err, ErrUnavailable) {
		code = c.Unavailable
	}
	if code == 0 {
		return err
	}
	return apperr.Coded(code, err)
}
