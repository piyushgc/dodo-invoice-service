// Package httpx contains the HTTP plumbing shared by every handler: the error envelope,
// JSON helpers, pagination parsing and middleware.
package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Error is the single error shape returned by the API:
//
//	{"error": {"code": "invalid_state_transition", "message": "...", "details": {...}, "request_id": "..."}}
type Error struct {
	Status  int            `json:"-"`
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// NewError builds an API error.
func NewError(status int, code, message string) *Error {
	return &Error{Status: status, Code: code, Message: message}
}

// WithDetails attaches structured details to the error.
func (e *Error) WithDetails(d map[string]any) *Error {
	e.Details = d
	return e
}

// Common errors.
var (
	ErrNotFound = func(what string) *Error {
		return NewError(http.StatusNotFound, "resource_not_found", what+" not found")
	}
	ErrInternal = NewError(http.StatusInternalServerError, "internal_error", "an internal error occurred")
)

// ErrorBody renders err as the API error envelope. Used both for live responses and for
// responses stored against an idempotency key.
func ErrorBody(ctx context.Context, e *Error) []byte {
	type envelope struct {
		Error struct {
			*Error
			RequestID string `json:"request_id,omitempty"`
		} `json:"error"`
	}
	var env envelope
	env.Error.Error = e
	env.Error.RequestID = RequestID(ctx)
	b, _ := json.Marshal(env)
	return b
}

// WriteJSON writes v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		slog.Error("marshal response", "err", err)
		status, b = http.StatusInternalServerError, []byte(`{"error":{"code":"internal_error","message":"an internal error occurred"}}`)
	}
	WriteRaw(w, status, b)
}

// WriteRaw writes pre-rendered JSON bytes.
func WriteRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)         //nolint:errcheck
	w.Write([]byte("\n")) //nolint:errcheck
}

// WriteError writes err using the error envelope. Anything that is not an *Error is logged
// and reported as a generic 500 so internals never leak to clients.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		slog.ErrorContext(r.Context(), "unhandled error", "err", err, "request_id", RequestID(r.Context()),
			"method", r.Method, "path", r.URL.Path)
		apiErr = ErrInternal
	}
	WriteRaw(w, apiErr.Status, ErrorBody(r.Context(), apiErr))
}

// Handle adapts a handler that returns an error into an http.HandlerFunc.
func Handle(h func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			WriteError(w, r, err)
		}
	}
}

const maxBodyBytes = 1 << 20

// DecodeJSON strictly decodes a JSON request body into dst. Unknown fields are rejected:
// that is how a client-supplied "total_cents" on an invoice gets a 400 instead of being
// silently ignored, and how a float like 1.5 for a cents field is refused.
func DecodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return NewError(http.StatusBadRequest, "invalid_json", "request body is not valid: "+cleanJSONError(err))
	}
	if dec.More() {
		return NewError(http.StatusBadRequest, "invalid_json", "request body must contain a single JSON object")
	}
	return nil
}

func cleanJSONError(err error) string {
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return fmt.Sprintf("field %q must be of type %s", typeErr.Field, typeErr.Type.String())
	}
	if errors.Is(err, io.EOF) {
		return "body is empty"
	}
	return strings.TrimPrefix(err.Error(), "json: ")
}

// PathUUID parses a UUID path parameter, returning a 404 for malformed IDs (a malformed
// ID can never match a resource).
func PathUUID(r *http.Request, name, what string) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		return uuid.Nil, ErrNotFound(what)
	}
	return id, nil
}

// Page holds keyset pagination parameters: ?limit=20&starting_after=<id>.
type Page struct {
	Limit         int
	StartingAfter *uuid.UUID
}

// ParsePage reads pagination query parameters.
func ParsePage(r *http.Request) (Page, error) {
	p := Page{Limit: 20}
	q := r.URL.Query()
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			return p, NewError(http.StatusBadRequest, "invalid_parameter", "limit must be an integer between 1 and 100")
		}
		p.Limit = n
	}
	if v := q.Get("starting_after"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			return p, NewError(http.StatusBadRequest, "invalid_parameter", "starting_after must be an ID")
		}
		p.StartingAfter = &id
	}
	return p, nil
}

// List is the envelope for list endpoints.
type List[T any] struct {
	Data    []T  `json:"data"`
	HasMore bool `json:"has_more"`
}

// NewList trims a result fetched with limit+1 rows into a page.
func NewList[T any](rows []T, limit int) List[T] {
	if rows == nil {
		rows = []T{}
	}
	if len(rows) > limit {
		return List[T]{Data: rows[:limit], HasMore: true}
	}
	return List[T]{Data: rows}
}

// ---- middleware ----

type ctxKey int

const requestIDKey ctxKey = iota

// RequestID returns the request ID stored in ctx, if any.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// Middleware wraps h with request IDs, panic recovery and access logging.
func Middleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Request-Id")
		if id == "" || len(id) > 64 {
			b := make([]byte, 8)
			rand.Read(b) //nolint:errcheck
			id = "req_" + hex.EncodeToString(b)
		}
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		r = r.WithContext(ctx)
		w.Header().Set("X-Request-Id", id)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		defer func() {
			if p := recover(); p != nil {
				slog.ErrorContext(ctx, "panic", "panic", p, "stack", string(debug.Stack()), "request_id", id)
				WriteRaw(rec, http.StatusInternalServerError, ErrorBody(ctx, ErrInternal))
			}
			slog.InfoContext(ctx, "request",
				"method", r.Method, "path", r.URL.Path, "status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(), "request_id", id)
		}()
		h.ServeHTTP(rec, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}
