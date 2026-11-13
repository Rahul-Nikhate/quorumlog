package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/Rahul-Nikhate/quorumlog/internal/cluster"
	"github.com/Rahul-Nikhate/quorumlog/internal/partition"
)

// Error codes of the API contract (docs/SCOPE.md).
const (
	CodeNotLeader           = "NOT_LEADER"
	CodeNotFound            = "NOT_FOUND"
	CodeBadRequest          = "BAD_REQUEST"
	CodeTimeout             = "TIMEOUT"
	CodeUnavailable         = "UNAVAILABLE"
	CodeOutOfOrderSequence  = "OUT_OF_ORDER_SEQUENCE"
	HeaderLeader            = "X-QL-Leader"
	HeaderForwarded         = "X-QL-Forwarded"
	maxMessagesPerRequest   = 1000
	maxFetchMessages        = 1000
	defaultFetchMessages    = 100
	maxWaitMillis           = 30_000
	defaultMaxBodyBytes     = 1 << 20
	defaultProduceTimeoutMs = 10_000
)

// ErrorBody is the JSON body of every error response.
type ErrorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// badRequest is a client error with a message.
type badRequest struct{ msg string }

func (e *badRequest) Error() string { return e.msg }

func errBad(msg string) error { return &badRequest{msg} }

// classify maps an error to an HTTP status and API code. Order matters:
// ErrOutcomeUnknown wraps its cause (a leader change or a deadline).
func classify(err error) (int, string) {
	var br *badRequest
	var mbe *http.MaxBytesError
	switch {
	case errors.As(err, &br), errors.Is(err, cluster.ErrBadName), errors.Is(err, partition.ErrInvalidBatch),
		errors.Is(err, partition.ErrOffsetOutOfRange):
		return http.StatusBadRequest, CodeBadRequest
	case errors.As(err, &mbe):
		return http.StatusRequestEntityTooLarge, CodeBadRequest
	case errors.Is(err, partition.ErrOutOfOrderSequence):
		return http.StatusConflict, CodeOutOfOrderSequence
	case errors.Is(err, cluster.ErrNotFound):
		return http.StatusNotFound, CodeNotFound
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, partition.ErrTimeout):
		return http.StatusGatewayTimeout, CodeTimeout
	case errors.Is(err, partition.ErrNotLeader):
		return http.StatusServiceUnavailable, CodeNotLeader
	default: // closed, storage failure, proposal dropped, outcome unknown, client gone
		return http.StatusServiceUnavailable, CodeUnavailable
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	status, code := classify(err)
	var nl *partition.NotLeaderError
	if errors.As(err, &nl) && nl.Leader != 0 {
		w.Header().Set(HeaderLeader, strconv.FormatUint(uint64(nl.Leader), 10))
	}
	writeJSON(w, status, ErrorBody{Error: err.Error(), Code: code})
}

func writeCode(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, ErrorBody{Error: msg, Code: code})
}
