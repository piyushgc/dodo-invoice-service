package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Headers sent with every delivery.
const (
	HeaderSignature = "Webhook-Signature" // t=<unix seconds>,v1=<hex HMAC-SHA256>
	HeaderEventID   = "Webhook-Id"        // event ID: receivers dedupe on this
	HeaderEventType = "Webhook-Event"
)

// DefaultTolerance is how old a signature timestamp may be before a receiver should
// reject it as a replay.
const DefaultTolerance = 5 * time.Minute

// Sign computes the signature header value. The signed message is "<timestamp>.<raw body>",
// so the timestamp cannot be changed without invalidating the signature.
func Sign(secret string, timestamp int64, body []byte) string {
	return fmt.Sprintf("t=%d,v1=%s", timestamp, hex.EncodeToString(mac(secret, timestamp, body)))
}

func mac(secret string, timestamp int64, body []byte) []byte {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(strconv.FormatInt(timestamp, 10)))
	m.Write([]byte("."))
	m.Write(body)
	return m.Sum(nil)
}

// Verify is the receiver-side check (used by the demo webhook sink and tests). It rejects
// bad signatures and timestamps outside the tolerance window, using a constant-time compare.
func Verify(secret, header string, body []byte, tolerance time.Duration, now time.Time) error {
	var ts int64
	var sig []byte
	for _, part := range strings.Split(header, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		switch k {
		case "t":
			ts, _ = strconv.ParseInt(v, 10, 64)
		case "v1":
			sig, _ = hex.DecodeString(v)
		}
	}
	if ts == 0 || sig == nil {
		return errors.New("malformed signature header")
	}
	if age := now.Sub(time.Unix(ts, 0)); age > tolerance || age < -tolerance {
		return errors.New("timestamp outside tolerance (possible replay)")
	}
	if !hmac.Equal(sig, mac(secret, ts, body)) {
		return errors.New("signature mismatch")
	}
	return nil
}
