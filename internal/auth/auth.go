// Package auth implements business API keys: generation, hashed storage, the request
// middleware, and the create/list/revoke endpoints used for rotation.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"invoicesvc/internal/db"
	"invoicesvc/internal/httpx"
)

const (
	keyPrefix    = "sk_"
	displayChars = 11 // "sk_" + 8 characters, enough to tell keys apart in a list
)

// GenerateKey returns a new plaintext key (shown to the caller exactly once), its display
// prefix and the SHA-256 hash that is the only thing persisted.
//
// 32 bytes from crypto/rand = 256 bits of entropy, so a fast hash is sufficient: there is
// nothing to brute-force, unlike a human password, and lookups stay a single index probe.
func GenerateKey() (plaintext, prefix string, hash []byte, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", nil, err
	}
	plaintext = keyPrefix + base64.RawURLEncoding.EncodeToString(b)
	return plaintext, plaintext[:displayChars], HashKey(plaintext), nil
}

// HashKey hashes a plaintext key for storage and lookup.
func HashKey(key string) []byte {
	sum := sha256.Sum256([]byte(key))
	return sum[:]
}

type ctxKey int

const businessKey ctxKey = iota

// BusinessID returns the authenticated business for this request. Every query in the
// service is scoped by this value.
func BusinessID(ctx context.Context) uuid.UUID {
	id, _ := ctx.Value(businessKey).(uuid.UUID)
	return id
}

// WithBusinessID is exported for tests and internal callers.
func WithBusinessID(ctx context.Context, id uuid.UUID) context.Context {
	return context.WithValue(ctx, businessKey, id)
}

var errUnauthorized = httpx.NewError(http.StatusUnauthorized, "unauthorized",
	"missing or invalid API key; send 'Authorization: Bearer sk_...'")

// Middleware authenticates `Authorization: Bearer <key>`. Revocation is effective on the
// very next request because nothing is cached.
func Middleware(pool *pgxpool.Pool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || !strings.HasPrefix(key, keyPrefix) {
				httpx.WriteError(w, r, errUnauthorized)
				return
			}
			var businessID uuid.UUID
			err := pool.QueryRow(r.Context(),
				`SELECT business_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`,
				HashKey(key)).Scan(&businessID)
			if errors.Is(err, pgx.ErrNoRows) {
				httpx.WriteError(w, r, errUnauthorized)
				return
			}
			if err != nil {
				httpx.WriteError(w, r, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(WithBusinessID(r.Context(), businessID)))
		})
	}
}

// APIKey is the public view of a key. The plaintext is only ever present on creation.
type APIKey struct {
	ID        uuid.UUID  `json:"id"`
	Prefix    string     `json:"prefix"`
	Key       string     `json:"key,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at"`
}

// CreateBusinessWithKey creates a business and its first key (used by seeding and tests).
func CreateBusinessWithKey(ctx context.Context, pool *pgxpool.Pool, name, plaintext string) (uuid.UUID, error) {
	businessID, _ := uuid.NewV7()
	err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO businesses (id, name) VALUES ($1, $2)`, businessID, name); err != nil {
			return err
		}
		keyID, _ := uuid.NewV7()
		_, err := tx.Exec(ctx, `INSERT INTO api_keys (id, business_id, prefix, key_hash) VALUES ($1, $2, $3, $4)`,
			keyID, businessID, plaintext[:displayChars], HashKey(plaintext))
		return err
	})
	return businessID, err
}

// SeedDemoBusiness makes sure a business exists for the given plaintext key, so that
// `docker compose up` yields a usable API key with no manual steps. Idempotent.
func SeedDemoBusiness(ctx context.Context, pool *pgxpool.Pool, plaintext string) (uuid.UUID, error) {
	if !strings.HasPrefix(plaintext, keyPrefix) || len(plaintext) < displayChars {
		return uuid.Nil, errors.New("demo API key must start with sk_ and be at least 11 characters")
	}
	var businessID uuid.UUID
	err := pool.QueryRow(ctx, `SELECT business_id FROM api_keys WHERE key_hash = $1`, HashKey(plaintext)).Scan(&businessID)
	if err == nil {
		return businessID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, err
	}
	return CreateBusinessWithKey(ctx, pool, "Demo Business", plaintext)
}

// Handlers serves /v1/api-keys.
type Handlers struct {
	DB *pgxpool.Pool
}

// Create issues an additional key for the authenticated business (rotation step 1).
func (h *Handlers) Create(w http.ResponseWriter, r *http.Request) error {
	businessID := BusinessID(r.Context())
	plaintext, prefix, hash, err := GenerateKey()
	if err != nil {
		return err
	}
	k := APIKey{Prefix: prefix, Key: plaintext}
	k.ID, _ = uuid.NewV7()
	err = h.DB.QueryRow(r.Context(),
		`INSERT INTO api_keys (id, business_id, prefix, key_hash) VALUES ($1, $2, $3, $4) RETURNING created_at`,
		k.ID, businessID, prefix, hash).Scan(&k.CreatedAt)
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusCreated, k)
	return nil
}

// List returns the business's keys (prefix only, never the secret).
func (h *Handlers) List(w http.ResponseWriter, r *http.Request) error {
	rows, err := h.DB.Query(r.Context(),
		`SELECT id, prefix, created_at, revoked_at FROM api_keys WHERE business_id = $1 ORDER BY created_at DESC`,
		BusinessID(r.Context()))
	if err != nil {
		return err
	}
	keys, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (APIKey, error) {
		var k APIKey
		return k, row.Scan(&k.ID, &k.Prefix, &k.CreatedAt, &k.RevokedAt)
	})
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.NewList(keys, len(keys)))
	return nil
}

// Revoke disables a key immediately (rotation step 2, or incident response).
func (h *Handlers) Revoke(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.PathUUID(r, "id", "api key")
	if err != nil {
		return err
	}
	var k APIKey
	err = h.DB.QueryRow(r.Context(), `
		UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now())
		WHERE id = $1 AND business_id = $2
		RETURNING id, prefix, created_at, revoked_at`,
		id, BusinessID(r.Context())).Scan(&k.ID, &k.Prefix, &k.CreatedAt, &k.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.ErrNotFound("api key")
	}
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, k)
	return nil
}
