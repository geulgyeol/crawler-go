package users

import (
	"context"
	"fmt"
	"os"
	"time"

	"crawler/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Open connects to PostgreSQL using DATABASE_URL. No credentials are stored in config.yaml.
func Open(ctx context.Context) (*pgxpool.Pool, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, fmt.Errorf("DATABASE_URL is required for user registration")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return pool, nil
}

// Register atomically claims a user. Only the worker that inserted the row
// should enqueue it; a conflicting insert returns false, never a stale check.
func Register(ctx context.Context, queries *db.Queries, link string) (bool, error) {
	if len(link) < 2 {
		return false, fmt.Errorf("invalid user ID %q", link)
	}
	var platform string
	switch link[0] {
	case 'N':
		platform = "naverblog"
	case 'T':
		platform = "tistory"
	default:
		return false, fmt.Errorf("invalid user ID %q", link)
	}

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := queries.RegisterUser(queryCtx, db.RegisterUserParams{
		BlogPlatform: platform,
		UserID:       link[1:],
	})
	if err != nil {
		return false, fmt.Errorf("register user %q: %w", link, err)
	}
	return rows == 1, nil
}
