package redisconn

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// New creates a Redis client, pings the server to verify connectivity, and returns it.
func New(ctx context.Context, addr string) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return rdb, nil
}
