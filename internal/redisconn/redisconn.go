package redisconn

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// New creates a Redis client, pings the server to verify connectivity, and returns it.
func New(ctx context.Context, addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return client, nil
}
