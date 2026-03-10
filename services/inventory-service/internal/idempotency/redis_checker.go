package idempotency

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisChecker struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisChecker(addr string) *RedisChecker {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	return &RedisChecker{
		client: client,
		ttl:    24 * time.Hour, // Keep processed events for 24 hours
	}
}

func (r *RedisChecker) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	key := fmt.Sprintf("processed:%s", eventID)
	exists, err := r.client.Exists(ctx, key).Result()
	return exists > 0, err
}

func (r *RedisChecker) MarkProcessed(ctx context.Context, eventID string) error {
	key := fmt.Sprintf("processed:%s", eventID)
	return r.client.Set(ctx, key, "1", r.ttl).Err()
}
