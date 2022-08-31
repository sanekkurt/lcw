package lcw

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisValueSizeLimit is maximum allowed value size in Redis
const RedisValueSizeLimit = 512 * 1024 * 1024

var (
	ErrNotAllowed = errors.New("set data is not allowed")
)

type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	DBSize(ctx context.Context) *redis.IntCmd
	FlushDB(ctx context.Context) *redis.StatusCmd
	Close() error
}

// RedisCache implements LoadingCache for Redis.
type RedisCache struct {
	options
	CacheStat
	backend   RedisClient
	keyPrefix string
}

// NewRedisCache makes Redis LoadingCache implementation.
func NewRedisCache(serviceName string, backend RedisClient, opts ...Option) (*RedisCache, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("the service name should not be empty")
	}

	res := RedisCache{
		keyPrefix: serviceName + "-",
		options: options{
			ttl: 5 * time.Minute,
		},
	}

	for _, opt := range opts {
		if err := opt(&res.options); err != nil {
			return nil, fmt.Errorf("failed to set cache option: %w", err)
		}
	}

	if res.maxValueSize <= 0 || res.maxValueSize > RedisValueSizeLimit {
		res.maxValueSize = RedisValueSizeLimit
	}

	res.backend = backend

	return &res, nil
}

// Set sets value by key to cache
func (c *RedisCache) Set(ctx context.Context, key string, data interface{}) error {
	if !c.allowed(key, data) {
		return ErrNotAllowed
	}

	_, setErr := c.backend.Set(ctx, c.keyPrefix+key, data, c.ttl).Result()
	if setErr != nil {
		atomic.AddInt64(&c.Errors, 1)
		return setErr
	}

	return nil
}

// Get gets value by key or load with fn if not found in cache
func (c *RedisCache) Get(ctx context.Context, key string, fn func() (interface{}, error)) (data interface{}, err error) {
	v, getErr := c.backend.Get(ctx, c.keyPrefix+key).Result()
	switch getErr {
	// RedisClient returns nil when find a key in DB
	case nil:
		atomic.AddInt64(&c.Hits, 1)
		return v, nil
	// RedisClient returns redis.Nil when doesn't find a key in DB
	case redis.Nil:
		if data, err = fn(); err != nil {
			atomic.AddInt64(&c.Errors, 1)
			return data, err
		}
	// RedisClient returns !nil when something goes wrong while get data
	default:
		atomic.AddInt64(&c.Errors, 1)
		return v, getErr
	}
	atomic.AddInt64(&c.Misses, 1)

	if setErr := c.Set(ctx, key, data); setErr != nil {
		if errors.Is(setErr, ErrNotAllowed) {
			return data, nil
		}

		return data, setErr
	}

	return data, nil
}

// Invalidate removes keys with passed predicate fn, i.e. fn(key) should be true to get evicted
func (c *RedisCache) Invalidate(ctx context.Context, fn func(key string) bool) {
	for _, key := range c.backend.Keys(ctx, c.keyPrefix+"*").Val() { // Keys() returns copy of cache's key, safe to remove directly
		if fn(strings.TrimPrefix(key, c.keyPrefix)) {
			c.backend.Del(context.Background(), key)
		}
	}
}

// Peek returns the key value (or undefined if not found) without updating the "recently used"-ness of the key.
func (c *RedisCache) Peek(ctx context.Context, key string) (interface{}, bool) {
	ret, err := c.backend.Get(ctx, c.keyPrefix+key).Result()
	if err != nil {
		return nil, false
	}
	return ret, true
}

// Purge clears the cache completely.
func (c *RedisCache) Purge(ctx context.Context) {
	c.backend.Del(ctx, c.backend.Keys(context.Background(), c.keyPrefix+"*").Val()...)
}

// Delete cache item by key
func (c *RedisCache) Delete(ctx context.Context, key string) {
	c.backend.Del(ctx, c.keyPrefix+key)
}

// Keys gets all keys for the cache
func (c *RedisCache) Keys(ctx context.Context) (res []string) {
	for _, key := range c.backend.Keys(ctx, c.keyPrefix+"*").Val() {
		res = append(res, strings.TrimPrefix(key, c.keyPrefix))
	}

	return res
}

// Stat returns cache statistics
func (c *RedisCache) Stat() CacheStat {
	return CacheStat{
		Hits:   c.Hits,
		Misses: c.Misses,
		Size:   c.size(),
		Keys:   c.keys(),
		Errors: c.Errors,
	}
}

// Close closes underlying connections
func (c *RedisCache) Close() error {
	return c.backend.Close()
}

func (c *RedisCache) size() int64 {
	return 0
}

func (c *RedisCache) keys() int {
	return len(c.backend.Keys(context.Background(), c.keyPrefix+"*").Val())
}

func (c *RedisCache) allowed(key string, data interface{}) bool {
	if c.maxKeys > 0 && int64(len(c.backend.Keys(context.Background(), c.keyPrefix+"*").Val())) >= int64(c.maxKeys) {
		return false
	}
	if c.maxKeySize > 0 && len(key) > c.maxKeySize {
		return false
	}
	if s, ok := data.(Sizer); ok {
		if c.maxValueSize > 0 && (s.Size() >= c.maxValueSize) {
			return false
		}
	}
	return true
}
