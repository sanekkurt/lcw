package lcw

import (
	"log"
	"sync/atomic"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"

	"github.com/go-pkgz/lcw/eventbus"
)

// LruCache wraps lru.LruCache with loading cache Get and size limits
type LruCache struct {
	options
	CacheStat
	backend     *lru.Cache
	currentSize int64
	id          string
}

// NewLruCache makes LRU LoadingCache implementation, 1000 max keys by default
func NewLruCache(opts ...Option) (*LruCache, error) {
	res := LruCache{
		options: options{
			maxKeys:      1000,
			maxValueSize: 0,
			eventBus:     &eventbus.NopPubSub{},
		},
		id: uuid.New().String(),
	}
	for _, opt := range opts {
		if err := opt(&res.options); err != nil {
			return nil, errors.Wrap(err, "failed to set cache option")
		}
	}

	if err := res.eventBus.Subscribe(res.onBusEvent); err != nil {
		return nil, errors.Wrapf(err, "can't subscribe to event bus")
	}

	onEvicted := func(key interface{}, value interface{}) {
		if res.onEvicted != nil {
			res.onEvicted(key.(string), value)
		}
		if s, ok := value.(Sizer); ok {
			size := s.Size()
			atomic.AddInt64(&res.currentSize, -1*int64(size))
		}
		_ = res.eventBus.Publish(res.id, key.(string))
	}

	var err error
	// OnEvicted called automatically for expired and manually deleted
	if res.backend, err = lru.NewWithEvict(res.maxKeys, onEvicted); err != nil {
		return nil, errors.Wrap(err, "failed to make lru cache backend")
	}

	return &res, nil
}

// Get gets value by key or load with fn if not found in cache
func (c *LruCache) Get(key string, fn func() (Value, error)) (data Value, err error) {
	if v, ok := c.backend.Get(key); ok {
		atomic.AddInt64(&c.Hits, 1)
		return v, nil
	}

	if data, err = fn(); err != nil {
		atomic.AddInt64(&c.Errors, 1)
		return data, err
	}

	atomic.AddInt64(&c.Misses, 1)

	if !c.allowed(key, data) {
		return data, nil
	}

	c.backend.Add(key, data)

	if s, ok := data.(Sizer); ok {
		atomic.AddInt64(&c.currentSize, int64(s.Size()))
		if c.maxCacheSize > 0 && atomic.LoadInt64(&c.currentSize) > c.maxCacheSize {
			for atomic.LoadInt64(&c.currentSize) > c.maxCacheSize {
				c.backend.RemoveOldest()
			}
		}
	}

	return data, nil
}

// Peek returns the key value (or undefined if not found) without updating the "recently used"-ness of the key.
func (c *LruCache) Peek(key string) (Value, bool) {
	return c.backend.Peek(key)
}

// Purge clears the cache completely.
func (c *LruCache) Purge() {
	c.backend.Purge()
	atomic.StoreInt64(&c.currentSize, 0)
}

// Invalidate removes keys with passed predicate fn, i.e. fn(key) should be true to get evicted
func (c *LruCache) Invalidate(fn func(key string) bool) {
	for _, k := range c.backend.Keys() { // Keys() returns copy of cache's key, safe to remove directly
		if key, ok := k.(string); ok && fn(key) {
			c.backend.Remove(key)
		}
	}
}

// Delete cache item by key
func (c *LruCache) Delete(key string) {
	c.backend.Remove(key)
}

// Keys returns cache keys
func (c *LruCache) Keys() (res []string) {
	keys := c.backend.Keys()
	res = make([]string, 0, len(keys))
	for _, key := range keys {
		res = append(res, key.(string))
	}
	return res
}

// Stat returns cache statistics
func (c *LruCache) Stat() CacheStat {
	return CacheStat{
		Hits:   c.Hits,
		Misses: c.Misses,
		Size:   c.size(),
		Keys:   c.keys(),
		Errors: c.Errors,
	}
}

// Close does nothing for this type of cache
func (c *LruCache) Close() error {
	return nil
}

func (c *LruCache) onBusEvent(id, key string) {
	log.Println("!! ", id, key)
	if id != c.id { // prevent reaction on event from this cache
		c.backend.Remove(key)
	}
}

func (c *LruCache) size() int64 {
	return atomic.LoadInt64(&c.currentSize)
}

func (c *LruCache) keys() int {
	return c.backend.Len()
}

func (c *LruCache) allowed(key string, data Value) bool {
	if c.maxKeySize > 0 && len(key) > c.maxKeySize {
		return false
	}
	if s, ok := data.(Sizer); ok {
		if c.maxValueSize > 0 && s.Size() >= c.maxValueSize {
			return false
		}
	}
	return true
}
