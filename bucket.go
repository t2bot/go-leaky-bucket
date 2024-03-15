package leaky

import (
	"encoding/gob"
	"errors"
	"sync"
	"time"
)

var ErrBucketFull = errors.New("leaky: bucket full or would overflow")

func init() {
	gob.Register(&Bucket{})
}

type Bucket struct {
	DrainBy       int64
	DrainInterval time.Duration
	Capacity      int64

	value     int64
	lastDrain time.Time
	lock      sync.Mutex
}

func NewBucket(drainBy int64, drainEvery time.Duration, capacity int64) (*Bucket, error) {
	if drainBy <= 0 || drainEvery <= 0 {
		return nil, errors.New("leaky: bucket never drains")
	}
	if capacity <= 0 {
		return nil, errors.New("leaky: bucket can never fill")
	}
	return &Bucket{
		DrainBy:       drainBy,
		DrainInterval: drainEvery,
		Capacity:      capacity,
		value:         0,
		lastDrain:     time.Now(),
		lock:          sync.Mutex{},
	}, nil
}

func (b *Bucket) drain() {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.lastDrain.IsZero() {
		b.lastDrain = time.Now() // assume we've never drained
	}

	if b.value <= 0 {
		b.value = 0
		b.lastDrain = time.Now()
		return // nothing to drain, so don't bother
	}

	since := time.Since(b.lastDrain)
	drainTime := since.Truncate(b.DrainInterval)
	leaks := int64(drainTime.Abs() / b.DrainInterval.Abs())
	b.value -= b.DrainBy * leaks
	if b.value < 0 {
		b.value = 0
	}
	b.lastDrain = time.Now().Add((since - drainTime) * -1)
}

func (b *Bucket) Peek() int64 {
	return b.value
}

func (b *Bucket) Value() int64 {
	b.drain()
	return b.value
}

func (b *Bucket) Remaining() int64 {
	b.drain()
	return b.Capacity - b.value
}

func (b *Bucket) Add(amount int64) error {
	b.drain()

	b.lock.Lock()
	defer b.lock.Unlock()

	newValue := b.value + amount
	if newValue > b.Capacity {
		return ErrBucketFull
	}
	b.value = newValue
	return nil
}

func (b *Bucket) Set(value int64, resetDrain bool) error {
	if value < 0 {
		return errors.New("leaky: bucket value cannot be negative")
	}
	if value > b.Capacity {
		return errors.New("leaky: bucket value cannot exceed capacity")
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	b.value = value
	if resetDrain {
		b.lastDrain = time.Now()
	}
	return nil
}
