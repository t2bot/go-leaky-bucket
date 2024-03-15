package leaky

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
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

func DecodeBucket(r io.Reader) (*Bucket, error) {
	bucket := &Bucket{}

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	// Check format version
	format := int32(0)
	if err := binary.Read(r, binary.BigEndian, &format); err != nil {
		return nil, err
	}
	if format != 1 {
		return nil, fmt.Errorf("leaky: unsupported format version %d", format)
	}

	// Read fields in write order
	if err := binary.Read(r, binary.BigEndian, &bucket.DrainBy); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &bucket.DrainInterval); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &bucket.Capacity); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &bucket.value); err != nil {
		return nil, err
	}
	lastDrainMs := int64(0)
	if err := binary.Read(r, binary.BigEndian, &lastDrainMs); err != nil {
		return nil, err
	}
	bucket.lastDrain = time.UnixMilli(lastDrainMs)

	return bucket, nil
}

func (b *Bucket) Encode(w io.Writer) error {
	buf := &bytes.Buffer{}

	b.lock.Lock()
	defer b.lock.Unlock()

	// Format version
	if err := binary.Write(buf, binary.BigEndian, int32(1)); err != nil {
		return errors.Join(errors.New("leaky: unable to write format version"), err)
	}

	// Fields, ordered
	if err := binary.Write(buf, binary.BigEndian, b.DrainBy); err != nil {
		return errors.Join(errors.New("leaky: unable to write `DrainBy`"), err)
	}
	if err := binary.Write(buf, binary.BigEndian, b.DrainInterval); err != nil {
		return errors.Join(errors.New("leaky: unable to write `DrainInterval`"), err)
	}
	if err := binary.Write(buf, binary.BigEndian, b.Capacity); err != nil {
		return errors.Join(errors.New("leaky: unable to write `Capacity`"), err)
	}
	if err := binary.Write(buf, binary.BigEndian, b.value); err != nil {
		return errors.Join(errors.New("leaky: unable to write `value`"), err)
	}
	if err := binary.Write(buf, binary.BigEndian, b.lastDrain.UnixMilli()); err != nil {
		return errors.Join(errors.New("leaky: unable to write `lastDrain`"), err)
	}

	// Write and return
	_, err := w.Write(buf.Bytes())
	return err
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
