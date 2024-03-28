package leaky

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// ErrBucketFull represents an error indicating that a bucket is full or would overflow.
var ErrBucketFull = errors.New("leaky: bucket full or would overflow")

// Bucket represents a leaky bucket implementation for rate limiting or throttling.
type Bucket struct {
	DrainBy       int64
	DrainInterval time.Duration
	Capacity      int64

	value     int64
	lastDrain time.Time
	lock      sync.Mutex
}

// NewBucket creates a new Bucket with the given drainBy, drainEvery, and capacity parameters.
// It returns an error if any of the parameters are invalid.
//
// Example usage:
//
//	bucket, err := NewBucket(5, 1 * time.Minute, 300)
//
// Parameters:
//
//	drainBy     - the amount to drain the bucket by each drain interval
//	drainEvery  - the duration between each drain interval
//	capacity    - the maximum capacity the bucket can hold
//
// Return values:
//
//	*Bucket     - the created Bucket instance
//	error       - error message if any of the parameters are invalid
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

// DecodeBucket produces a Bucket from a previous Encode operation.
// It returns an error if any read operation fails. Read operations are performed sequentially rather
// than atomically. If an error occurs, partial data may remain on the reader.
//
// Example usage:
//
//	buf := bytes.NewBuffer(myEncodedData)
//	if bucket, err := leaky.DecodeBucket(buf); err != nil {
//		log.Fatal(err)
//	} else {
//		// Use the decoded `bucket`
//	}
//
// Parameters:
//
//	r       - an io.Reader interface from which the binary data will be read
//
// Return values:
//
//	*Bucket - the Bucket instance decoded from the binary data in r
//	error   - error message if any errors occurred during reading or decoding
func DecodeBucket(r io.Reader) (*Bucket, error) {
	bucket := &Bucket{}

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	// Check format version
	format := int32(0)
	if err := binary.Read(r, binary.BigEndian, &format); err != nil {
		return nil, errors.Join(errors.New("leaky: unable to read format version"), err)
	}
	if format != 1 {
		return nil, fmt.Errorf("leaky: unsupported format version %d", format)
	}

	// Read fields in write order
	if err := binary.Read(r, binary.BigEndian, &bucket.DrainBy); err != nil {
		return nil, errors.Join(errors.New("leaky: unable to read `DrainBy`"), err)
	}
	if err := binary.Read(r, binary.BigEndian, &bucket.DrainInterval); err != nil {
		return nil, errors.Join(errors.New("leaky: unable to read `DrainInterval`"), err)
	}
	if err := binary.Read(r, binary.BigEndian, &bucket.Capacity); err != nil {
		return nil, errors.Join(errors.New("leaky: unable to read `Capacity`"), err)
	}
	if err := binary.Read(r, binary.BigEndian, &bucket.value); err != nil {
		return nil, errors.Join(errors.New("leaky: unable to read `value`"), err)
	}
	timestampSize := int32(0)
	if err := binary.Read(r, binary.BigEndian, &timestampSize); err != nil {
		return nil, errors.Join(errors.New("leaky: unable to read size of `lastDrain`"), err)
	}
	timestampBytes := make([]byte, timestampSize)
	if c, err := r.Read(timestampBytes); err != nil {
		return nil, errors.Join(errors.New("leaky: unable to read `lastDrain`"), err)
	} else if int32(c) != timestampSize {
		return nil, errors.New("leaky: did not read entire timestamp")
	}
	if err := bucket.lastDrain.UnmarshalBinary(timestampBytes); err != nil {
		return nil, errors.Join(errors.New("leaky: unable to unmarshal `lastDrain`"), err)
	}

	return bucket, nil
}

// Encode writes the bucket's state to the provided io.Writer.
// It returns an error if any writing operation fails. Write operations are performed sequentially rather
// than atomically. If an error occurs, partial data may be written to the writer.
//
// Example usage:
//
//	buf := &bytes.Buffer{}
//	if err := bucket.Encode(buf); err != nil {
//		log.Fatal(err)
//	}
//	// Use the encoded data stored in `buf` as needed.
//
// Parameters:
//
//	w   - an io.Writer interface to which the binary data will be written
//
// Return values:
//
//	error   - error message if any errors occurred during writing or encoding
func (b *Bucket) Encode(w io.Writer) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	// Format version
	if err := binary.Write(w, binary.BigEndian, int32(1)); err != nil {
		return errors.Join(errors.New("leaky: unable to write format version"), err)
	}

	// Fields, ordered
	if err := binary.Write(w, binary.BigEndian, b.DrainBy); err != nil {
		return errors.Join(errors.New("leaky: unable to write `DrainBy`"), err)
	}
	if err := binary.Write(w, binary.BigEndian, b.DrainInterval); err != nil {
		return errors.Join(errors.New("leaky: unable to write `DrainInterval`"), err)
	}
	if err := binary.Write(w, binary.BigEndian, b.Capacity); err != nil {
		return errors.Join(errors.New("leaky: unable to write `Capacity`"), err)
	}
	if err := binary.Write(w, binary.BigEndian, b.value); err != nil {
		return errors.Join(errors.New("leaky: unable to write `value`"), err)
	}
	if timestampBytes, err := b.lastDrain.MarshalBinary(); err != nil {
		return errors.Join(errors.New("leaky: unable to marshal `lastDrain`"), err)
	} else {
		if err := binary.Write(w, binary.BigEndian, int32(len(timestampBytes))); err != nil {
			return errors.Join(errors.New("leaky: unable to write length of `lastDrain`"), err)
		}
		if _, err := w.Write(timestampBytes); err != nil {
			return errors.Join(errors.New("leaky: unable to write `lastDrain`"), err)
		}
	}

	return nil
}

// drain updates the value of the bucket by subtracting the drained amount based on the elapsed time since the last drain.
// If the bucket is already empty, it does nothing.
//
// If the bucket has never been drained before, it sets the last drain time as the current time.
// If the bucket value is zero or negative after the drain, it sets the value to zero and updates the last drain time.
//
// The elapsed time since the last drain is calculated by subtracting the last drain time from the current time.
// The elapsed time is truncated to the nearest multiple of the drain interval.
// The number of leaks is then calculated by dividing the elapsed time by the drain interval.
// The drained amount is calculated by multiplying the drain by the number of leaks.
// The bucket value is updated by subtracting the drained amount.
// If the bucket value becomes negative, it is set to zero.
//
// Finally, the last drain time is updated to the current time minus the remaining elapsed time (since - drainTime).
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

// Peek returns the current value of the bucket without performing any drain.
func (b *Bucket) Peek() int64 {
	return b.value
}

// Value returns the current value of the bucket after performing a drain operation.
func (b *Bucket) Value() int64 {
	b.drain()
	return b.value
}

// Remaining returns the remaining capacity of the Bucket.
// It first applies the drain operation to update the bucket's internal value.
// The remaining capacity is calculated by subtracting the current value from the Capacity.
// This method does not modify the bucket's internal value.
//
// Returns the remaining capacity as an int64 value.
func (b *Bucket) Remaining() int64 {
	b.drain()
	return b.Capacity - b.value
}

// Add increments the value of the Bucket by the specified amount.
// If the new value would exceed Capacity, ErrBucketFull is returned without modifying the bucket's
// internal value. Otherwise, the amount is added to the bucket atomically. In either case, a drain
// operation is performed before checking the capacity.
//
// The amount may be negative to drain the bucket instead. ErrBucketFull will not be raised when
// draining. Note that when negative the bucket may additionally drain on its own. For example, if
// 1 drain operation is expected due to the timer, that will happen before the negative amount is
// applied.
//
// Returns nil if successful.
//
// Parameters:
//
//	amount  - the amount by which the bucket's value will be incremented
//
// Return values:
//
//	error   - ErrBucketFull if the new value would exceed the capacity, otherwise nil
func (b *Bucket) Add(amount int64) error {
	b.drain() // always drain first

	if amount == 0 {
		return nil // optimization
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	newValue := b.value + amount
	if amount > 0 && newValue > b.Capacity {
		// Only complain if we're not draining.
		return ErrBucketFull
	}
	if newValue < 0 {
		newValue = 0
	}
	b.value = newValue
	return nil
}

// Drain reduces the value of the bucket by the specified amount.
// It is equivalent to calling Add with a negative amount.
// If the resulting value is below 0, it is set to 0.
//
// See the Add documentation with a negative amount for more details.
//
// Parameters:
//
//	amount  - the amount to drain from the bucket
//
// Return values:
//
//	error   - an error message if the drain operation fails
func (b *Bucket) Drain(amount int64) error {
	return b.Add(-amount)
}

// Set sets the value of the Bucket.
// The value must be positive or zero, and within capacity for the bucket. An error is returned otherwise.
// This is an atomic operation, and resets the drain time.
//
// Parameters:
//
//	value  - the value to set the bucket to
//
// Return values:
//
//	error   - error message if the value is invalid
func (b *Bucket) Set(value int64) error {
	if value < 0 {
		return errors.New("leaky: bucket value cannot be negative")
	}
	if value > b.Capacity {
		return errors.New("leaky: bucket value cannot exceed capacity")
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	b.value = value
	b.lastDrain = time.Now()
	return nil
}
