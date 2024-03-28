package leaky

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type faultyReaderWriter struct {
	io.Reader
	io.Writer

	FailOnReadOp  int // 1 indexed
	FailOnWriteOp int // 1 indexed
	Buffer        *bytes.Buffer

	readOp  int
	writeOp int
}

func newFaultyReaderWriter(failReadOp int, failWriteOp int) *faultyReaderWriter {
	return &faultyReaderWriter{
		FailOnReadOp:  failReadOp,
		FailOnWriteOp: failWriteOp,
		Buffer:        &bytes.Buffer{},
		readOp:        0,
		writeOp:       0,
	}
}

func (rw *faultyReaderWriter) Read(b []byte) (int, error) {
	rw.readOp++
	if rw.readOp == rw.FailOnReadOp {
		return 0, errors.New("read error")
	}
	return rw.Buffer.Read(b)
}

func (rw *faultyReaderWriter) Write(b []byte) (int, error) {
	rw.writeOp++
	if rw.writeOp == rw.FailOnWriteOp {
		return 0, errors.New("write error")
	}
	return rw.Buffer.Write(b)
}

var createCaseFunctions = []func(drainBy int64, drainEvery time.Duration, capacity int64) (*Bucket, error){
	func(drainBy int64, drainEvery time.Duration, capacity int64) (*Bucket, error) {
		return &Bucket{
			DrainBy:       drainBy,
			DrainInterval: drainEvery,
			Capacity:      capacity,
		}, nil
	},
	func(drainBy int64, drainEvery time.Duration, capacity int64) (*Bucket, error) {
		return NewBucket(drainBy, drainEvery, capacity)
	},
}

func TestNewBucket(t *testing.T) {
	var err error

	// Zero drain
	_, err = NewBucket(0, time.Minute, 300)
	assert.EqualError(t, err, "leaky: bucket never drains")
	_, err = NewBucket(5, 0*time.Minute, 300)
	assert.EqualError(t, err, "leaky: bucket never drains")

	// Negative drain
	_, err = NewBucket(-10, time.Minute, 300)
	assert.EqualError(t, err, "leaky: bucket never drains")
	_, err = NewBucket(5, -10*time.Minute, 300)
	assert.EqualError(t, err, "leaky: bucket never drains")

	// No capacity
	_, err = NewBucket(5, time.Minute, 0)
	assert.EqualError(t, err, "leaky: bucket can never fill")
	_, err = NewBucket(5, time.Minute, -10)
	assert.EqualError(t, err, "leaky: bucket can never fill")

	// Happy path
	bucket, err := NewBucket(5, time.Minute, 300)
	assert.Nil(t, err)
	assert.NotNil(t, bucket)
	assert.Equal(t, int64(5), bucket.DrainBy)
	assert.Equal(t, time.Minute, bucket.DrainInterval)
	assert.Equal(t, int64(300), bucket.Capacity)
	assert.Equal(t, int64(0), bucket.value)
	assert.Equal(t, false, bucket.lastDrain.IsZero()) // ensure we set a timestamp
}

func TestBucketEncodeThenDecode(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("TestBucketEncodeThenDecode(case:%d): unexpected error %v", i, err)
			continue
		}
		bucket.value = 42                                            // force a given value
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval) // prepare for 1 drain operation

		// Encode
		buf := &bytes.Buffer{}
		if err = bucket.Encode(buf); err != nil {
			t.Errorf("TestBucketEncodeThenDecode(case:%d): unexpected encode error %v", i, err)
			continue
		}

		// Decode
		var bucket2 *Bucket
		if bucket2, err = DecodeBucket(buf); err != nil {
			t.Errorf("TestBucketEncodeThenDecode(case:%d): unexpected decode error %v", i, err)
			continue
		}
		assert.NotEqualf(t, bucket, bucket2, "TestBucketEncodeThenDecode(case:%d)", i)
		assert.Equalf(t, bucket.DrainBy, bucket2.DrainBy, "TestBucketEncodeThenDecode(case:%d)", i)
		assert.Equalf(t, bucket.DrainInterval, bucket2.DrainInterval, "TestBucketEncodeThenDecode(case:%d)", i)
		assert.Equalf(t, bucket.Capacity, bucket2.Capacity, "TestBucketEncodeThenDecode(case:%d)", i)
		assert.Equalf(t, bucket.value, bucket2.value, "TestBucketEncodeThenDecode(case:%d)", i)
		assert.Equalf(t, 0, bucket2.lastDrain.Compare(bucket.lastDrain), "TestBucketEncodeThenDecode(case:%d)", i)
		assert.Equalf(t, bucket.lastDrain.UnixNano(), bucket2.lastDrain.UnixNano(), "TestBucketEncodeThenDecode(case:%d)", i)
	}
}

func TestBucket_Encode(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("TestBucket_Encode(case:%d): unexpected error %v", i, err)
			continue
		}
		bucket.value = 42                                            // force a given value
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval) // prepare for 1 drain operation

		errorMessages := []string{
			"leaky: unable to write format version",
			"leaky: unable to write `DrainBy`",
			"leaky: unable to write `DrainInterval`",
			"leaky: unable to write `Capacity`",
			"leaky: unable to write `value`",
			//"leaky: unable to marshal `lastDrain`",
			"leaky: unable to write length of `lastDrain`",
			"leaky: unable to write `lastDrain`",
		}
		for j, message := range errorMessages {
			rw := newFaultyReaderWriter(j+1, j+1)
			if err = bucket.Encode(rw); err != nil {
				assert.ErrorContainsf(t, err, message, "TestBucket_Encode(case:%d,msg:%d)", i, j)
			} else {
				t.Errorf("TestBucket_Encode(case:%d,msg:%d): expected error %s", i, j, message)
			}
		}
	}
}

func TestBucket_Decode(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("TestBucket_Decode(case:%d): unexpected error %v", i, err)
			continue
		}
		bucket.value = 42                                            // force a given value
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval) // prepare for 1 drain operation

		buf := &bytes.Buffer{}
		if err = bucket.Encode(buf); err != nil {
			t.Errorf("TestBucket_Decode(case:%d): unexpected error %v", i, err)
			continue
		}

		errorMessages := []string{
			"leaky: unable to read format version",
			//"leaky: unsupported format version %d",
			"leaky: unable to read `DrainBy`",
			"leaky: unable to read `DrainInterval`",
			"leaky: unable to read `Capacity`",
			"leaky: unable to read `value`",
			"leaky: unable to read size of `lastDrain`",
			"leaky: unable to read `lastDrain`",
			//"leaky: did not read entire timestamp",
			//"leaky: unable to unmarshal `lastDrain`",
		}
		for j, message := range errorMessages {
			rw := newFaultyReaderWriter(j+1, j+1)
			rw.Buffer = bytes.NewBuffer(buf.Bytes())
			bucket2, err := DecodeBucket(rw)
			assert.Nilf(t, bucket2, "TestBucket_Decode(case:%d,msg:%d)", i, j)
			if err != nil {
				assert.ErrorContainsf(t, err, message, "TestBucket_Decode(case:%d,msg:%d)", i, j)
			} else {
				t.Errorf("TestBucket_Decode(case:%d,msg:%d): expected error %s", i, j, message)
			}
		}
	}
}

func TestBucket_drain(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("TestBucket_drain(case:%d): unexpected error %v", i, err)
			continue
		}

		// Shouldn't drain when empty
		assert.Equal(t, int64(0), bucket.value)
		bucket.drain()
		assert.Equal(t, int64(0), bucket.value)

		// Still shouldn't drain when empty
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval)
		bucket.drain()
		assert.Equal(t, int64(0), bucket.value)

		// Shouldn't become negative when drained
		bucket.value = bucket.DrainBy / 2
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval)
		bucket.drain()
		assert.Equal(t, int64(0), bucket.value)

		// Should drain exactly 1 interval
		bucket.value = bucket.Capacity
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval)
		bucket.drain()
		assert.Equal(t, bucket.Capacity-bucket.DrainBy, bucket.value)

		// Should drain exactly 2 intervals, but only to zero (not become negative)
		bucket.value = bucket.DrainBy
		bucket.lastDrain = time.Now().Add(-2 * bucket.DrainInterval)
		bucket.drain()
		assert.Equal(t, int64(0), bucket.value)

		// Should drain exactly 2 intervals
		bucket.value = bucket.DrainBy * 3
		bucket.lastDrain = time.Now().Add(-2 * bucket.DrainInterval)
		bucket.drain()
		assert.Equal(t, bucket.DrainBy, bucket.value)

		// Should drain by partial intervals
		bucket.value = bucket.DrainBy * 3
		bucket.lastDrain = time.Now().Add(-1 * (bucket.DrainInterval / 2))
		bucket.drain()
		assert.Equal(t, bucket.DrainBy*3, bucket.value)
		bucket.lastDrain = bucket.lastDrain.Add(-3 * (bucket.DrainInterval / 2)) // 1.5x interval
		bucket.drain()
		assert.Equal(t, bucket.DrainBy*1, bucket.value)
		bucket.lastDrain = bucket.lastDrain.Add(-3 * (bucket.DrainInterval / 2)) // 1.5x interval
		bucket.drain()
		assert.Equal(t, int64(0), bucket.value)
	}
}

func TestBucket_Peek(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("TestBucket_Peek(case:%d): unexpected error %v", i, err)
			continue
		}

		// Doesn't drain on call, even if it could
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval)
		bucket.value = 100
		assert.Equalf(t, int64(100), bucket.Peek(), "Bucket_Peek(case:%d) should be equal", i)
	}
}

func TestBucket_Value(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("TestBucket_Value(case:%d): unexpected error %v", i, err)
			continue
		}

		// Doesn't drain on first call
		bucket.value = 100
		assert.Equalf(t, int64(100), bucket.Value(), "TestBucket_Value(case:%d) should be equal", i)

		// Does drain if required
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval)
		assert.Equalf(t, int64(95), bucket.Value(), "TestBucket_Value(case:%d) should be equal", i)
	}
}

func TestBucket_Remaining(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("TestBucket_Remaining(case:%d): unexpected error %v", i, err)
			continue
		}

		// Doesn't drain on first call
		bucket.value = 100
		assert.Equalf(t, int64(200), bucket.Remaining(), "TestBucket_Remaining(case:%d) should be equal", i)

		// Does drain if required
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval)
		assert.Equalf(t, int64(205), bucket.Remaining(), "TestBucket_Remaining(case:%d) should be equal", i)
	}
}

func TestBucket_Add(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("Bucket_Add(case:%d): unexpected error %v", i, err)
			continue
		}

		if err = bucket.Add(100); err != nil {
			t.Errorf("Bucket_Add(case:%d): unexpected Add error %v", i, err)
		}
		assert.Equalf(t, int64(100), bucket.value, "Bucket_Add(case:%d) should be equal", i)

		// Test overflow
		if err = bucket.Add(250); err != nil {
			if !errors.Is(err, ErrBucketFull) {
				t.Errorf("Bucket_Add(case:%d): expected overflow error, got %v", i, err)
			}
		} else if err == nil {
			t.Errorf("Bucket_Add(case:%d): expected overflow error, got nil", i)
		}
		assert.Equalf(t, int64(100), bucket.value, "Bucket_Add(case:%d) should be equal", i)

		// Test exact fill
		if err = bucket.Add(200); err != nil {
			t.Errorf("Bucket_Add(case:%d): unexpected Add error %v", i, err)
		}
		assert.Equalf(t, int64(300), bucket.value, "Bucket_Add(case:%d) should be equal", i)

		// Drains before add
		bucket.value = bucket.Capacity
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval)
		if err = bucket.Add(bucket.DrainBy); err != nil {
			t.Errorf("Bucket_Add(case:%d): unexpected Add error %v", i, err)
		}
		assert.Equalf(t, int64(300), bucket.value, "Bucket_Add(case:%d) should be equal", i)

		// Drains even when adding zero
		bucket.value = bucket.Capacity
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval)
		if err = bucket.Add(0); err != nil {
			t.Errorf("Bucket_Add(case:%d): unexpected Add error %v", i, err)
		}
		assert.Equalf(t, bucket.Capacity-bucket.DrainBy, bucket.value, "Bucket_Add(case:%d) should be equal", i)
	}
}

func TestBucket_Add_Drains(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("Bucket_Add_Drains(case:%d): unexpected error %v", i, err)
			continue
		}

		if err = bucket.Set(100); err != nil {
			t.Errorf("Bucket_Add_Drains(case:%d): unexpected Set error %v", i, err)
		}
		assert.Equalf(t, int64(100), bucket.value, "Bucket_Add_Drains(case:%d) should be equal", i)

		// Drains when requested
		if err = bucket.Add(-50); err != nil {
			t.Errorf("Bucket_Add_Drains(case:%d): unexpected Add error %v", i, err)
		}
		assert.Equalf(t, int64(50), bucket.value, "Bucket_Add_Drains(case:%d) should be equal", i)

		// Force an overflow and drain above the capacity limit
		bucket.value = bucket.Capacity * 2
		if err = bucket.Add(-(bucket.Capacity / 2)); err != nil {
			t.Errorf("Bucket_Add_Drains(case:%d): unexpected Add error %v", i, err)
		}
		assert.Equalf(t, (bucket.Capacity*2)-(bucket.Capacity/2), bucket.value, "Bucket_Add_Drains(case:%d) should be equal", i)
	}
}

func TestBucket_Drain(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("Bucket_Drain(case:%d): unexpected error %v", i, err)
			continue
		}

		bucket.value = bucket.Capacity

		if err = bucket.Drain(100); err != nil {
			t.Errorf("Bucket_Drain(case:%d): unexpected Drain error %v", i, err)
		}
		assert.Equalf(t, bucket.Capacity-100, bucket.value, "Bucket_Drain(case:%d) should be equal", i)

		// Test underflow
		if err = bucket.Drain(250); err != nil {
			t.Errorf("Bucket_Drain(case:%d): expected no error, got %v", i, err)
		}
		assert.Equalf(t, int64(0), bucket.value, "Bucket_Drain(case:%d) should be equal", i)

		// Test exact drain
		bucket.value = 200
		if err = bucket.Drain(200); err != nil {
			t.Errorf("Bucket_Drain(case:%d): unexpected Drain error %v", i, err)
		}
		assert.Equalf(t, int64(0), bucket.value, "Bucket_Drain(case:%d) should be equal", i)

		// Drains before draining further
		bucket.value = bucket.Capacity
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval)
		if err = bucket.Drain(bucket.DrainBy); err != nil {
			t.Errorf("Bucket_Drain(case:%d): unexpected Drain error %v", i, err)
		}
		assert.Equalf(t, bucket.Capacity-(bucket.DrainBy*2), bucket.value, "Bucket_Drain(case:%d) should be equal", i)

		// Drains even when draining zero
		bucket.value = bucket.Capacity
		bucket.lastDrain = time.Now().Add(-1 * bucket.DrainInterval)
		if err = bucket.Drain(0); err != nil {
			t.Errorf("Bucket_Drain(case:%d): unexpected Add error %v", i, err)
		}
		assert.Equalf(t, bucket.Capacity-bucket.DrainBy, bucket.value, "Bucket_Drain(case:%d) should be equal", i)
	}
}

func TestBucket_Drain_Adds(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("Bucket_Drain_Adds(case:%d): unexpected error %v", i, err)
			continue
		}

		if err = bucket.Set(100); err != nil {
			t.Errorf("Bucket_Drain_Adds(case:%d): unexpected Set error %v", i, err)
		}
		assert.Equalf(t, int64(100), bucket.value, "Bucket_Drain_Adds(case:%d) should be equal", i)

		// Adds when requested
		if err = bucket.Drain(-50); err != nil {
			t.Errorf("Bucket_Drain_Adds(case:%d): unexpected Drain error %v", i, err)
		}
		assert.Equalf(t, int64(150), bucket.value, "Bucket_Drain_Adds(case:%d) should be equal", i)

		// Test overflow
		bucket.value = bucket.Capacity
		if err = bucket.Drain(-1); err != nil {
			if !errors.Is(err, ErrBucketFull) {
				t.Errorf("Bucket_Drain_Adds(case:%d): expected overflow error, got %v", i, err)
			}
		} else if err == nil {
			t.Errorf("Bucket_Drain_Adds(case:%d): expected overflow error, got nil", i)
		}
		assert.Equalf(t, bucket.Capacity, bucket.value, "Bucket_Drain_Adds(case:%d) should be equal", i)
	}
}

func TestBucket_Set(t *testing.T) {
	for i, createFn := range createCaseFunctions {
		bucket, err := createFn(5, time.Minute, 300)
		if err != nil {
			t.Errorf("TestBucket_Set(case:%d): unexpected error %v", i, err)
			continue
		}

		// Must be positive value
		if err = bucket.Set(-1); err != nil {
			assert.EqualErrorf(t, err, "leaky: bucket value cannot be negative", "TestBucket_Set(case:%d)", i)
		} else {
			t.Errorf("TestBucket_Set(case:%d): expected error, got nil", i)
		}

		// Must be less than capacity
		if err = bucket.Set(bucket.Capacity + 1); err != nil {
			assert.EqualErrorf(t, err, "leaky: bucket value cannot exceed capacity", "TestBucket_Set(case:%d)", i)
		} else {
			t.Errorf("TestBucket_Set(case:%d): expected error, got nil", i)
		}

		// Can be zero, and resets lastDrain, and doesn't drain
		bucket.lastDrain = time.Now().Add(-5 * bucket.DrainInterval)
		if err = bucket.Set(0); err != nil {
			t.Errorf("TestBucket_Set(case:%d): unexpected Set error %v", i, err)
		}
		assert.Equal(t, int64(0), bucket.value)
		assert.InDeltaf(t, 0*time.Millisecond, time.Since(bucket.lastDrain), float64(10*time.Millisecond), "TestBucket_Set(case:%d)", i)

		// Can be positive, and resets lastDrain, and doesn't drain
		bucket.lastDrain = time.Now().Add(-5 * bucket.DrainInterval)
		if err = bucket.Set(5); err != nil {
			t.Errorf("TestBucket_Set(case:%d): unexpected Set error %v", i, err)
		}
		assert.Equal(t, int64(5), bucket.value)
		assert.InDeltaf(t, 0*time.Millisecond, time.Since(bucket.lastDrain), float64(10*time.Millisecond), "TestBucket_Set(case:%d)", i)
	}
}
