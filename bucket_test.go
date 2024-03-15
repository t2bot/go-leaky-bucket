package leaky

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
		if err = bucket.Set(-1, true); err != nil {
			assert.EqualErrorf(t, err, "leaky: bucket value cannot be negative", "TestBucket_Set(case:%d)", i)
		} else {
			t.Errorf("TestBucket_Set(case:%d): expected error, got nil", i)
		}

		// Must be less than capacity
		if err = bucket.Set(bucket.Capacity+1, true); err != nil {
			assert.EqualErrorf(t, err, "leaky: bucket value cannot exceed capacity", "TestBucket_Set(case:%d)", i)
		} else {
			t.Errorf("TestBucket_Set(case:%d): expected error, got nil", i)
		}

		// Can be zero, and resets lastDrain, and doesn't drain
		bucket.lastDrain = time.Now().Add(-5 * bucket.DrainInterval)
		if err = bucket.Set(0, true); err != nil {
			t.Errorf("TestBucket_Set(case:%d): unexpected Set error %v", i, err)
		}
		assert.Equal(t, int64(0), bucket.value)
		assert.InDeltaf(t, 0*time.Millisecond, time.Since(bucket.lastDrain), float64(10*time.Millisecond), "TestBucket_Set(case:%d)", i)

		// Can be positive, and resets lastDrain, and doesn't drain
		bucket.lastDrain = time.Now().Add(-5 * bucket.DrainInterval)
		if err = bucket.Set(5, true); err != nil {
			t.Errorf("TestBucket_Set(case:%d): unexpected Set error %v", i, err)
		}
		assert.Equal(t, int64(5), bucket.value)
		assert.InDeltaf(t, 0*time.Millisecond, time.Since(bucket.lastDrain), float64(10*time.Millisecond), "TestBucket_Set(case:%d)", i)

		// Doesn't reset lastDrain when resetDrain=false
		drainTime := time.Now().Add(-5 * bucket.DrainInterval)
		bucket.lastDrain = drainTime
		if err = bucket.Set(10, false); err != nil {
			t.Errorf("TestBucket_Set(case:%d): unexpected Set error %v", i, err)
		}
		assert.Equalf(t, int64(10), bucket.value, "TestBucket_Set(case:%d) should be equal", i)
		assert.Equalf(t, drainTime, bucket.lastDrain, "TestBucket_Set(case:%d) should be equal", i)
	}
}
