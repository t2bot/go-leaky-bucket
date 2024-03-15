package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/t2bot/go-leaky-bucket"
)

func main() {
	bucket, err := leaky.NewBucket(5, time.Minute, 300)
	if err != nil {
		panic(err) // TODO: Handle error
	}

	// Try to add to the bucket
	if err = bucket.Add(50); errors.Is(err, leaky.ErrBucketFull) {
		panic("bucket is full") // or cancel the request, return a 429, etc
	} else if err != nil {
		panic(err) // TODO: Handle error
	} else {
		// continue processing normally
		fmt.Println("Size after add:", bucket.Value())
	}

	// Inspect the bucket
	// All of these operations cause a drain to happen before returning a value.
	fmt.Println("Remaining capacity:", bucket.Remaining())
	fmt.Println("Size:", bucket.Value())

	// Inspect the bucket *without* causing a drain
	// Caution: it may have been a while since the last drain. You probably want `.Value()`
	fmt.Println("Undrained size:", bucket.Peek())

	// Force the bucket to have a particular size
	if err = bucket.Set(42); err != nil {
		panic(err) // TODO: Handle error
	} else {
		// The bucket is now set to 42, and the drain has been reset. Calling `.Value()` or similar
		// right now would not cause the size to decrease.
		fmt.Println("Size after Set:", bucket.Value()) // will not drain, even if a minute had passed
	}

	// Expand the bucket in any direction
	bucket.Capacity = 700
	bucket.DrainBy = 40
	bucket.DrainInterval = time.Hour
	fmt.Println("Remaining capacity after expansion:", bucket.Remaining())
	fmt.Println("Size after expansion:", bucket.Value())
}
