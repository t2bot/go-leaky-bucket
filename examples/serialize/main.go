package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/t2bot/go-leaky-bucket"
)

func main() {
	bucket, err := leaky.NewBucket(5, time.Minute, 300)
	if err != nil {
		panic(err) // TODO: Handle error
	}

	// Set a value so we can compare later
	if err = bucket.Set(42 /*resetDrain=*/, true); err != nil {
		panic(err) // TODO: Handle error
	}

	fmt.Println("Bucket before serialization:")
	printBucket(bucket)

	// Serialize the bucket
	buf := &bytes.Buffer{}
	if err = bucket.Encode(buf); err != nil {
		panic(err) // TODO: Handle error
	}

	// Deserialize the bucket
	var bucket2 *leaky.Bucket
	if bucket2, err = leaky.DecodeBucket(buf); err != nil {
		panic(err) // TODO: Handle error
	}

	fmt.Println("Bucket after deserialization:")
	printBucket(bucket2)
}

func printBucket(bucket *leaky.Bucket) {
	fmt.Println("  Size:", bucket.Value())
	fmt.Println("  Capacity:", bucket.Capacity)
	fmt.Println("  DrainBy:", bucket.DrainBy)
	fmt.Println("  DrainInterval:", bucket.DrainInterval)
}
