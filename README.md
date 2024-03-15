# go-leaky-bucket
[Leaky bucket meter](https://en.wikipedia.org/wiki/Leaky_bucket#As_a_meter) implementation in Go.

This implementation atomically drains when its value is being mutated rather than using a timer or continual
drain. Before mutation, the bucket will drain the supplied number of units as many times as necessary to match
the precision of the supplied interval.

For example, if a bucket is created which drains 5 units every 2 minutes, then after 2.5 minutes only 5 units will
be drained. However, the 30 seconds of "unused" drain time will be accounted for to ensure future drains are kept
accurate. If another 1.5 minutes were to pass, the bucket will drain by another 5 units because the unused time
was recorded.

As a bonus, this implementation supports encoding and decoding the bucket in binary, allowing it to be persisted
across application restarts or shared among processes as needed. Synchronization logic is left as an exercise for
the consumer.

See [`./examples`](./examples) for usage and inspiration.