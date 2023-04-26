## About

… a [Redis](https://redis.io/topics/introduction) client for the Go programming
language.

* Boring API with full type-safety
* Automatic [pipelining](https://redis.io/topics/pipelining) on concurrency
* High throughput despite low footprint
* Efficient OS and network utilization
* Robust error recovery

Network I/O is executed in the same goroutine that invoked the 
[Client](https://godoc.org/github.com/pascaldekloe/redis#Client).
There is no internal error reporting/logging by design.

This is free and unencumbered software released into the
[public domain](https://creativecommons.org/publicdomain/zero/1.0).

[![Go Reference](https://pkg.go.dev/badge/github.com/pascaldekloe/redis.svg)](https://pkg.go.dev/github.com/pascaldekloe/redis)
[![Build Status](https://github.com/pascaldekloe/redis/actions/workflows/go.yml/badge.svg)](https://github.com/pascaldekloe/redis/actions/workflows/go.yml)


## Synchonous Command Execution

```go
// Redis is a thread-safe connection establishment.
var Redis = redis.NewClient[string,string]("rds1.example.com", time.Second/2, 0)

// Grow adds a string to a list.
func Grow() {
	newLen, err := Redis.RPUSH("demo_list", "hello")
	if err != nil {
		log.Print("demo_list update error: ", err)
		return
	}
	log.Printf("demo_list has %d elements", newLen)
}

// Ping pushes a message to a publish–subscribe channel.
func Ping() {
	clientCount, err := Redis.PUBLISH("demo_channel", "ping")
	if err != nil {
		log.Print("demo_channel publish error: ", err)
		return
	}
	log.Printf("pinged %d clients in demo_channel", clientCount)
}
```


## Zero-Copy [Pub/Sub](https://redis.io/topics/pubsub) Reception

```go
// RedisListener is a thread-safe connection establishment.
var RedisListener = redis.NewListener(redis.ListenerConfig{
	Func: func(channel string, message []byte, err error) {
		switch err {
		case nil:
			log.Printf("received %q on %q", message, channel)
		case redis.ErrClosed:
			log.Print("subscription establishment terminated")
		case io.ErrShortBuffer: // see ListenerConfig BufferSize
			log.Printf("message on %q skipped due size", channel)
		default:
			log.Print("subscription error: ", err)
			// recovery attempts follow automatically
		}
	},
	Addr: "rds1.example.com:6379",
})

// Peek listens shortly to a publish–subscribe channel.
func Peek() {
	RedisListener.SUBSCRIBE("demo_channel")
	time.Sleep(time.Second)
	RedisListener.UNSUBSCRIBE("demo_channel")
}
```


## Performance

• Memory allocations are limited to user data only.
• Subscription receival does not allocate any memory.
• Unix domain sockets make a big improvement over TCP.

Redis.conf needs `client-output-buffer-limit pubsub 256mb 256mb 60` to prevent
`scheduled to be closed ASAP for overcoming of output buffer limits` during
pub/sub benchmarks.

The following results were measured with Redis version 7, Go version 1.20, on an Apple M1.

```
name                              TCP time/op    Unix time/op     delta
SimpleString/sequential-8            23.1µs ± 0%     10.2µs ± 0%     -55.73%  (p=0.000 n=10+10)
SimpleString/parallel-8              4.13µs ± 0%     1.89µs ± 1%     -54.15%  (p=0.000 n=10+9)
Integer/sequential-8                 22.3µs ± 0%     10.3µs ± 0%     -53.93%  (p=0.000 n=10+9)
Integer/parallel-8                   4.00µs ± 0%     1.90µs ± 0%     -52.59%  (p=0.000 n=9+10)
Bulk/8B/sequential/bytes-8           23.1µs ± 0%     10.3µs ± 0%     -55.48%  (p=0.000 n=9+10)
Bulk/8B/sequential/string-8          23.1µs ± 0%     10.3µs ± 0%     -55.44%  (p=0.000 n=10+10)
Bulk/8B/parallel/bytes-8             4.07µs ± 0%     1.91µs ± 0%     -53.10%  (p=0.000 n=10+10)
Bulk/8B/parallel/string-8            4.07µs ± 0%     1.91µs ± 1%     -52.99%  (p=0.000 n=9+10)
Bulk/800B/sequential/bytes-8         23.6µs ± 0%     10.5µs ± 0%     -55.44%  (p=0.000 n=9+9)
Bulk/800B/sequential/string-8        23.5µs ± 0%     10.5µs ± 0%     -55.44%  (p=0.000 n=10+10)
Bulk/800B/parallel/bytes-8           4.19µs ± 0%     2.47µs ± 1%     -40.97%  (p=0.000 n=10+10)
Bulk/800B/parallel/string-8          4.22µs ± 0%     3.17µs ± 0%     -24.86%  (p=0.000 n=10+10)
Bulk/24000B/sequential/bytes-8       30.0µs ± 1%     34.8µs ± 0%     +16.00%  (p=0.000 n=10+9)
Bulk/24000B/sequential/string-8      33.8µs ± 0%     39.0µs ± 0%     +15.53%  (p=0.000 n=9+10)
Bulk/24000B/parallel/bytes-8         8.60µs ± 1%    28.53µs ± 0%    +231.91%  (p=0.000 n=10+10)
Bulk/24000B/parallel/string-8        13.9µs ± 2%     31.6µs ± 0%    +126.55%  (p=0.000 n=9+10)
Array/1×8B/sequential/bytes-8        23.5µs ± 0%     10.5µs ± 0%     -55.41%  (p=0.000 n=10+10)
Array/1×8B/sequential/string-8       23.4µs ± 0%     10.4µs ± 0%     -55.40%  (p=0.000 n=10+10)
Array/1×8B/parallel/bytes-8          4.21µs ± 0%     1.93µs ± 1%     -54.05%  (p=0.000 n=10+10)
Array/1×8B/parallel/string-8         4.20µs ± 0%     1.93µs ± 0%     -53.98%  (p=0.000 n=9+10)
Array/12×8B/sequential/bytes-8       24.6µs ± 0%     11.8µs ± 0%     -52.25%  (p=0.000 n=10+10)
Array/12×8B/sequential/string-8      24.4µs ± 1%     11.6µs ± 0%     -52.29%  (p=0.000 n=9+7)
Array/12×8B/parallel/bytes-8         4.41µs ± 1%     2.62µs ± 0%     -40.48%  (p=0.000 n=9+10)
Array/12×8B/parallel/string-8        4.54µs ± 1%     2.59µs ± 3%     -43.05%  (p=0.000 n=10+10)
Array/144×8B/sequential/bytes-8      35.8µs ± 0%     24.3µs ± 0%     -31.97%  (p=0.000 n=9+8)
Array/144×8B/sequential/string-8     34.8µs ± 0%     23.4µs ± 0%     -32.58%  (p=0.000 n=9+10)
Array/144×8B/parallel/bytes-8        13.6µs ± 2%     11.5µs ± 0%     -14.86%  (p=0.000 n=10+9)
Array/144×8B/parallel/string-8       13.0µs ± 1%     10.1µs ± 0%     -22.12%  (p=0.000 n=10+10)
PubSub/8B/1publishers-8              4.71µs ± 1%     2.05µs ±12%     -56.56%  (p=0.000 n=10+10)
PubSub/8B/2publishers-8              2.70µs ± 1%     1.90µs ± 9%     -29.74%  (p=0.000 n=9+10)
PubSub/8B/16publishers-8             1.77µs ± 1%     1.57µs ± 7%     -11.01%  (p=0.000 n=9+10)
PubSub/800B/1publishers-8            4.93µs ± 1%     2.15µs ± 1%     -56.30%  (p=0.000 n=10+8)
PubSub/800B/2publishers-8            2.85µs ± 0%     1.83µs ± 5%     -35.94%  (p=0.000 n=9+9)
PubSub/800B/16publishers-8           1.83µs ± 1%     1.62µs ± 1%     -11.51%  (p=0.000 n=10+9)
PubSub/24000B/1publishers-8          10.7µs ± 3%     36.9µs ±15%    +244.31%  (p=0.000 n=10+10)
PubSub/24000B/2publishers-8          10.9µs ± 1%     36.4µs ±16%    +232.35%  (p=0.000 n=10+10)
PubSub/24000B/16publishers-8         11.1µs ± 1%     36.5µs ±15%    +228.50%  (p=0.000 n=8+10)

name                              TCP speed      Unix speed       delta
Bulk/8B/sequential/bytes-8          350kB/s ± 0%    780kB/s ± 0%    +122.86%  (p=0.000 n=10+10)
Bulk/8B/sequential/string-8         350kB/s ± 0%    780kB/s ± 0%    +122.86%  (p=0.000 n=10+10)
Bulk/8B/parallel/bytes-8           1.96MB/s ± 0%   4.19MB/s ± 1%    +113.35%  (p=0.000 n=10+10)
Bulk/8B/parallel/string-8          1.97MB/s ± 0%   4.18MB/s ± 1%    +112.69%  (p=0.000 n=9+10)
Bulk/800B/sequential/bytes-8       34.0MB/s ± 0%   76.2MB/s ± 0%    +124.40%  (p=0.000 n=9+9)
Bulk/800B/sequential/string-8      34.0MB/s ± 0%   76.3MB/s ± 0%    +124.39%  (p=0.000 n=10+10)
Bulk/800B/parallel/bytes-8          191MB/s ± 0%    323MB/s ± 1%     +69.40%  (p=0.000 n=10+10)
Bulk/800B/parallel/string-8         189MB/s ± 0%    252MB/s ± 0%     +33.08%  (p=0.000 n=10+10)
Bulk/24000B/sequential/bytes-8      800MB/s ± 1%    690MB/s ± 0%     -13.80%  (p=0.000 n=10+9)
Bulk/24000B/sequential/string-8     711MB/s ± 0%    615MB/s ± 0%     -13.44%  (p=0.000 n=9+10)
Bulk/24000B/parallel/bytes-8       2.79GB/s ± 1%   0.84GB/s ± 0%     -69.87%  (p=0.000 n=10+10)
Bulk/24000B/parallel/string-8      1.72GB/s ± 2%   0.76GB/s ± 0%     -55.86%  (p=0.000 n=9+10)
Array/1×8B/sequential/bytes-8       340kB/s ± 0%    763kB/s ± 1%    +124.41%  (p=0.000 n=10+10)
Array/1×8B/sequential/string-8      340kB/s ± 0%    770kB/s ± 0%    +126.47%  (p=0.000 n=10+8)
Array/1×8B/parallel/bytes-8        1.90MB/s ± 0%   4.14MB/s ± 1%    +117.84%  (p=0.000 n=9+10)
Array/1×8B/parallel/string-8       1.90MB/s ± 0%   4.14MB/s ± 0%    +117.28%  (p=0.000 n=9+9)
Array/12×8B/sequential/bytes-8     3.90MB/s ± 0%   8.16MB/s ± 0%    +109.40%  (p=0.000 n=10+9)
Array/12×8B/sequential/string-8    3.93MB/s ± 0%   8.25MB/s ± 0%    +109.75%  (p=0.000 n=8+7)
Array/12×8B/parallel/bytes-8       21.8MB/s ± 1%   36.6MB/s ± 0%     +68.01%  (p=0.000 n=9+10)
Array/12×8B/parallel/string-8      21.1MB/s ± 1%   37.1MB/s ± 3%     +75.62%  (p=0.000 n=10+10)
Array/144×8B/sequential/bytes-8    32.2MB/s ± 0%   47.3MB/s ± 0%     +47.00%  (p=0.000 n=9+8)
Array/144×8B/sequential/string-8   33.1MB/s ± 0%   49.2MB/s ± 0%     +48.34%  (p=0.000 n=9+10)
Array/144×8B/parallel/bytes-8      84.9MB/s ± 2%   99.8MB/s ± 0%     +17.45%  (p=0.000 n=10+9)
Array/144×8B/parallel/string-8     88.5MB/s ± 1%  113.6MB/s ± 0%     +28.40%  (p=0.000 n=10+10)
PubSub/8B/1publishers-8            1.70MB/s ± 2%   3.92MB/s ±11%    +131.00%  (p=0.000 n=10+10)
PubSub/8B/2publishers-8            2.96MB/s ± 1%   4.22MB/s ± 9%     +42.74%  (p=0.000 n=9+10)
PubSub/8B/16publishers-8           4.53MB/s ± 1%   5.09MB/s ± 8%     +12.44%  (p=0.000 n=9+10)
PubSub/800B/1publishers-8           162MB/s ± 1%    372MB/s ± 1%    +128.82%  (p=0.000 n=10+8)
PubSub/800B/2publishers-8           280MB/s ± 0%    434MB/s ± 9%     +54.67%  (p=0.000 n=9+10)
PubSub/800B/16publishers-8          437MB/s ± 1%    494MB/s ± 1%     +13.00%  (p=0.000 n=10+9)
PubSub/24000B/1publishers-8        2.24GB/s ± 3%   0.65GB/s ±14%     -70.78%  (p=0.000 n=10+10)
PubSub/24000B/2publishers-8        2.19GB/s ± 1%   0.67GB/s ±15%     -69.65%  (p=0.000 n=10+10)
PubSub/24000B/16publishers-8       2.16GB/s ± 1%   0.66GB/s ±14%     -69.36%  (p=0.000 n=8+10)

name                              TCP ns/delay   Unix ns/delay    delta
PubSub/8B/1publishers-8               35.8k ± 1%     60.0k ±117%        ~     (p=0.481 n=10+10)
PubSub/8B/2publishers-8               39.7k ± 1%   1289.5k ±139%   +3149.67%  (p=0.034 n=8+10)
PubSub/8B/16publishers-8               225k ± 1%      836k ±133%    +271.99%  (p=0.034 n=8+10)
PubSub/800B/1publishers-8             37.3k ± 1%      15.5k ±16%     -58.38%  (p=0.000 n=9+8)
PubSub/800B/2publishers-8             42.3k ± 0%     86.0k ±152%        ~     (p=0.497 n=9+10)
PubSub/800B/16publishers-8             233k ± 0%       266k ±24%     +14.25%  (p=0.000 n=9+8)
PubSub/24000B/1publishers-8          34.8M ±132%      42.1M ±87%        ~     (p=0.720 n=10+9)
PubSub/24000B/2publishers-8            178k ± 1%     45255k ±77%  +25294.45%  (p=0.000 n=10+9)
PubSub/24000B/16publishers-8          1.42M ± 1%     46.32M ±45%   +3160.60%  (p=0.000 n=8+9)

name                              TCP ns/publish Unix ns/publish  delta
PubSub/8B/1publishers-8               37.7k ± 1%      16.4k ±12%     -56.57%  (p=0.000 n=10+10)
PubSub/8B/2publishers-8               43.2k ± 1%      30.4k ± 9%     -29.73%  (p=0.000 n=9+10)
PubSub/8B/16publishers-8               226k ± 1%       201k ± 8%     -11.00%  (p=0.000 n=9+10)
PubSub/800B/1publishers-8             39.4k ± 1%      17.2k ± 1%     -56.29%  (p=0.000 n=10+8)
PubSub/800B/2publishers-8             45.6k ± 0%      29.2k ± 5%     -35.94%  (p=0.000 n=9+9)
PubSub/800B/16publishers-8             234k ± 1%       207k ± 1%     -11.59%  (p=0.000 n=10+9)
PubSub/24000B/1publishers-8           82.9k ± 6%     285.4k ±15%    +244.32%  (p=0.000 n=10+10)
PubSub/24000B/2publishers-8            175k ± 1%       559k ±28%    +219.06%  (p=0.000 n=10+10)
PubSub/24000B/16publishers-8          1.42M ± 1%      4.49M ±19%    +217.02%  (p=0.000 n=8+10)
```
