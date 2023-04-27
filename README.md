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

Version 2 [github.com/pascaldekloe/redis/v2] utilizes generics.

[![Go Reference](https://pkg.go.dev/badge/github.com/pascaldekloe/redis/v2.svg)](https://pkg.go.dev/github.com/pascaldekloe/redis/v2)
[![Build Status](https://github.com/pascaldekloe/redis/actions/workflows/go.yml/badge.svg)](https://github.com/pascaldekloe/redis/actions/workflows/go.yml)


## Synchonous Command Execution

```go
// Redis is a thread-safe connection establishment.
var Redis = redis.NewDefaultClient[string,string]("rds1.example.com")

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
• Unix domain sockets significantly reduce latency.
• TCP performs on large payloads and high concurrency.

Configure Redis with `client-output-buffer-limit pubsub 512mb 512mb 60` for the benchmarks.

The following results were measured with Redis version 7, and Go version 1.20, on an Apple M1.

```
goos: darwin
goarch: arm64
pkg: github.com/pascaldekloe/redis/v2
                            │   tcp.txt    │               unix.txt                │
                            │    sec/op    │    sec/op      vs base                │
SimpleString/sequential-8     18.507µ ± 1%    6.452µ ±  1%   -65.14% (p=0.000 n=8)
SimpleString/parallel-8        3.603µ ± 0%    1.631µ ±  1%   -54.74% (p=0.000 n=8)
Integer/sequential-8          18.211µ ± 0%    6.327µ ±  0%   -65.26% (p=0.000 n=8)
Integer/parallel-8             3.499µ ± 0%    1.618µ ±  0%   -53.77% (p=0.000 n=8)
Bulk/8B/sequential-8          18.419µ ± 0%    6.418µ ±  0%   -65.15% (p=0.000 n=8)
Bulk/8B/parallel-8             3.541µ ± 0%    1.631µ ±  1%   -53.95% (p=0.000 n=8)
Bulk/800B/sequential-8        18.724µ ± 0%    6.717µ ±  0%   -64.13% (p=0.000 n=8)
Bulk/800B/parallel-8           3.620µ ± 4%    2.034µ ±  0%   -43.80% (p=0.000 n=8)
Bulk/8000B/sequential-8       21.159µ ± 1%    8.904µ ±  0%   -57.92% (p=0.000 n=8)
Bulk/8000B/parallel-8          5.226µ ± 5%    6.201µ ±  0%   +18.65% (p=0.000 n=8)
Array/1×8B/sequential-8       18.841µ ± 0%    6.659µ ±  0%   -64.66% (p=0.000 n=8)
Array/1×8B/parallel-8          3.946µ ± 4%    1.722µ ±  2%   -56.36% (p=0.000 n=8)
Array/12×8B/sequential-8      19.996µ ± 1%    7.755µ ±  0%   -61.22% (p=0.000 n=8)
Array/12×8B/parallel-8         4.172µ ± 4%    2.233µ ±  5%   -46.49% (p=0.000 n=8)
Array/144×8B/sequential-8      31.16µ ± 0%    19.37µ ±  1%   -37.85% (p=0.000 n=8)
Array/144×8B/parallel-8        9.729µ ± 3%    9.110µ ±  3%    -6.36% (p=0.000 n=8)
PubSub/8B/1publishers-8        4.572µ ± 0%    1.928µ ±  3%   -57.84% (p=0.000 n=8)
PubSub/8B/2publishers-8        2.631µ ± 0%    1.530µ ±  4%   -41.83% (p=0.000 n=8)
PubSub/8B/16publishers-8       1.411µ ± 1%    1.335µ ±  8%    -5.39% (p=0.000 n=8)
PubSub/800B/1publishers-8      4.845µ ± 1%    2.241µ ±  2%   -53.75% (p=0.000 n=8)
PubSub/800B/2publishers-8      2.815µ ± 2%    1.831µ ±  3%   -34.96% (p=0.000 n=8)
PubSub/800B/16publishers-8     1.435µ ± 1%    1.933µ ±  5%   +34.67% (p=0.000 n=8)
PubSub/8000B/1publishers-8     5.471µ ± 0%   13.661µ ± 16%  +149.71% (p=0.000 n=8)
PubSub/8000B/2publishers-8     5.185µ ± 2%   11.883µ ± 13%  +129.20% (p=0.000 n=8)
PubSub/8000B/16publishers-8    5.009µ ± 2%   13.477µ ± 11%  +169.05% (p=0.000 n=8)
geomean                        6.465µ         4.096µ         -36.65%

                            │    tcp.txt    │                unix.txt                │
                            │      B/s      │      B/s        vs base                │
Bulk/8B/sequential-8           419.9Ki ± 0%   1220.7Ki ±  1%  +190.70% (p=0.000 n=8)
Bulk/8B/parallel-8             2.155Mi ± 0%    4.678Mi ±  1%  +117.04% (p=0.000 n=8)
Bulk/800B/sequential-8         40.75Mi ± 0%   113.59Mi ±  0%  +178.78% (p=0.000 n=8)
Bulk/800B/parallel-8           210.7Mi ± 3%    375.0Mi ±  0%   +77.96% (p=0.000 n=8)
Bulk/8000B/sequential-8        360.6Mi ± 0%    856.9Mi ±  0%  +137.64% (p=0.000 n=8)
Bulk/8000B/parallel-8          1.426Gi ± 3%    1.202Gi ±  1%   -15.72% (p=0.000 n=8)
Array/1×8B/sequential-8        410.2Ki ± 2%   1171.9Ki ±  0%  +185.71% (p=0.000 n=8)
Array/1×8B/parallel-8          1.931Mi ± 2%    4.435Mi ±  3%  +129.63% (p=0.000 n=8)
Array/12×8B/sequential-8       4.578Mi ± 0%   11.806Mi ±  0%  +157.92% (p=0.000 n=8)
Array/12×8B/parallel-8         21.94Mi ± 2%    41.01Mi ±  4%   +86.90% (p=0.000 n=8)
Array/144×8B/sequential-8      35.26Mi ± 0%    56.72Mi ±  2%   +60.87% (p=0.000 n=8)
Array/144×8B/parallel-8        112.9Mi ± 1%    120.6Mi ±  3%    +6.79% (p=0.000 n=8)
PubSub/8B/1publishers-8        1.669Mi ± 1%    3.958Mi ±  8%  +137.14% (p=0.000 n=8)
PubSub/8B/2publishers-8        2.899Mi ± 0%    4.988Mi ±  4%   +72.04% (p=0.000 n=8)
PubSub/8B/16publishers-8       5.407Mi ± 1%    5.717Mi ±  1%    +5.73% (p=0.000 n=8)
PubSub/800B/1publishers-8      157.5Mi ± 1%    340.5Mi ±  9%  +116.22% (p=0.000 n=8)
PubSub/800B/2publishers-8      271.1Mi ± 2%    416.8Mi ±  4%   +53.75% (p=0.000 n=8)
PubSub/800B/16publishers-8     531.7Mi ± 0%    394.8Mi ±  7%   -25.75% (p=0.000 n=8)
PubSub/8000B/1publishers-8    1394.7Mi ± 2%    558.9Mi ± 15%   -59.92% (p=0.000 n=8)
PubSub/8000B/2publishers-8    1471.6Mi ± 7%    642.3Mi ± 16%   -56.35% (p=0.000 n=8)
PubSub/8000B/16publishers-8   1523.2Mi ± 2%    566.2Mi ± 11%   -62.83% (p=0.000 n=8)
geomean                        38.78Mi         56.13Mi         +44.74%
```
