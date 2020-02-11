[![API Documentation](https://godoc.org/github.com/pascaldekloe/redis?status.svg)](https://godoc.org/github.com/pascaldekloe/redis)
[![Build Status](https://circleci.com/gh/pascaldekloe/redis.svg?style=svg)](https://circleci.com/gh/pascaldekloe/redis)
[![Code Report](https://goreportcard.com/badge/github.com/pascaldekloe/redis)](https://goreportcard.com/report/github.com/pascaldekloe/redis)

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


## Synchonous Command Execution

```go
// Redis is a thread-safe connection establishment.
var Redis = redis.NewClient("rds1.example.com", time.Second/2, 0)

// Grow adds a string to a list.
func Grow() {
	newLen, err := Redis.RPUSHString("demo_list", "hello")
	if err != nil {
		log.Print("demo_list update error: ", err)
		return
	}
	log.Printf("demo_list has %d elements", newLen)
}

// Ping pushes a message to a publish–subscribe channel.
func Ping() {
	clientCount, err := Redis.PUBLISHString("demo_channel", "ping")
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

The following results were measured on a i5-7500 with TCP.

```
name                              time/op
SimpleString/sequential-4           31.1µs ± 1%
SimpleString/parallel-4             9.88µs ± 5%
Integer/sequential-4                30.4µs ± 2%
Integer/parallel-4                  9.46µs ± 5%
Bulk/8B/sequential/bytes-4          30.5µs ± 1%
Bulk/8B/sequential/string-4         30.4µs ± 1%
Bulk/8B/parallel/bytes-4            9.55µs ± 4%
Bulk/8B/parallel/string-4           9.67µs ± 2%
Bulk/800B/sequential/bytes-4        31.9µs ± 1%
Bulk/800B/sequential/string-4       31.8µs ± 0%
Bulk/800B/parallel/bytes-4          10.7µs ± 4%
Bulk/800B/parallel/string-4         10.7µs ± 8%
Bulk/24000B/sequential/bytes-4      42.5µs ± 0%
Bulk/24000B/sequential/string-4     58.0µs ± 0%
Bulk/24000B/parallel/bytes-4        19.0µs ± 7%
Bulk/24000B/parallel/string-4       37.4µs ± 0%
Array/1×8B/sequential/bytes-4       32.8µs ± 1%
Array/1×8B/sequential/string-4      32.7µs ± 0%
Array/1×8B/parallel/bytes-4         10.5µs ± 6%
Array/1×8B/parallel/string-4        10.3µs ± 7%
Array/12×8B/sequential/bytes-4      33.3µs ± 1%
Array/12×8B/sequential/string-4     32.8µs ± 1%
Array/12×8B/parallel/bytes-4        11.1µs ± 8%
Array/12×8B/parallel/string-4       11.0µs ± 7%
Array/144×8B/sequential/bytes-4     50.9µs ± 0%
Array/144×8B/sequential/string-4    50.6µs ± 0%
Array/144×8B/parallel/bytes-4       20.7µs ± 5%
Array/144×8B/parallel/string-4      20.6µs ± 9%
PubSub/8B/1publishers-4             33.4µs ± 0%
PubSub/8B/2publishers-4             20.1µs ± 4%
PubSub/8B/16publishers-4            4.55µs ± 5%
PubSub/800B/1publishers-4           34.3µs ± 0%
PubSub/800B/2publishers-4           20.8µs ± 6%
PubSub/800B/16publishers-4          5.55µs ±11%
PubSub/24000B/1publishers-4         44.4µs ± 1%
PubSub/24000B/2publishers-4         34.4µs ± 4%
PubSub/24000B/16publishers-4        35.2µs ± 4%

name                              alloc/op
SimpleString/sequential-4            0.00B     
SimpleString/parallel-4              0.00B     
Integer/sequential-4                 0.00B     
Integer/parallel-4                   0.00B     
Bulk/8B/sequential/bytes-4           8.00B ± 0%
Bulk/8B/sequential/string-4          8.00B ± 0%
Bulk/8B/parallel/bytes-4             8.00B ± 0%
Bulk/8B/parallel/string-4            8.00B ± 0%
Bulk/800B/sequential/bytes-4          896B ± 0%
Bulk/800B/sequential/string-4         896B ± 0%
Bulk/800B/parallel/bytes-4            896B ± 0%
Bulk/800B/parallel/string-4           896B ± 0%
Bulk/24000B/sequential/bytes-4      24.6kB ± 0%
Bulk/24000B/sequential/string-4     24.6kB ± 0%
Bulk/24000B/parallel/bytes-4        24.6kB ± 0%
Bulk/24000B/parallel/string-4       24.6kB ± 0%
Array/1×8B/sequential/bytes-4        40.0B ± 0%
Array/1×8B/sequential/string-4       24.0B ± 0%
Array/1×8B/parallel/bytes-4          40.0B ± 0%
Array/1×8B/parallel/string-4         24.0B ± 0%
Array/12×8B/sequential/bytes-4        384B ± 0%
Array/12×8B/sequential/string-4       288B ± 0%
Array/12×8B/parallel/bytes-4          384B ± 0%
Array/12×8B/parallel/string-4         288B ± 0%
Array/144×8B/sequential/bytes-4     4.61kB ± 0%
Array/144×8B/sequential/string-4    3.46kB ± 0%
Array/144×8B/parallel/bytes-4       4.61kB ± 0%
Array/144×8B/parallel/string-4      3.46kB ± 0%
PubSub/8B/1publishers-4              48.0B ± 0%
PubSub/8B/2publishers-4              48.0B ± 0%
PubSub/8B/16publishers-4             48.0B ± 0%
PubSub/800B/1publishers-4             936B ± 0%
PubSub/800B/2publishers-4             936B ± 0%
PubSub/800B/16publishers-4            936B ± 0%
PubSub/24000B/1publishers-4         24.7kB ± 0%
PubSub/24000B/2publishers-4         24.8kB ± 0%
PubSub/24000B/16publishers-4        24.9kB ± 0%

name                              allocs/op
SimpleString/sequential-4             0.00     
SimpleString/parallel-4               0.00     
Integer/sequential-4                  0.00     
Integer/parallel-4                    0.00     
Bulk/8B/sequential/bytes-4            1.00 ± 0%
Bulk/8B/sequential/string-4           1.00 ± 0%
Bulk/8B/parallel/bytes-4              1.00 ± 0%
Bulk/8B/parallel/string-4             1.00 ± 0%
Bulk/800B/sequential/bytes-4          1.00 ± 0%
Bulk/800B/sequential/string-4         1.00 ± 0%
Bulk/800B/parallel/bytes-4            1.00 ± 0%
Bulk/800B/parallel/string-4           1.00 ± 0%
Bulk/24000B/sequential/bytes-4        1.00 ± 0%
Bulk/24000B/sequential/string-4       1.00 ± 0%
Bulk/24000B/parallel/bytes-4          1.00 ± 0%
Bulk/24000B/parallel/string-4         1.00 ± 0%
Array/1×8B/sequential/bytes-4         2.00 ± 0%
Array/1×8B/sequential/string-4        2.00 ± 0%
Array/1×8B/parallel/bytes-4           2.00 ± 0%
Array/1×8B/parallel/string-4          2.00 ± 0%
Array/12×8B/sequential/bytes-4        13.0 ± 0%
Array/12×8B/sequential/string-4       13.0 ± 0%
Array/12×8B/parallel/bytes-4          13.0 ± 0%
Array/12×8B/parallel/string-4         13.0 ± 0%
Array/144×8B/sequential/bytes-4        145 ± 0%
Array/144×8B/sequential/string-4       145 ± 0%
Array/144×8B/parallel/bytes-4          145 ± 0%
Array/144×8B/parallel/string-4         145 ± 0%
PubSub/8B/1publishers-4               3.00 ± 0%
PubSub/8B/2publishers-4               3.00 ± 0%
PubSub/8B/16publishers-4              3.00 ± 0%
PubSub/800B/1publishers-4             3.00 ± 0%
PubSub/800B/2publishers-4             3.00 ± 0%
PubSub/800B/16publishers-4            3.00 ± 0%
PubSub/24000B/1publishers-4           3.00 ± 0%
PubSub/24000B/2publishers-4           3.00 ± 0%
PubSub/24000B/16publishers-4          3.00 ± 0%

name                              speed
Bulk/8B/sequential/bytes-4         260kB/s ± 0%
Bulk/8B/sequential/string-4        260kB/s ± 0%
Bulk/8B/parallel/bytes-4           833kB/s ± 6%
Bulk/8B/parallel/string-4          827kB/s ± 2%
Bulk/800B/sequential/bytes-4      25.1MB/s ± 1%
Bulk/800B/sequential/string-4     25.1MB/s ± 0%
Bulk/800B/parallel/bytes-4        74.6MB/s ± 4%
Bulk/800B/parallel/string-4       74.1MB/s ± 9%
Bulk/24000B/sequential/bytes-4     565MB/s ± 0%
Bulk/24000B/sequential/string-4    414MB/s ± 0%
Bulk/24000B/parallel/bytes-4      1.27GB/s ± 7%
Bulk/24000B/parallel/string-4      641MB/s ± 0%
Array/1×8B/sequential/bytes-4      240kB/s ± 0%
Array/1×8B/sequential/string-4     244kB/s ± 2%
Array/1×8B/parallel/bytes-4        762kB/s ± 6%
Array/1×8B/parallel/string-4       780kB/s ± 6%
Array/12×8B/sequential/bytes-4    2.89MB/s ± 1%
Array/12×8B/sequential/string-4   2.93MB/s ± 1%
Array/12×8B/parallel/bytes-4      8.63MB/s ± 8%
Array/12×8B/parallel/string-4     8.78MB/s ± 8%
Array/144×8B/sequential/bytes-4   22.6MB/s ± 0%
Array/144×8B/sequential/string-4  22.8MB/s ± 0%
Array/144×8B/parallel/bytes-4     55.8MB/s ± 5%
Array/144×8B/parallel/string-4    56.2MB/s ± 9%
PubSub/8B/1publishers-4            240kB/s ± 0%
PubSub/8B/2publishers-4            397kB/s ± 4%
PubSub/8B/16publishers-4          1.76MB/s ± 4%
PubSub/800B/1publishers-4         23.3MB/s ± 0%
PubSub/800B/2publishers-4         38.5MB/s ± 6%
PubSub/800B/16publishers-4         145MB/s ±11%
PubSub/24000B/1publishers-4        540MB/s ± 1%
PubSub/24000B/2publishers-4        699MB/s ± 4%
PubSub/24000B/16publishers-4       682MB/s ± 4%

name                              ns/delay
PubSub/8B/1publishers-4              42.9k ± 0%
PubSub/8B/2publishers-4              46.8k ± 4%
PubSub/8B/16publishers-4             71.4k ± 4%
PubSub/800B/1publishers-4            44.7k ± 0%
PubSub/800B/2publishers-4            49.2k ± 6%
PubSub/800B/16publishers-4           91.8k ±10%
PubSub/24000B/1publishers-4          65.5k ± 1%
PubSub/24000B/2publishers-4          87.3k ± 3%
PubSub/24000B/16publishers-4          583k ± 4%

name                              ns/publish
PubSub/8B/1publishers-4              33.4k ± 0%
PubSub/8B/2publishers-4              40.3k ± 4%
PubSub/8B/16publishers-4             71.9k ±10%
PubSub/800B/1publishers-4            34.3k ± 0%
PubSub/800B/2publishers-4            41.6k ± 6%
PubSub/800B/16publishers-4           88.2k ±10%
PubSub/24000B/1publishers-4          44.4k ± 1%
PubSub/24000B/2publishers-4          68.7k ± 4%
PubSub/24000B/16publishers-4          563k ± 4%
```
