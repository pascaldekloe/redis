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

Note how memory allocations are limited to user data only.
The publish–subscribe pattern is entirely allocation free.

The following results were measured on an Intel i5-7500.
Unix domain sockets can make a big improvement over TCP.

```
name                              tcp time/op    unix time/op     delta
SimpleString/sequential-4            31.1µs ± 1%     15.2µs ± 0%     -51.06%  (p=0.000 n=9+10)
SimpleString/parallel-4              9.75µs ± 3%     4.74µs ±13%     -51.34%  (p=0.000 n=10+10)
Integer/sequential-4                 30.3µs ± 1%     14.4µs ± 0%     -52.69%  (p=0.000 n=10+10)
Integer/parallel-4                   9.84µs ±13%     4.45µs ± 9%     -54.81%  (p=0.000 n=10+10)
Bulk/8B/sequential/bytes-4           31.0µs ± 1%     14.7µs ± 0%     -52.71%  (p=0.000 n=10+10)
Bulk/8B/sequential/string-4          31.1µs ± 1%     14.7µs ± 0%     -52.84%  (p=0.000 n=8+10)
Bulk/8B/parallel/bytes-4             9.76µs ± 4%     4.52µs ± 6%     -53.75%  (p=0.000 n=10+10)
Bulk/8B/parallel/string-4            9.84µs ± 5%     4.60µs ± 9%     -53.30%  (p=0.000 n=10+10)
Bulk/800B/sequential/bytes-4         32.8µs ± 1%     16.2µs ± 1%     -50.69%  (p=0.000 n=10+9)
Bulk/800B/sequential/string-4        32.8µs ± 1%     16.2µs ± 0%     -50.42%  (p=0.000 n=10+10)
Bulk/800B/parallel/bytes-4           10.4µs ± 7%      5.3µs ± 5%     -49.08%  (p=0.000 n=10+10)
Bulk/800B/parallel/string-4          10.5µs ± 6%      5.8µs ± 6%     -44.79%  (p=0.000 n=10+10)
Bulk/24000B/sequential/bytes-4       45.2µs ± 1%     43.9µs ± 1%      -2.83%  (p=0.000 n=9+10)
Bulk/24000B/parallel/bytes-4         20.3µs ±10%     35.7µs ± 1%     +76.33%  (p=0.000 n=10+10)
Array/1×8B/sequential/bytes-4        32.7µs ± 2%     16.9µs ± 0%     -48.37%  (p=0.000 n=10+10)
Array/1×8B/sequential/string-4       32.7µs ± 2%     16.8µs ± 0%     -48.42%  (p=0.000 n=10+10)
Array/1×8B/parallel/bytes-4          10.4µs ± 5%      5.4µs ± 5%     -47.63%  (p=0.000 n=9+10)
Array/1×8B/parallel/string-4         10.4µs ± 6%      5.4µs ± 3%     -48.15%  (p=0.000 n=10+9)
Array/12×8B/sequential/bytes-4       34.9µs ± 1%     18.5µs ± 1%     -46.92%  (p=0.000 n=9+10)
Array/12×8B/sequential/string-4      34.6µs ± 1%     18.3µs ± 0%     -46.93%  (p=0.000 n=9+10)
Array/12×8B/parallel/bytes-4         11.5µs ± 4%      6.2µs ± 6%     -46.07%  (p=0.000 n=10+10)
Array/12×8B/parallel/string-4        11.1µs ± 7%      6.3µs ± 5%     -43.60%  (p=0.000 n=10+10)
Array/144×8B/sequential/bytes-4      53.1µs ± 1%     33.9µs ± 0%     -36.05%  (p=0.000 n=10+9)
Array/144×8B/sequential/string-4     52.4µs ± 0%     33.4µs ± 1%     -36.29%  (p=0.000 n=8+10)
Array/144×8B/parallel/bytes-4        19.1µs ± 5%     14.4µs ± 1%     -24.61%  (p=0.000 n=10+10)
Array/144×8B/parallel/string-4       19.0µs ± 6%     14.1µs ± 1%     -26.07%  (p=0.000 n=10+9)
PubSub/8B/1publishers-4              11.2µs ± 4%      5.6µs ± 0%     -50.28%  (p=0.000 n=10+8)
PubSub/8B/2publishers-4              7.18µs ±10%     3.96µs ± 4%     -44.85%  (p=0.000 n=10+9)
PubSub/8B/16publishers-4             2.27µs ± 5%     1.94µs ± 4%     -14.63%  (p=0.000 n=10+10)
PubSub/800B/1publishers-4            12.0µs ± 6%      6.0µs ± 3%     -49.73%  (p=0.000 n=10+10)
PubSub/800B/2publishers-4            7.67µs ± 4%     4.58µs ± 1%     -40.30%  (p=0.000 n=10+8)
PubSub/800B/16publishers-4           2.85µs ±14%     3.19µs ± 2%     +11.64%  (p=0.000 n=9+10)
PubSub/24000B/1publishers-4          32.5µs ± 9%     41.3µs ± 2%     +27.17%  (p=0.000 n=10+9)
PubSub/24000B/2publishers-4          33.0µs ±10%     41.3µs ± 2%     +25.03%  (p=0.000 n=10+9)
PubSub/24000B/16publishers-4         30.5µs ± 5%     42.4µs ± 4%     +39.17%  (p=0.000 n=10+10)

name                              tcp alloc/op   unix alloc/op    delta
SimpleString/sequential-4             0.00B           0.00B             ~     (all equal)
SimpleString/parallel-4               0.00B           0.00B             ~     (all equal)
Integer/sequential-4                  0.00B           0.00B             ~     (all equal)
Integer/parallel-4                    0.00B           0.00B             ~     (all equal)
Bulk/8B/sequential/bytes-4            8.00B ± 0%      8.00B ± 0%        ~     (all equal)
Bulk/8B/sequential/string-4           8.00B ± 0%      8.00B ± 0%        ~     (all equal)
Bulk/8B/parallel/bytes-4              8.00B ± 0%      8.00B ± 0%        ~     (all equal)
Bulk/8B/parallel/string-4             8.00B ± 0%      8.00B ± 0%        ~     (all equal)
Bulk/800B/sequential/bytes-4           896B ± 0%       896B ± 0%        ~     (all equal)
Bulk/800B/sequential/string-4          896B ± 0%       896B ± 0%        ~     (all equal)
Bulk/800B/parallel/bytes-4             896B ± 0%       896B ± 0%        ~     (all equal)
Bulk/800B/parallel/string-4            896B ± 0%       896B ± 0%        ~     (all equal)
Bulk/24000B/sequential/bytes-4       24.6kB ± 0%     24.6kB ± 0%      -0.01%  (p=0.002 n=8+10)
Bulk/24000B/parallel/bytes-4         24.6kB ± 0%     24.6kB ± 0%      +0.00%  (p=0.000 n=10+10)
Array/1×8B/sequential/bytes-4         40.0B ± 0%      40.0B ± 0%        ~     (all equal)
Array/1×8B/sequential/string-4        24.0B ± 0%      24.0B ± 0%        ~     (all equal)
Array/1×8B/parallel/bytes-4           40.0B ± 0%      40.0B ± 0%        ~     (all equal)
Array/1×8B/parallel/string-4          24.0B ± 0%      24.0B ± 0%        ~     (all equal)
Array/12×8B/sequential/bytes-4         384B ± 0%       384B ± 0%        ~     (all equal)
Array/12×8B/sequential/string-4        288B ± 0%       288B ± 0%        ~     (all equal)
Array/12×8B/parallel/bytes-4           384B ± 0%       384B ± 0%        ~     (all equal)
Array/12×8B/parallel/string-4          288B ± 0%       288B ± 0%        ~     (all equal)
Array/144×8B/sequential/bytes-4      4.61kB ± 0%     4.61kB ± 0%        ~     (all equal)
Array/144×8B/sequential/string-4     3.46kB ± 0%     3.46kB ± 0%        ~     (all equal)
Array/144×8B/parallel/bytes-4        4.61kB ± 0%     4.61kB ± 0%        ~     (all equal)
Array/144×8B/parallel/string-4       3.46kB ± 0%     3.46kB ± 0%        ~     (all equal)
PubSub/8B/1publishers-4               0.00B           0.00B             ~     (all equal)
PubSub/8B/2publishers-4               0.00B           0.00B             ~     (all equal)
PubSub/8B/16publishers-4              0.00B           0.00B             ~     (all equal)
PubSub/800B/1publishers-4             0.00B           0.00B             ~     (all equal)
PubSub/800B/2publishers-4             0.00B           0.00B             ~     (all equal)
PubSub/800B/16publishers-4            0.00B           0.00B             ~     (all equal)
PubSub/24000B/1publishers-4           2.60B ±23%      3.33B ±50%     +28.21%  (p=0.037 n=10+9)
PubSub/24000B/2publishers-4           5.00B ± 0%      7.60B ±21%     +52.00%  (p=0.000 n=8+10)
PubSub/24000B/16publishers-4          40.4B ±11%      57.4B ±10%     +42.08%  (p=0.000 n=10+10)

name                              tcp allocs/op  unix allocs/op   delta
SimpleString/sequential-4              0.00            0.00             ~     (all equal)
SimpleString/parallel-4                0.00            0.00             ~     (all equal)
Integer/sequential-4                   0.00            0.00             ~     (all equal)
Integer/parallel-4                     0.00            0.00             ~     (all equal)
Bulk/8B/sequential/bytes-4             1.00 ± 0%       1.00 ± 0%        ~     (all equal)
Bulk/8B/sequential/string-4            1.00 ± 0%       1.00 ± 0%        ~     (all equal)
Bulk/8B/parallel/bytes-4               1.00 ± 0%       1.00 ± 0%        ~     (all equal)
Bulk/8B/parallel/string-4              1.00 ± 0%       1.00 ± 0%        ~     (all equal)
Bulk/800B/sequential/bytes-4           1.00 ± 0%       1.00 ± 0%        ~     (all equal)
Bulk/800B/sequential/string-4          1.00 ± 0%       1.00 ± 0%        ~     (all equal)
Bulk/800B/parallel/bytes-4             1.00 ± 0%       1.00 ± 0%        ~     (all equal)
Bulk/800B/parallel/string-4            1.00 ± 0%       1.00 ± 0%        ~     (all equal)
Bulk/24000B/sequential/bytes-4         1.00 ± 0%       1.00 ± 0%        ~     (all equal)
Bulk/24000B/parallel/bytes-4           1.00 ± 0%       1.00 ± 0%        ~     (all equal)
Array/1×8B/sequential/bytes-4          2.00 ± 0%       2.00 ± 0%        ~     (all equal)
Array/1×8B/sequential/string-4         2.00 ± 0%       2.00 ± 0%        ~     (all equal)
Array/1×8B/parallel/bytes-4            2.00 ± 0%       2.00 ± 0%        ~     (all equal)
Array/1×8B/parallel/string-4           2.00 ± 0%       2.00 ± 0%        ~     (all equal)
Array/12×8B/sequential/bytes-4         13.0 ± 0%       13.0 ± 0%        ~     (all equal)
Array/12×8B/sequential/string-4        13.0 ± 0%       13.0 ± 0%        ~     (all equal)
Array/12×8B/parallel/bytes-4           13.0 ± 0%       13.0 ± 0%        ~     (all equal)
Array/12×8B/parallel/string-4          13.0 ± 0%       13.0 ± 0%        ~     (all equal)
Array/144×8B/sequential/bytes-4         145 ± 0%        145 ± 0%        ~     (all equal)
Array/144×8B/sequential/string-4        145 ± 0%        145 ± 0%        ~     (all equal)
Array/144×8B/parallel/bytes-4           145 ± 0%        145 ± 0%        ~     (all equal)
Array/144×8B/parallel/string-4          145 ± 0%        145 ± 0%        ~     (all equal)
PubSub/8B/1publishers-4                0.00            0.00             ~     (all equal)
PubSub/8B/2publishers-4                0.00            0.00             ~     (all equal)
PubSub/8B/16publishers-4               0.00            0.00             ~     (all equal)
PubSub/800B/1publishers-4              0.00            0.00             ~     (all equal)
PubSub/800B/2publishers-4              0.00            0.00             ~     (all equal)
PubSub/800B/16publishers-4             0.00            0.00             ~     (all equal)
PubSub/24000B/1publishers-4            0.00            0.00             ~     (all equal)
PubSub/24000B/2publishers-4            0.00            0.00             ~     (all equal)
PubSub/24000B/16publishers-4           0.00            0.00             ~     (all equal)

name                              tcp speed      unix speed       delta
Bulk/8B/sequential/bytes-4          260kB/s ± 0%    547kB/s ± 1%    +110.38%  (p=0.000 n=10+10)
Bulk/8B/sequential/string-4         260kB/s ± 0%    543kB/s ± 1%    +108.85%  (p=0.000 n=9+10)
Bulk/8B/parallel/bytes-4            821kB/s ± 4%   1773kB/s ± 6%    +115.96%  (p=0.000 n=10+10)
Bulk/8B/parallel/string-4           813kB/s ± 5%   1743kB/s ± 8%    +114.39%  (p=0.000 n=10+10)
Bulk/800B/sequential/bytes-4       24.4MB/s ± 1%   49.4MB/s ± 1%    +102.80%  (p=0.000 n=10+9)
Bulk/800B/sequential/string-4      24.4MB/s ± 1%   49.2MB/s ± 0%    +101.71%  (p=0.000 n=10+10)
Bulk/800B/parallel/bytes-4         77.0MB/s ± 7%  151.0MB/s ± 5%     +96.23%  (p=0.000 n=10+10)
Bulk/800B/parallel/string-4        76.6MB/s ± 6%  138.7MB/s ± 6%     +81.11%  (p=0.000 n=10+10)
Bulk/24000B/sequential/bytes-4      531MB/s ± 1%    546MB/s ± 1%      +2.91%  (p=0.000 n=9+10)
Bulk/24000B/parallel/bytes-4       1.19GB/s ±10%   0.67GB/s ± 1%     -43.43%  (p=0.000 n=10+10)
Array/1×8B/sequential/bytes-4       246kB/s ± 2%    470kB/s ± 0%     +91.06%  (p=0.000 n=10+8)
Array/1×8B/sequential/string-4      244kB/s ± 2%    474kB/s ± 1%     +94.26%  (p=0.000 n=10+10)
Array/1×8B/parallel/bytes-4         773kB/s ± 6%   1477kB/s ± 5%     +90.99%  (p=0.000 n=9+10)
Array/1×8B/parallel/string-4        769kB/s ± 5%   1470kB/s ± 7%     +91.16%  (p=0.000 n=10+10)
Array/12×8B/sequential/bytes-4     2.75MB/s ± 1%   5.18MB/s ± 1%     +88.48%  (p=0.000 n=9+10)
Array/12×8B/sequential/string-4    2.78MB/s ± 1%   5.23MB/s ± 0%     +88.35%  (p=0.000 n=9+10)
Array/12×8B/parallel/bytes-4       8.34MB/s ± 4%  15.47MB/s ± 6%     +85.53%  (p=0.000 n=10+10)
Array/12×8B/parallel/string-4      8.67MB/s ± 8%  15.35MB/s ± 5%     +77.10%  (p=0.000 n=10+10)
Array/144×8B/sequential/bytes-4    21.7MB/s ± 1%   34.0MB/s ± 0%     +56.38%  (p=0.000 n=10+9)
Array/144×8B/sequential/string-4   22.0MB/s ± 0%   34.5MB/s ± 0%     +56.96%  (p=0.000 n=8+10)
Array/144×8B/parallel/bytes-4      60.3MB/s ± 5%   79.9MB/s ± 1%     +32.59%  (p=0.000 n=10+10)
Array/144×8B/parallel/string-4     60.6MB/s ± 6%   81.9MB/s ± 1%     +35.12%  (p=0.000 n=10+9)
PubSub/8B/1publishers-4             715kB/s ± 3%   1439kB/s ± 1%    +101.22%  (p=0.000 n=10+8)
PubSub/8B/2publishers-4            1.12MB/s ±11%   2.03MB/s ± 1%     +81.74%  (p=0.000 n=10+8)
PubSub/8B/16publishers-4           3.52MB/s ± 5%   4.12MB/s ± 4%     +17.04%  (p=0.000 n=10+10)
PubSub/800B/1publishers-4          67.0MB/s ± 6%  133.2MB/s ± 3%     +98.77%  (p=0.000 n=10+10)
PubSub/800B/2publishers-4           104MB/s ± 4%    174MB/s ± 2%     +66.97%  (p=0.000 n=10+9)
PubSub/800B/16publishers-4          275MB/s ±22%    251MB/s ± 2%      -8.78%  (p=0.002 n=10+10)
PubSub/24000B/1publishers-4         741MB/s ± 9%    581MB/s ± 2%     -21.61%  (p=0.000 n=10+9)
PubSub/24000B/2publishers-4         729MB/s ±11%    581MB/s ± 2%     -20.23%  (p=0.000 n=10+9)
PubSub/24000B/16publishers-4        788MB/s ± 4%    566MB/s ± 4%     -28.17%  (p=0.000 n=10+10)

name                              tcp ns/delay   unix ns/delay    delta
PubSub/8B/1publishers-4               48.4k ± 3%      20.9k ± 0%     -56.84%  (p=0.000 n=10+8)
PubSub/8B/2publishers-4               58.2k ± 9%      27.6k ± 1%     -52.52%  (p=0.000 n=10+8)
PubSub/8B/16publishers-4               150k ± 3%       114k ± 1%     -24.05%  (p=0.000 n=8+10)
PubSub/800B/1publishers-4             51.7k ± 6%      22.8k ± 4%     -55.98%  (p=0.000 n=10+10)
PubSub/800B/2publishers-4             62.1k ± 4%      33.1k ± 3%     -46.71%  (p=0.000 n=10+10)
PubSub/800B/16publishers-4             207k ±29%     33119k ±23%  +15901.25%  (p=0.000 n=10+9)
PubSub/24000B/1publishers-4            140k ± 8%     7430k ±169%   +5211.72%  (p=0.000 n=10+10)
PubSub/24000B/2publishers-4            274k ±10%     8110k ±125%   +2862.26%  (p=0.000 n=10+10)
PubSub/24000B/16publishers-4          1.96M ± 5%      3.23M ±24%     +64.95%  (p=0.000 n=10+9)

name                              tcp ns/publish unix ns/publish  delta
PubSub/8B/1publishers-4               44.7k ± 4%      22.2k ± 0%     -50.27%  (p=0.000 n=10+8)
PubSub/8B/2publishers-4               57.4k ±10%      31.7k ± 4%     -44.85%  (p=0.000 n=10+9)
PubSub/8B/16publishers-4               145k ± 5%       124k ± 4%     -14.60%  (p=0.000 n=10+10)
PubSub/800B/1publishers-4             47.8k ± 6%      24.0k ± 3%     -49.73%  (p=0.000 n=10+10)
PubSub/800B/2publishers-4             61.3k ± 4%      36.6k ± 1%     -40.30%  (p=0.000 n=10+8)
PubSub/800B/16publishers-4             182k ±14%       202k ± 2%     +10.63%  (p=0.000 n=9+10)
PubSub/24000B/1publishers-4            130k ± 9%       164k ± 2%     +26.48%  (p=0.000 n=10+9)
PubSub/24000B/2publishers-4            264k ±10%       328k ± 2%     +24.16%  (p=0.000 n=10+9)
PubSub/24000B/16publishers-4          1.95M ± 5%      2.71M ± 4%     +39.09%  (p=0.000 n=10+10)
```
