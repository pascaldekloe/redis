[![API Documentation](https://godoc.org/github.com/pascaldekloe/redis?status.svg)](https://godoc.org/github.com/pascaldekloe/redis)
[![Build Status](https://circleci.com/gh/pascaldekloe/redis.svg?style=svg)](https://circleci.com/gh/pascaldekloe/redis)

## About

… a redis client for the Go programming language.

The implementation utilises a single network connection with asynchronous I/O.
Connection multiplexing causes overhead and higher latencies for all commands.

This is free and unencumbered software released into the
[public domain](https://creativecommons.org/publicdomain/zero/1.0).


## Performance

The following results were measured on a E5-1650 v2 (from the year 2013).

#### TCP

```
name                             time/op
SimpleString/sequential-12       53.6µs ± 0%
SimpleString/parallel-12         8.73µs ± 1%
Integer/sequential-12            52.5µs ± 0%
Integer/parallel-12              8.65µs ± 1%
BulkString/1B/sequential-12      52.7µs ± 0%
BulkString/1B/parallel-12        8.71µs ± 1%
BulkString/144B/sequential-12    52.4µs ± 1%
BulkString/144B/parallel-12      8.87µs ± 0%
BulkString/20736B/sequential-12  70.0µs ± 0%
BulkString/20736B/parallel-12    29.3µs ± 5%
Array/1×8B/sequential-12         55.1µs ± 0%
Array/1×8B/parallel-12           9.00µs ± 1%
Array/12×8B/sequential-12        55.6µs ± 0%
Array/12×8B/parallel-12          9.58µs ± 1%
Array/144×8B/sequential-12       85.5µs ± 0%
Array/144×8B/parallel-12         21.1µs ± 2%

name                             alloc/op
SimpleString/sequential-12        0.00B     
SimpleString/parallel-12          0.00B     
Integer/sequential-12             0.00B     
Integer/parallel-12               0.00B     
BulkString/1B/sequential-12       1.00B ± 0%
BulkString/1B/parallel-12         1.00B ± 0%
BulkString/144B/sequential-12      144B ± 0%
BulkString/144B/parallel-12        144B ± 0%
BulkString/20736B/sequential-12  21.8kB ± 0%
BulkString/20736B/parallel-12    21.8kB ± 0%
Array/1×8B/sequential-12          40.0B ± 0%
Array/1×8B/parallel-12            40.0B ± 0%
Array/12×8B/sequential-12          384B ± 0%
Array/12×8B/parallel-12            384B ± 0%
Array/144×8B/sequential-12       4.61kB ± 0%
Array/144×8B/parallel-12         4.61kB ± 0%

name                             allocs/op
SimpleString/sequential-12         0.00     
SimpleString/parallel-12           0.00     
Integer/sequential-12              0.00     
Integer/parallel-12                0.00     
BulkString/1B/sequential-12        1.00 ± 0%
BulkString/1B/parallel-12          1.00 ± 0%
BulkString/144B/sequential-12      1.00 ± 0%
BulkString/144B/parallel-12        1.00 ± 0%
BulkString/20736B/sequential-12    1.00 ± 0%
BulkString/20736B/parallel-12      1.00 ± 0%
Array/1×8B/sequential-12           2.00 ± 0%
Array/1×8B/parallel-12             2.00 ± 0%
Array/12×8B/sequential-12          13.0 ± 0%
Array/12×8B/parallel-12            13.0 ± 0%
Array/144×8B/sequential-12          145 ± 0%
Array/144×8B/parallel-12            145 ± 0%
```

#### Unix Domain Socket

```
name                             time/op
SimpleString/sequential-12       33.3µs ± 1%
SimpleString/parallel-12         4.77µs ± 1%
Integer/sequential-12            32.4µs ± 0%
Integer/parallel-12              4.73µs ± 0%
BulkString/1B/sequential-12      33.0µs ± 0%
BulkString/1B/parallel-12        4.74µs ± 1%
BulkString/144B/sequential-12    33.8µs ± 0%
BulkString/144B/parallel-12      4.81µs ± 1%
BulkString/20736B/sequential-12  82.5µs ± 0%
BulkString/20736B/parallel-12    50.3µs ± 1%
Array/1×8B/sequential-12         35.5µs ± 0%
Array/1×8B/parallel-12           4.96µs ± 1%
Array/12×8B/sequential-12        36.7µs ± 0%
Array/12×8B/parallel-12          5.67µs ± 1%
Array/144×8B/sequential-12       57.0µs ± 0%
Array/144×8B/parallel-12         18.6µs ± 1%

name                             alloc/op
SimpleString/sequential-12        0.00B     
SimpleString/parallel-12          0.00B     
Integer/sequential-12             0.00B     
Integer/parallel-12               0.00B     
BulkString/1B/sequential-12       1.00B ± 0%
BulkString/1B/parallel-12         1.00B ± 0%
BulkString/144B/sequential-12      144B ± 0%
BulkString/144B/parallel-12        144B ± 0%
BulkString/20736B/sequential-12  21.8kB ± 0%
BulkString/20736B/parallel-12    21.8kB ± 0%
Array/1×8B/sequential-12          40.0B ± 0%
Array/1×8B/parallel-12            40.0B ± 0%
Array/12×8B/sequential-12          384B ± 0%
Array/12×8B/parallel-12            384B ± 0%
Array/144×8B/sequential-12       4.61kB ± 0%
Array/144×8B/parallel-12         4.61kB ± 0%

name                             allocs/op
SimpleString/sequential-12         0.00     
SimpleString/parallel-12           0.00     
Integer/sequential-12              0.00     
Integer/parallel-12                0.00     
BulkString/1B/sequential-12        1.00 ± 0%
BulkString/1B/parallel-12          1.00 ± 0%
BulkString/144B/sequential-12      1.00 ± 0%
BulkString/144B/parallel-12        1.00 ± 0%
BulkString/20736B/sequential-12    1.00 ± 0%
BulkString/20736B/parallel-12      1.00 ± 0%
Array/1×8B/sequential-12           2.00 ± 0%
Array/1×8B/parallel-12             2.00 ± 0%
Array/12×8B/sequential-12          13.0 ± 0%
Array/12×8B/parallel-12            13.0 ± 0%
Array/144×8B/sequential-12          145 ± 0%
Array/144×8B/parallel-12            145 ± 0%
```
