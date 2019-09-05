[![API Documentation](https://godoc.org/github.com/pascaldekloe/redis?status.svg)](https://godoc.org/github.com/pascaldekloe/redis)
[![Build Status](https://circleci.com/gh/pascaldekloe/redis.svg?style=svg)](https://circleci.com/gh/pascaldekloe/redis)

## About

… a redis client for the Go programming language.

This is free and unencumbered software released into the
[public domain](https://creativecommons.org/publicdomain/zero/1.0).


## Performance

Unix domain sockets provide a significant boost, when compared to TCP.
The following results were measured on a E5-1650 v2 (from the year 2013).

```
name                             old time/op  new time/op  delta
SimpleString/sequential-12       63.6µs ± 0%  34.4µs ± 1%  -46.03%  (p=0.000 n=16+16)
SimpleString/parallel-12         20.3µs ± 1%   6.1µs ± 3%  -70.03%  (p=0.000 n=15+16)
Integer/sequential-12            62.0µs ± 0%  33.2µs ± 0%  -46.51%  (p=0.000 n=15+16)
Integer/parallel-12              20.0µs ± 2%   5.9µs ± 2%  -70.42%  (p=0.000 n=16+14)
BulkString/1B/sequential-12      62.8µs ± 0%  33.7µs ± 0%  -46.34%  (p=0.000 n=16+16)
BulkString/1B/parallel-12        20.2µs ± 1%   6.0µs ± 1%  -70.49%  (p=0.000 n=14+15)
BulkString/144B/sequential-12    63.6µs ± 0%  34.6µs ± 0%  -45.57%  (p=0.000 n=16+14)
BulkString/144B/parallel-12      20.3µs ± 2%   6.1µs ± 2%  -70.01%  (p=0.000 n=16+16)
BulkString/20736B/sequential-12  80.0µs ± 1%  80.8µs ± 0%   +1.04%  (p=0.000 n=15+15)
BulkString/20736B/parallel-12    33.6µs ± 4%  54.5µs ± 0%  +62.21%  (p=0.000 n=14+13)
Array/1×8B/sequential-12         65.8µs ± 0%  36.1µs ± 0%  -45.10%  (p=0.000 n=13+14)
Array/1×8B/parallel-12           20.6µs ± 1%   6.5µs ± 4%  -68.63%  (p=0.000 n=16+16)
Array/12×8B/sequential-12        68.4µs ± 0%  38.8µs ± 0%  -43.31%  (p=0.000 n=13+15)
Array/12×8B/parallel-12          20.6µs ± 0%   7.4µs ± 5%  -63.98%  (p=0.000 n=13+15)
Array/144×8B/sequential-12       90.9µs ± 0%  60.9µs ± 0%  -32.99%  (p=0.000 n=16+16)
Array/144×8B/parallel-12         26.2µs ± 3%  23.7µs ± 2%   -9.27%  (p=0.000 n=14+16)
```
