[![API Documentation](https://godoc.org/github.com/pascaldekloe/redis?status.svg)](https://godoc.org/github.com/pascaldekloe/redis)
[![Build Status](https://circleci.com/gh/pascaldekloe/redis.svg?style=svg)](https://circleci.com/gh/pascaldekloe/redis)

## About

… a Redis client for the Go programming language.

The implementation utilises a single network connection with asynchronous I/O.
Connection multiplexing causes overhead and higher latencies for all commands.

This is free and unencumbered software released into the
[public domain](https://creativecommons.org/publicdomain/zero/1.0).


## Performance

A local Unix domain socket connection is roughly twice as fast than TCP.
The following results were measured on a E5-1650 v2 (from the year 2013).

```
name                             TCP time/op    Unix time/op   delta
SimpleString/sequential-12         54.1µs ± 0%    27.9µs ± 1%  -48.44%  (p=0.000 n=10+10)
SimpleString/parallel-12           7.19µs ± 1%    3.52µs ± 1%  -50.95%  (p=0.000 n=10+10)
Integer/sequential-12              51.4µs ± 0%    27.5µs ± 4%  -46.58%  (p=0.000 n=10+10)
Integer/parallel-12                6.99µs ± 0%    3.47µs ± 1%  -50.39%  (p=0.000 n=9+10)
BulkString/1B/sequential-12        51.7µs ± 0%    26.0µs ± 1%  -49.77%  (p=0.000 n=10+10)
BulkString/1B/parallel-12          7.22µs ± 1%    3.47µs ± 1%  -51.99%  (p=0.000 n=9+10)
BulkString/144B/sequential-12      52.5µs ± 1%    27.6µs ± 2%  -47.37%  (p=0.000 n=10+10)
BulkString/144B/parallel-12        7.46µs ± 1%    3.48µs ± 0%  -53.32%  (p=0.000 n=9+9)
BulkString/20736B/sequential-12    64.5µs ± 0%    78.2µs ± 0%  +21.24%  (p=0.000 n=10+9)
BulkString/20736B/parallel-12      33.5µs ± 2%    61.0µs ± 0%  +81.79%  (p=0.000 n=9+10)
Array/1×8B/sequential-12           54.4µs ± 1%    28.7µs ± 2%  -47.23%  (p=0.000 n=10+9)
Array/1×8B/parallel-12             7.75µs ± 0%    4.51µs ± 1%  -41.76%  (p=0.000 n=8+10)
Array/12×8B/sequential-12          55.4µs ± 0%    31.7µs ± 0%  -42.81%  (p=0.000 n=10+9)
Array/12×8B/parallel-12            8.85µs ± 2%    7.36µs ± 1%  -16.79%  (p=0.000 n=10+10)
Array/144×8B/sequential-12         79.5µs ± 0%    52.2µs ± 0%  -34.37%  (p=0.000 n=10+10)
Array/144×8B/parallel-12           28.7µs ± 0%    25.9µs ± 0%   -9.75%  (p=0.000 n=9+8)

name                             TCP alloc/op   Unix alloc/op  delta
SimpleString/sequential-12          0.00B          0.00B          ~     (all equal)
SimpleString/parallel-12            0.00B          0.00B          ~     (all equal)
Integer/sequential-12               0.00B          0.00B          ~     (all equal)
Integer/parallel-12                 0.00B          0.00B          ~     (all equal)
BulkString/1B/sequential-12         1.00B ± 0%     1.00B ± 0%     ~     (all equal)
BulkString/1B/parallel-12           1.00B ± 0%     1.00B ± 0%     ~     (all equal)
BulkString/144B/sequential-12        144B ± 0%      144B ± 0%     ~     (all equal)
BulkString/144B/parallel-12          144B ± 0%      144B ± 0%     ~     (all equal)
BulkString/20736B/sequential-12    21.8kB ± 0%    21.8kB ± 0%   +0.01%  (p=0.000 n=10+10)
BulkString/20736B/parallel-12      21.8kB ± 0%    21.8kB ± 0%   -0.00%  (p=0.001 n=10+9)
Array/1×8B/sequential-12            40.0B ± 0%     40.0B ± 0%     ~     (all equal)
Array/1×8B/parallel-12              40.0B ± 0%     40.0B ± 0%     ~     (all equal)
Array/12×8B/sequential-12            384B ± 0%      384B ± 0%     ~     (all equal)
Array/12×8B/parallel-12              384B ± 0%      384B ± 0%     ~     (all equal)
Array/144×8B/sequential-12         4.61kB ± 0%    4.61kB ± 0%     ~     (p=1.000 n=10+10)
Array/144×8B/parallel-12           4.61kB ± 0%    4.61kB ± 0%     ~     (p=0.211 n=10+10)

name                             TCP allocs/op  Unix allocs/op delta
SimpleString/sequential-12           0.00           0.00          ~     (all equal)
SimpleString/parallel-12             0.00           0.00          ~     (all equal)
Integer/sequential-12                0.00           0.00          ~     (all equal)
Integer/parallel-12                  0.00           0.00          ~     (all equal)
BulkString/1B/sequential-12          1.00 ± 0%      1.00 ± 0%     ~     (all equal)
BulkString/1B/parallel-12            1.00 ± 0%      1.00 ± 0%     ~     (all equal)
BulkString/144B/sequential-12        1.00 ± 0%      1.00 ± 0%     ~     (all equal)
BulkString/144B/parallel-12          1.00 ± 0%      1.00 ± 0%     ~     (all equal)
BulkString/20736B/sequential-12      1.00 ± 0%      1.00 ± 0%     ~     (all equal)
BulkString/20736B/parallel-12        1.00 ± 0%      1.00 ± 0%     ~     (all equal)
Array/1×8B/sequential-12             2.00 ± 0%      2.00 ± 0%     ~     (all equal)
Array/1×8B/parallel-12               2.00 ± 0%      2.00 ± 0%     ~     (all equal)
Array/12×8B/sequential-12            13.0 ± 0%      13.0 ± 0%     ~     (all equal)
Array/12×8B/parallel-12              13.0 ± 0%      13.0 ± 0%     ~     (all equal)
Array/144×8B/sequential-12            145 ± 0%       145 ± 0%     ~     (all equal)
Array/144×8B/parallel-12              145 ± 0%       145 ± 0%     ~     (all equal)
```
