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
% setenv TEST_REDIS_ADDR localhost
% go test -bench .
goos: darwin
goarch: amd64
pkg: github.com/pascaldekloe/redis
BenchmarkSimpleString/sequential-12         	   20311	     58792 ns/op
BenchmarkSimpleString/parallel-12           	  123637	      9545 ns/op
BenchmarkInteger/sequential-12              	   20932	     57337 ns/op
BenchmarkInteger/parallel-12                	  125294	      9412 ns/op
BenchmarkBulkString/1B/sequential-12        	   20614	     58095 ns/op
BenchmarkBulkString/1B/parallel-12          	  123476	      9506 ns/op
BenchmarkBulkString/144B/sequential-12      	   20310	     59293 ns/op
BenchmarkBulkString/144B/parallel-12        	  122462	      9681 ns/op
BenchmarkBulkString/20736B/sequential-12    	   16248	     74041 ns/op
BenchmarkBulkString/20736B/parallel-12      	   37521	     32471 ns/op
BenchmarkArray/1×8B/sequential-12           	   19658	     60909 ns/op
BenchmarkArray/1×8B/parallel-12             	  118478	      9983 ns/op
BenchmarkArray/12×8B/sequential-12          	   18774	     63937 ns/op
BenchmarkArray/12×8B/parallel-12            	  109963	     10732 ns/op
BenchmarkArray/144×8B/sequential-12         	   13960	     85876 ns/op
BenchmarkArray/144×8B/parallel-12           	   48681	     24483 ns/op
PASS
ok  	github.com/pascaldekloe/redis	25.554s
```

#### Unix Domain Socket

```
% setenv TEST_REDIS_ADDR /var/run/redis.sock
% go test -bench .
goos: darwin
goarch: amd64
pkg: github.com/pascaldekloe/redis
BenchmarkSimpleString/sequential-12         	   38068	     31202 ns/op
BenchmarkSimpleString/parallel-12           	  211083	      5486 ns/op
BenchmarkInteger/sequential-12              	   39561	     30262 ns/op
BenchmarkInteger/parallel-12                	  217764	      5373 ns/op
BenchmarkBulkString/1B/sequential-12        	   38583	     30995 ns/op
BenchmarkBulkString/1B/parallel-12          	  218179	      5374 ns/op
BenchmarkBulkString/144B/sequential-12      	   37426	     32065 ns/op
BenchmarkBulkString/144B/parallel-12        	  210168	      5498 ns/op
BenchmarkBulkString/20736B/sequential-12    	   15894	     75305 ns/op
BenchmarkBulkString/20736B/parallel-12      	   24069	     49771 ns/op
BenchmarkArray/1×8B/sequential-12           	   35386	     33714 ns/op
BenchmarkArray/1×8B/parallel-12             	  194251	      6049 ns/op
BenchmarkArray/12×8B/sequential-12          	   32833	     36484 ns/op
BenchmarkArray/12×8B/parallel-12            	  167574	      6911 ns/op
BenchmarkArray/144×8B/sequential-12         	   20746	     57730 ns/op
BenchmarkArray/144×8B/parallel-12           	   53972	     21806 ns/op
PASS
ok  	github.com/pascaldekloe/redis	23.386s
```
