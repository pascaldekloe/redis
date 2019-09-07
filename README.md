[![API Documentation](https://godoc.org/github.com/pascaldekloe/redis?status.svg)](https://godoc.org/github.com/pascaldekloe/redis)
[![Build Status](https://circleci.com/gh/pascaldekloe/redis.svg?style=svg)](https://circleci.com/gh/pascaldekloe/redis)

## About

… a redis client for the Go programming language.

This is free and unencumbered software released into the
[public domain](https://creativecommons.org/publicdomain/zero/1.0).


## Performance

The following results were measured on a E5-1650 v2 (from the year 2013).

#### TCP

```
BenchmarkSimpleString/sequential-12         	   20076	     59504 ns/op
BenchmarkSimpleString/parallel-12           	  121870	      9769 ns/op
BenchmarkInteger/sequential-12              	   20616	     58077 ns/op
BenchmarkInteger/parallel-12                	  123735	      9620 ns/op
BenchmarkBulkString/1B/sequential-12        	   20432	     58637 ns/op
BenchmarkBulkString/1B/parallel-12          	  123364	      9677 ns/op
BenchmarkBulkString/144B/sequential-12      	   20186	     59357 ns/op
BenchmarkBulkString/144B/parallel-12        	  120703	      9843 ns/op
BenchmarkBulkString/20736B/sequential-12    	   15932	     75499 ns/op
BenchmarkBulkString/20736B/parallel-12      	   36267	     32821 ns/op
BenchmarkArray/1×8B/sequential-12           	   19417	     61326 ns/op
BenchmarkArray/1×8B/parallel-12             	  116478	     10162 ns/op
BenchmarkArray/12×8B/sequential-12          	   18674	     64727 ns/op
BenchmarkArray/12×8B/parallel-12            	  109290	     10917 ns/op
BenchmarkArray/144×8B/sequential-12         	   13855	     86633 ns/op
BenchmarkArray/144×8B/parallel-12           	   48794	     25336 ns/op
```

#### Unix Domain Socket

```
BenchmarkSimpleString/sequential-12         	   36267	     32912 ns/op
BenchmarkSimpleString/parallel-12           	  204867	      5699 ns/op
BenchmarkInteger/sequential-12              	   37681	     31813 ns/op
BenchmarkInteger/parallel-12                	  210139	      5511 ns/op
BenchmarkBulkString/1B/sequential-12        	   36902	     32574 ns/op
BenchmarkBulkString/1B/parallel-12          	  212095	      5561 ns/op
BenchmarkBulkString/144B/sequential-12      	   35874	     33455 ns/op
BenchmarkBulkString/144B/parallel-12        	  208605	      5678 ns/op
BenchmarkBulkString/20736B/sequential-12    	   15589	     77267 ns/op
BenchmarkBulkString/20736B/parallel-12      	   22905	     52213 ns/op
BenchmarkArray/1×8B/sequential-12           	   34530	     34630 ns/op
BenchmarkArray/1×8B/parallel-12             	  190671	      6102 ns/op
BenchmarkArray/12×8B/sequential-12          	   32229	     37111 ns/op
BenchmarkArray/12×8B/parallel-12            	  164794	      7125 ns/op
BenchmarkArray/144×8B/sequential-12         	   20258	     59222 ns/op
BenchmarkArray/144×8B/parallel-12           	   52867	     22336 ns/op
```
