package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/pascaldekloe/redis"
)

var (
	addrFlag = flag.String("addr", "localhost:6379", "Redis node `address`.")
	authFlag = flag.Bool("auth", false, "Reads a password from the standard input.")

	rawFlag       = flag.Bool("raw", false, "Output values as is, instead of quoted strings.")
	delimitFlag   = flag.String("delimit", "\n", "The output `separator` between values.")
	terminateFlag = flag.String("terminate", "\n", "The output `suffix` on the last value.")
	nullFlag      = flag.String("null", "<null>", "The output `value` for key absence.")
)

// Redis manages the connection.
var Redis *redis.Client

func main() {
	flag.Parse()
	keys := flag.Args()
	if len(keys) == 0 {
		os.Stderr.WriteString(`NAME
	reget — resolve Redis content

SYNOPSIS
	reget [ options ] [ key ... ]

DESCRIPTION
	For each operand, reget prints the associated value according to
	the node.

	The following options are available:

`)
		flag.PrintDefaults()
		os.Exit(1)
	}

	Redis = redis.NewClient(*addrFlag, 0, 0)
	defer Redis.Close()
	if *authFlag {
		password, _ := ioutil.ReadAll(os.Stdin)
		if err := Redis.AUTH(password); err != nil {
			fmt.Fprintln(os.Stderr, "reget: AUTH with", err)
			os.Exit(4)
		}
	}

	print(keys)
}

func print(keys []string) {
	values, err := Redis.MGET(keys...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "reget: MGET with", err)
		os.Exit(255)
	}

	w := os.Stdout
	for i, v := range values {
		switch {
		case v == nil:
			w.WriteString(*nullFlag)
		case *rawFlag:
			w.Write(v)
		default:
			w.WriteString(strconv.QuoteToGraphic(string(v)))
		}

		if i < len(values)-1 {
			w.WriteString(*delimitFlag)
		} else {
			w.WriteString(*terminateFlag)
		}
	}
}
