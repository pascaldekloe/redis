package redis_test

import (
	"log"
	"time"

	"github.com/pascaldekloe/redis"
)

// SET With Options
func ExampleClient_SETArgs() {
	// connection setup
	var Redis = redis.NewClient("rds1.example.com", 5*time.Millisecond, time.Second)
	// terminate after example
	defer Redis.Close()

	// execute command
	ok, err := Redis.SETArgs("hello", nil, redis.EX, "60", redis.NX)
	if err != nil {
		log.Print("error: ", err)
		return
	}

	// evaluate NX condition
	if ok {
		log.Print("new string expires in a minute")
	} else {
		log.Print("left existing as is")
	}
}
