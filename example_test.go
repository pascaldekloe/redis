package redis_test

import (
	"log"
	"time"

	"github.com/pascaldekloe/redis"
)

func ExampleClient_SETStringWithOptions() {
	// connection setup
	var Redis = redis.NewClient("rds1.example.com", 5*time.Millisecond, time.Second)
	// terminate after example
	defer Redis.Close()

	// execute command
	ok, err := Redis.SETStringWithOptions("k", "v", redis.SETOptions{
		Flags:  redis.NX | redis.EX,
		Expire: time.Minute,
	})
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
