package redis_test

import (
	"io"
	"log"
	"time"

	"github.com/pascaldekloe/redis/v2"
)

func ExampleClient_SETWithOptions() {
	// connection setup
	var Redis = redis.NewClient[string, string]("rds1.example.com", time.Second/2, 0)
	defer Redis.Close()

	// execute command
	ok, err := Redis.SETWithOptions("k", "v", redis.SETOptions{
		Flags:  redis.NX | redis.EX,
		Expire: time.Minute,
	})
	if err != nil {
		log.Print("command error: ", err)
		return
	}

	// evaluate NX condition
	if ok {
		log.Print("new string expires in a minute")
	} else {
		log.Print("left existing string as is")
	}
}

func ExampleListener() {
	// connection setup
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
	defer RedisListener.Close()

	// listen quickly
	RedisListener.SUBSCRIBE("demo_channel")
	time.Sleep(time.Millisecond)
}
