package main

import (
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/kingsoft-wps/go/log"
	"github.com/kingsoft-wps/go/nosql"
)

var (
	host     = flag.String("h", "localhost", "redis host")
	port     = flag.Int("p", 6379, "redis port")
	db       = flag.Int("n", 0, "redis db")
	conn     = flag.Int("c", 300, "concurrent num")
	loglevel = flag.String("l", "debug", "log level.  trace, debug, info, warn, error, fatal.")
	sleep    = flag.Int("s", 20, "time interval to sleep, in second")
)

var LogLevel = map[string]int{
	"trace": log.LevelTrace,
	"debug": log.LevelDebug,
	"info":  log.LevelInfo,
	"warn":  log.LevelWarn,
	"error": log.LevelError,
	"fatal": log.LevelFatal,
}

func doTask(store nosql.Store) error {
	key := "test"
	value := "hello"
	err := store.Set(key, value)
	if err != nil {
		log.Error("store Set error.")
		return err
	}

	log.Debug("set ok")

	v, err := store.String(store.Get(key))
	if err != nil {
		log.Error("store get error.")
		return err
	}

	if v != value {
		log.Error("get: %s, expected: %s", v, value)
		return errors.New("not equal")
	}

	log.Debug("get ok")

	return nil
}

func main() {
	flag.Parse()
	fmt.Println("connecting to:", *host, *port, *db)

	log.SetLevel(LogLevel[*loglevel])

	store, err := nosql.NewRedisStore(*host, *port, *db)
	store.SetMaxIdle(conn+10)
	store.SetMaxActive(conn+10)
	if err != nil {
		panic("connect redis error")
	}

	for {
		var wg sync.WaitGroup
		for i := 0; i < *conn; i++ {
			wg.Add(1)
			go func(store nosql.Store) {
				doTask(store)
				wg.Done()
			}(store)
		}
		wg.Wait()

		log.Info("sleeping ...")
		time.Sleep(time.Duration(*sleep) * time.Second)
	}

}
