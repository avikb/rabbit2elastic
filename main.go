package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Rabbit struct {
		Dsn      string `yaml:"dsn"`
		Queue    string `yaml:"queue"`
		Prefetch int    `yaml:"prefetch"`
		Name     string `yaml:"name"`
	} `yaml:"rabbit"`
	Elastic struct {
		Hosts             []string      `yaml:"hosts"`
		MaxRetries        int           `yaml:"maxRetries"`
		PauseBetweenRetry time.Duration `yaml:"pauseBetweenRetry"`
	} `yaml:"elastic"`
	Route        map[string]string `yaml:"route"`
	DefaultRoute string            `yaml:"route-default"`
}

func main() {
	cfgname := flag.String("config", "config.yaml", "config filename")
	flag.Parse()

	cfgdata, err := ioutil.ReadFile(*cfgname)
	fatalOnErr(err)

	var cfg Config
	fatalOnErr(yaml.Unmarshal(cfgdata, &cfg))

	conn, err := amqp.Dial(cfg.Rabbit.Dsn)
	fatalOnErr(err)
	defer conn.Close()

	ch, err := conn.Channel()
	fatalOnErr(err)
	defer ch.Close()

	fatalOnErr(ch.Qos(cfg.Rabbit.Prefetch, 0, false))

	msgs, err := ch.Consume(cfg.Rabbit.Queue, cfg.Rabbit.Name, false, false, false, false, nil)
	fatalOnErr(err)

	es, err := NewElastic(cfg.Elastic.Hosts, cfg.Elastic.MaxRetries, cfg.Elastic.PauseBetweenRetry)
	fatalOnErr(err)

	indexer, err := es.NewBulkIndexer()
	fatalOnErr(err)
	defer indexer.Close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sig:
			return
		case msg, ok := <-msgs:
			if !ok {
				log.Fatalln("rabbitmq exception")
			}
			t, ok := msg.Headers["type"].(string)
			if !ok {
				log.Fatalln("bad 'type' header: ", msg.Headers["type"])
			}
			index := cfg.Route[t]
			if index == "" {
				log.Fatalf("index for type '%s' not defined", t)
			}
			indexer.Index(index, json.RawMessage(msg.Body), func() {
				fatalOnErr(msg.Ack(false))
			})
		}
	}
}

func fatalOnErr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
