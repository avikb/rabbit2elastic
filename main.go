package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/streadway/amqp"
)

func main() {
	rdsn := flag.String("rabbit", "amqp://guest:guest@localhost:5672/", "rabbitmq dsn")
	rqname := flag.String("queue", "queue", "rabbitmq queue name (in dsn's vhost)")
	ehost := flag.String("elastic", "http://127.0.0.1:9200", "elastic host")
	eindex := flag.String("index", "index", "elastic index name witch we will fill")
	flag.Parse()

	conn, err := amqp.Dial(*rdsn)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln(err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		*rqname,     // queue name
		"go-shovel", // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		log.Fatalln(err)
	}

	es, err := NewElastic([]string{*ehost}, 60)
	if err != nil {
		log.Fatalln(err)
	}
	indexer, err := es.NewBulkIndexer(*eindex)
	if err != nil {
		panic(err)
	}
	defer indexer.Close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sig:
			return
		case msg := <-msgs:
			indexer.Index(msg.Body, func() {
				if err := msg.Ack(false); err != nil {
					log.Fatalln(err)
				}
			})
		}
	}
}
