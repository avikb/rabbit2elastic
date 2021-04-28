package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esutil"
)

type Elastic struct {
	es *elasticsearch.Client
}

func NewElastic(hosts []string, maxRetries int, pauseBetweenRetry time.Duration) (*Elastic, error) {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:            hosts,
		EnableRetryOnTimeout: true,
		RetryOnStatus:        []int{429, 502, 503, 504},
		MaxRetries:           maxRetries,
		RetryBackoff: func(i int) time.Duration {
			if i == maxRetries {
				log.Fatalln("max retries have been reached")
			}
			fmt.Println("retry: ", i)
			return pauseBetweenRetry
		},
	})
	if err != nil {
		return nil, err
	}
	return &Elastic{es: es}, nil
}

type BulkIndexer struct {
	es   *elasticsearch.Client
	bulk esutil.BulkIndexer
}

func (e *Elastic) NewBulkIndexer() (*BulkIndexer, error) {
	bulk, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:       e.es,
		DocumentType: "_doc",
		// NumWorkers:    2,               // default: NumCPUs
		FlushInterval: 2 * time.Second, // default: 30 secs
		// FlushBytes:    1024 * 1024,     // defaul: 5Mb
		OnError: func(ctx context.Context, err error) {
			log.Printf("elastic: %s", err)
		},
	})
	if err != nil {
		return nil, err
	}
	return &BulkIndexer{es: e.es, bulk: bulk}, nil
}

func (b *BulkIndexer) Close() error {
	return b.bulk.Close(context.Background())
}

func (b *BulkIndexer) BulkStats() esutil.BulkIndexerStats {
	return b.bulk.Stats()
}

type task struct {
	r  io.Reader
	sf func()
}

func (t *task) Read(p []byte) (n int, err error) {
	return t.r.Read(p)
}

func (b *BulkIndexer) Index(index string, itm interface{}, onSuccess func()) error {
	t := task{r: esutil.NewJSONReader(itm), sf: onSuccess}
	return b.bulk.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			Index:  index,
			Action: "index",
			Body:   &t,
			OnSuccess: func(c context.Context, bii esutil.BulkIndexerItem, biri esutil.BulkIndexerResponseItem) {
				t := bii.Body.(*task)
				if t.sf != nil {
					t.sf()
				}
			},
			OnFailure: func(c context.Context, bii esutil.BulkIndexerItem, biri esutil.BulkIndexerResponseItem, e error) {
				log.Fatalln("elastic: ", biri.Error)
			},
		},
	)
}
