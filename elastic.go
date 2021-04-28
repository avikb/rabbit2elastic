package main

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esutil"
)

type Elastic struct {
	es *elasticsearch.Client
}

func NewElastic(hosts []string, maxRetries int) (*Elastic, error) {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:            hosts,
		EnableRetryOnTimeout: true,
		RetryOnStatus:        []int{429, 502, 503, 504},
		MaxRetries:           maxRetries,
		RetryBackoff: func(i int) time.Duration {
			if i == maxRetries {
				log.Fatalln("max retries have been reached")
			}
			return time.Second
		},
	})
	if err != nil {
		return nil, err
	}
	return &Elastic{es: es}, nil
}

type BulkIndexer struct {
	es    *elasticsearch.Client
	bulk  esutil.BulkIndexer
	index string
}

func (e *Elastic) NewBulkIndexer(index string) (*BulkIndexer, error) {
	bulk, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:        index,
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
	return &BulkIndexer{es: e.es, bulk: bulk, index: index}, nil
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

func (b *BulkIndexer) Index(itm interface{}, onSuccess func()) error {
	t := task{r: esutil.NewJSONReader(itm), sf: onSuccess}
	return b.bulk.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			Action: "index",
			Body:   &t,
			OnSuccess: func(c context.Context, bii esutil.BulkIndexerItem, biri esutil.BulkIndexerResponseItem) {
				t := bii.Body.(*task)
				if t.sf != nil {
					t.sf()
				}
			},
		},
	)
}
