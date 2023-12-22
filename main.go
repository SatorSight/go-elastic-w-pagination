package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"go-elastic/es"
	"go-elastic/logger"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	esClient := prepareESClient()
	ctx := prepareContext()

	res := loadSimplePagination(esClient, ctx, "")
	pp(res)
}

func create100kUsers(client *es.Client, ctx context.Context, index string) {
	client.Create100kUsers(ctx, index)
}

func createIndex(client *es.Client, ctx context.Context, index string) {
	err := client.CreateIndex(ctx, index, "mapping.json")
	if err != nil {
		log.Fatalf("failed to create index: %v", err)
	}
}

func prepareContext() context.Context {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	return ctx
}

func loadSimplePagination(client *es.Client, ctx context.Context, index string) []es.User {
	from := 0
	size := 10

	var result []es.User

	for i := from; i < 100; i += size {
		res, err2 := client.Load(ctx, index, i, size, 0)
		if err2 != nil {
			log.Fatalf("failed to fetch results: %v", err2)
		}

		users := res.Users
		result = append(result, users...)
	}

	return result
}

func simpleLoad(client *es.Client, ctx context.Context, index string) es.SearchResult {
	res, err := client.Load(ctx, index, 0, 10, 0)
	if err != nil {
		log.Fatalf("failed to fetch results: %v", err)
	}

	return res
}

func pp(data any) {
	b, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(b))
}

func cursorPaginate(client *es.Client, ctx context.Context, index string) []es.User {
	from := 0
	size := 10
	//var cursor float64 = 0
	var res []es.User

	initRes, _ := client.Load(ctx, index, from, size, 0)
	ls := initRes.LastSort
	res = append(res, initRes.Users...)

	for i := 1; i < 10; i++ {
		res2, err2 := client.Load(ctx, index, 0, size, ls)
		if err2 != nil {
			log.Fatalf("failed to fetch results: %v", err2)
		}

		ls = res2.LastSort
		log.Printf("current cursor: %v\n", ls)
		users := res2.Users
		res = append(res, users...)
	}
	return res
}

func prepareESClient() *es.Client {
	esHost := "http://localhost:4566/es/us-east-1/my-data"
	esUsername := ""
	esPassword := ""
	esIndex := "my-simple-index"

	lg, err := logger.New(logger.Config{
		Level:    "debug",
		Encoding: "json",
		Color:    true,
		Outputs:  []string{"stdout"},
		Tags:     []string{},
	}, "Development", "my-app", "1")

	if err != nil {
		panic("logger init error")
	}

	esLogger := logger.NewLoggerForEs(lg)

	var t http.RoundTripper = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment, // not so necessary right now, for future.
		ForceAttemptHTTP2:     false,                     // ?
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		// DisableCompression - this is important, in dev env we start AWS Localstack without https,
		// so we need to disable any compressions,
		DisableCompression: true,
	}

	esCfg := elasticsearch.Config{
		Addresses:             []string{esHost}, // @see envs in config.conf
		Username:              esUsername,
		Password:              esPassword,
		CloudID:               "",
		APIKey:                "",
		Header:                nil,
		CACert:                nil,
		RetryOnStatus:         nil, // List of status codes for retry. Default: 502, 503, 504.
		DisableRetry:          false,
		EnableRetryOnTimeout:  true,
		MaxRetries:            3,
		DiscoverNodesOnStart:  false,
		DiscoverNodesInterval: 0,
		EnableMetrics:         true,
		EnableDebugLogger:     true,
		RetryBackoff:          nil,
		Transport:             t,
		Logger:                esLogger,
		Selector:              nil,
		ConnectionPoolFunc:    nil,
	}

	maxTimeoutStr := "30s"
	maxTimeout, _ := time.ParseDuration(maxTimeoutStr)

	customStorageCfg := es.Config{
		DefaultIndex:          esIndex,
		MaxSearchQueryTimeout: maxTimeout,
		PathToMappingSchema:   "",
		IsTrackTotalHits:      true, // always needed for cnt operations.
	}

	esClient, err := es.New(lg, esCfg, customStorageCfg)
	if err != nil {
		log.Fatalln("failed to init esClient")
	}
	return esClient
}
