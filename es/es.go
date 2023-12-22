package es

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	jsoniter "github.com/json-iterator/go"
	"go-elastic/logger"
	"go.uber.org/zap"
	"log"
	"os"
	"strings"
	"time"
)

type (
	Config struct {
		Hosts                 []string
		DefaultIndex          string
		DisableCompression    bool
		MaxSearchQueryTimeout time.Duration
		PathToMappingSchema   string
		IsTrackTotalHits      bool
	}

	Client struct {
		log                   *logger.Logger
		esCfg                 elasticsearch.Config
		esClient              *elasticsearch.Client
		defaultIndex          string
		maxSearchQueryTimeout time.Duration
		isTrackTotalHits      bool
	}

	User struct {
		ID        int
		CreatedAt time.Time
		Username  string
	}

	SearchResult struct {
		Users      []User
		TotalCount int64
		LastSort   float64
	}
)

func New(log *logger.Logger, esCfg elasticsearch.Config, customCfg Config) (*Client, error) {
	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		log.Info("Could not create new ElasticSearch client due error")
		return nil, err
	}

	log.Info("Successfully create new ElasticSearch client.")
	log.Sugar().Info("Successfully connected to AWS OpenSearch (Elasticsearch) cluster. Hosts: %v", customCfg.Hosts)
	log.Info("Try to create defaultIndex (if not exist)")

	c := &Client{
		log:                   log,
		esCfg:                 esCfg,
		esClient:              es,
		defaultIndex:          customCfg.DefaultIndex,
		maxSearchQueryTimeout: customCfg.MaxSearchQueryTimeout,
		isTrackTotalHits:      true,
	}

	return c, nil
}

func (c *Client) CreateIndex(ctx context.Context, index string, mapping string) error {
	var file []byte
	file, err := os.ReadFile(mapping)
	if err != nil || file == nil {
		c.log.Fatal("Could not read file with mapping defaultIndex schema",
			zap.String("path_to_mapping_schema", mapping),
			zap.Error(err))
	}
	indexMappingSchema := string(file)

	req := esapi.IndicesCreateRequest{
		Index: index,
		Body:  strings.NewReader(indexMappingSchema),
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return fmt.Errorf("err creating defaultIndex: %v", err)
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			c.log.Error("res.Body.Close() problem", zap.Error(err))
		}
	}()

	if res.IsError() {
		return fmt.Errorf("err creating defaultIndex. res: %s", res.String())
	}

	return nil
}

func (c *Client) Load(
	ctx context.Context,
	index string,
	from int,
	size int,
	cursor float64,
) (SearchResult, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}

	sortQuery := []map[string]map[string]interface{}{
		{"ID": {"order": "asc"}},
	}

	query["sort"] = sortQuery

	if cursor != 0 {
		query["search_after"] = []float64{cursor}
	}

	var buf bytes.Buffer
	if err := jsoniter.NewEncoder(&buf).Encode(query); err != nil {
		return SearchResult{}, fmt.Errorf("es.client.Load(): error encoding query: %v. err: %w", query, err)
	}

	if index == "" {
		index = c.defaultIndex
	}

	var res *esapi.Response
	var err error
	if cursor != 0 {
		res, err = c.esClient.Search(
			c.esClient.Search.WithContext(ctx),
			c.esClient.Search.WithTimeout(c.maxSearchQueryTimeout),
			c.esClient.Search.WithIndex(index),
			c.esClient.Search.WithBody(&buf),
			c.esClient.Search.WithSize(size),
			c.esClient.Search.WithTrackTotalHits(c.isTrackTotalHits),
			c.esClient.Search.WithPretty(), // todo probably remove, if will performance degradation.
		)
		if err != nil {
			return SearchResult{}, fmt.Errorf("es.client.Load(): search response err: %w", err)
		}
	} else {
		res, err = c.esClient.Search(
			c.esClient.Search.WithContext(ctx),
			c.esClient.Search.WithTimeout(c.maxSearchQueryTimeout),
			c.esClient.Search.WithIndex(index),
			c.esClient.Search.WithBody(&buf),
			c.esClient.Search.WithFrom(from),
			c.esClient.Search.WithSize(size),
			c.esClient.Search.WithTrackTotalHits(c.isTrackTotalHits),
			c.esClient.Search.WithPretty(), // todo probably remove, if will performance degradation.
		)
		if err != nil {
			return SearchResult{}, fmt.Errorf("es.client.Load(): search response err: %w", err)
		}
	}

	defer func() {
		if err = res.Body.Close(); err != nil {
			c.log.Error("es.Client.Load() res.Body.Close()",
				zap.Any("query", query),
				zap.Any("buf", buf),
				zap.String("method", "es.Client.Load"),
				zap.Error(err),
			)
		}
	}()

	if res.IsError() {
		c.log.Error("es.Clint.Load() failure response",
			zap.Any("query", query),
			zap.String("res.Status", res.Status()),
		)

		var e map[string]interface{}
		if err = jsoniter.NewDecoder(res.Body).Decode(&e); err != nil {
			return SearchResult{}, fmt.Errorf("es.client.Load(): error parsing the failtuire response body. err: %w", err)
		}

		return SearchResult{}, fmt.Errorf("es.client.Load(): status: parsing the response body. Err desc: %v", e)
	}

	r := map[string]any{}
	if err = jsoniter.NewDecoder(res.Body).Decode(&r); err != nil {
		var res2 []byte
		_, _ = res.Body.Read(res2)
		c.log.Info(string(res2))
		return SearchResult{},
			fmt.Errorf("es.client.Load(): error parsing the response body. for good response. err: %w", err)
	}

	result :=
		func() SearchResult {
			totalCnt := int64(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
			if totalCnt == 0 {
				return SearchResult{}
			}

			cntFind := len(r["hits"].(map[string]interface{})["hits"].([]interface{}))
			docs := make([]User, 0, cntFind)
			var lastSort float64

			for _, v := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
				lastSort = v.(map[string]interface{})["sort"].([]interface{})[0].(float64)
				doc := User{}

				// Why we have double convert from map[string]interface{} -> string -> struct.
				// Answer: We have problem (in Go lang) to convert map[string]any to struct directly.
				// More you can reade here in SO:  https://stackoverflow.com/questions/26744873/converting-map-to-struct.
				jsonBody, err := jsoniter.Marshal(v.(map[string]interface{})["_source"].(map[string]interface{}))
				if err != nil {
					c.log.Error("es.client.Load() err from jsoniter.Marshal",
						zap.Any("v", v),
						zap.Any("r['hits']", r["hits"]),
						zap.Error(err),
					)

					return SearchResult{Users: docs, TotalCount: totalCnt}
				}

				if err = jsoniter.Unmarshal(jsonBody, &doc); err != nil {
					c.log.Error("es.client.Load() err from jsoniter.Unmarshal",
						zap.Any("v", v),
						zap.Any("r['hits']", r["hits"]),
						zap.Error(err),
					)

					return SearchResult{Users: docs, TotalCount: totalCnt}
				}

				docs = append(docs, doc)
			}
			return SearchResult{Users: docs, TotalCount: totalCnt, LastSort: lastSort}
		}()

	return result, nil
}

func (c *Client) Store(ctx context.Context, index string, doc User) error {
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("es.Client.Store() recover from panic",
				zap.Any("r", r),
			)
		}
	}()

	if index == "" {
		index = c.defaultIndex
	}

	docB, err := jsoniter.Marshal(doc)
	if err != nil {
		return fmt.Errorf("error marshaling the JSON document: %+v, err: %w", doc, err)
	}

	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: "", // auto-generate by elastic search. // todo probably we need to use own custom doc.ID.
		//Body:       bytes.NewReader(docB),
		Body:    strings.NewReader(string(docB)),
		Refresh: "true",
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return fmt.Errorf("error sending the Elasticsearch request: %s", err)
	}
	defer func() {
		err = res.Body.Close()
		if err != nil {
			c.log.Error("res.Body.Close() problem", zap.Error(err))
		}
	}()

	if res.IsError() {
		return fmt.Errorf("error inserting document: %s", res.Status())
	}

	c.log.Debug("Successfully send doc to elastic",
		zap.Any("doc", doc),
		zap.Int("status_code", res.StatusCode),
	)

	return nil
}

func (c *Client) Create100kUsers(ctx context.Context, index string) {
	user := User{
		ID:        0,
		CreatedAt: time.Now(),
		Username:  "init",
	}

	for i := 0; i < 100000; i++ {
		user.Username = fmt.Sprintf("%v %v", "user", i)
		user.ID = i
		err := c.Store(ctx, index, user)
		if err != nil {
			log.Fatal("failed to store", zap.Error(err))
		}
	}

}
