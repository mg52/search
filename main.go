package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mg52/search/search"
)

// AddToIndexRequest is the payload for adding documents to an existing index.
type AddToIndexRequest struct {
	IndexName string `json:"indexName"`
}

// AddToIndexResponse is returned on successful addition.
type AddToIndexResponse struct {
	IndexName  string `json:"indexName"`
	AddedCount int    `json:"addedCount"`
	Duration   string `json:"duration"`
	DurationMs int64  `json:"durationMs"`
}

type CreateIndexRequest struct {
	IndexName   string   `json:"indexName"`
	IndexFields []string `json:"indexFields"`
	Filters     []string `json:"filters"`
	PageCount   int      `json:"pageCount"`
	Workers     int      `json:"workers"`
}

type CreateIndexResponse struct {
	IndexName    string `json:"indexName"`
	IndexedCount int    `json:"indexedCount"`
	Duration     string `json:"duration"`
}

type HTTP struct {
	mu          sync.RWMutex
	controllers map[string]*search.SearchEngineController
}

// NewHTTP initializes the handler with an empty map.
func NewHTTP() *HTTP {
	return &HTTP{
		controllers: make(map[string]*search.SearchEngineController),
	}
}

func ErrWriter(w http.ResponseWriter, err error) {
	var jsonBytes []byte
	jsonBytes, jsonErr := json.Marshal(map[string]interface{}{
		"err": fmt.Sprintf("%v", err),
	})
	if jsonErr != nil {
		jsonBytes = []byte(fmt.Sprintf("err: %v", err))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	w.Write(jsonBytes)
}

func (ht *HTTP) Search(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		ErrWriter(w, errors.New("unsupported method"))
		return
	}

	// 1) Which index?
	indexName := r.URL.Query().Get("index")
	if indexName == "" {
		ErrWriter(w, errors.New("`index` query parameter is required"))
		return
	}

	// 2) Lookup controller
	ht.mu.RLock()
	sec, ok := ht.controllers[indexName]
	ht.mu.RUnlock()
	if !ok {
		ErrWriter(w, fmt.Errorf("index %q not found", indexName))
		return
	}

	startTime := time.Now()

	// 3) Parse "q" and "page"
	query := r.URL.Query().Get("q")
	pageStr := r.URL.Query().Get("page")
	pageInt, err := strconv.Atoi(pageStr)
	if err != nil {
		ErrWriter(w, fmt.Errorf("invalid page number: %w", err))
		return
	}

	// 4) Parse filters (filter=year:2017,year:2018,...)
	filters := make(map[string][]interface{})
	filterStr := r.URL.Query().Get("filter")
	if filterStr != "" {
		for _, item := range strings.Split(filterStr, ",") {
			parts := strings.SplitN(item, ":", 2)
			if len(parts) != 2 {
				fmt.Printf("Skipping invalid filter: %s\n", item)
				continue
			}
			key, val := parts[0], parts[1]
			filters[key] = append(filters[key], val)
		}
	}

	// 5) Perform search
	result := sec.Search(query, pageInt, filters)
	duration := time.Since(startTime)
	fmt.Printf("Search [%s] took %s for query %q\n", indexName, duration, query)

	// 6) Write response
	resp := map[string]interface{}{
		"status":     "success",
		"statusCode": 200,
		"index":      indexName,
		"query":      query,
		"response":   result,
		"duration":   duration.String(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		// this is unlikely, but handle it
		ErrWriter(w, err)
	}
}

func (ht *HTTP) CreateIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		ErrWriter(w, fmt.Errorf("method not allowed"))
		return
	}

	var req CreateIndexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ErrWriter(w, fmt.Errorf("invalid JSON payload: %w", err))
		return
	}

	if req.IndexName == "" {
		ErrWriter(w, errors.New("`indexName` is required"))
		return
	}
	ht.mu.RLock()
	_, exists := ht.controllers[req.IndexName]
	ht.mu.RUnlock()
	if exists {
		ErrWriter(w, fmt.Errorf("index %q already exists", req.IndexName))
		return
	}

	// defaults
	if req.PageCount <= 0 {
		req.PageCount = 10
	}
	if req.Workers <= 0 {
		req.Workers = 8
	}

	// build filter map
	filterMap := make(map[string]bool, len(req.Filters))
	for _, f := range req.Filters {
		filterMap[f] = true
	}

	// spin up an empty controller
	start := time.Now()
	sec := search.NewSearchEngineController(
		req.IndexFields,
		filterMap,
		req.PageCount,
		req.Workers,
	)
	// no docs to index yet
	elapsed := time.Since(start)

	ht.mu.Lock()
	ht.controllers[req.IndexName] = sec
	ht.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(CreateIndexResponse{
		IndexName:    req.IndexName,
		IndexedCount: 0,
		Duration:     elapsed.String(),
	})
}

// AddToIndex appends the documents from the given JSON file into an existing index.
func (ht *HTTP) AddToIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		ErrWriter(w, fmt.Errorf("method not allowed"))
		return
	}

	// indexName now in the query
	indexName := r.URL.Query().Get("indexName")
	if indexName == "" {
		ErrWriter(w, errors.New("`indexName` query parameter is required"))
		return
	}

	ht.mu.RLock()
	sec, ok := ht.controllers[indexName]
	ht.mu.RUnlock()
	if !ok {
		ErrWriter(w, fmt.Errorf("index %q not found", indexName))
		return
	}

	// require a multipart‐uploaded JSON file
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		ErrWriter(w, fmt.Errorf("invalid multipart form: %w", err))
		return
	}
	file, _, err := r.FormFile("file")
	if err != nil {
		ErrWriter(w, fmt.Errorf("file upload required: %w", err))
		return
	}
	defer file.Close()

	raw, err := io.ReadAll(file)
	if err != nil {
		ErrWriter(w, fmt.Errorf("unable to read uploaded file: %w", err))
		return
	}

	var docs []map[string]interface{}
	if err := json.Unmarshal(raw, &docs); err != nil {
		ErrWriter(w, fmt.Errorf("invalid JSON in file: %w", err))
		return
	}

	start := time.Now()
	sec.Index(docs)
	elapsed := time.Since(start)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(AddToIndexResponse{
		IndexName:  indexName,
		AddedCount: len(docs),
		Duration:   elapsed.String(),
		DurationMs: elapsed.Milliseconds(),
	})
}

// AddOrUpdateDocument handles POST /add-or-update-document?index=<indexName>
// with the document JSON in the body.
func (ht *HTTP) AddOrUpdateDocument(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		ErrWriter(w, fmt.Errorf("unsupported method"))
		return
	}

	// 1) grab index name
	indexName := r.URL.Query().Get("index")
	if indexName == "" {
		ErrWriter(w, fmt.Errorf("`index` query parameter is required"))
		return
	}

	// 2) lookup controller
	ht.mu.RLock()
	sec, ok := ht.controllers[indexName]
	ht.mu.RUnlock()
	if !ok {
		ErrWriter(w, fmt.Errorf("index %q not found", indexName))
		return
	}

	// 3) decode single document from body
	var doc map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		ErrWriter(w, fmt.Errorf("invalid JSON body: %w", err))
		return
	}

	// 4) add or update in the index
	sec.AddOrUpdateDocument(doc)

	// 5) respond with the updated document
	resp := map[string]interface{}{
		"status":     "success",
		"statusCode": 200,
		"index":      indexName,
		"document":   doc,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ErrWriter(w, err)
	}
}

// AddOrUpdateDocumentInBulk handles POST /add-or-update-document-bulk?index=<indexName>
// with the request body being a JSON array of documents.
func (ht *HTTP) AddOrUpdateDocumentInBulk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		ErrWriter(w, fmt.Errorf("unsupported method"))
		return
	}

	// 1) grab index name
	indexName := r.URL.Query().Get("index")
	if indexName == "" {
		ErrWriter(w, fmt.Errorf("`index` query parameter is required"))
		return
	}

	// 2) lookup controller
	ht.mu.RLock()
	sec, ok := ht.controllers[indexName]
	ht.mu.RUnlock()
	if !ok {
		ErrWriter(w, fmt.Errorf("index %q not found", indexName))
		return
	}

	// 3) decode array of documents from body
	var docs []map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&docs); err != nil {
		ErrWriter(w, fmt.Errorf("invalid JSON body: %w", err))
		return
	}

	// 4) add or update each document using a worker pool
	start := time.Now()

	workerCount := sec.SearchEngineCount
	jobs := make(chan map[string]interface{}, len(docs))
	var wg sync.WaitGroup

	// start workers
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for doc := range jobs {
				sec.AddOrUpdateDocument(doc)
			}
		}()
	}

	// dispatch jobs
	for _, doc := range docs {
		jobs <- doc
	}
	close(jobs)

	// wait for all workers to finish
	wg.Wait()
	dur := time.Since(start)

	// 5) respond with summary
	resp := map[string]interface{}{
		"status":        "success",
		"statusCode":    200,
		"index":         indexName,
		"documentCount": len(docs),
		"duration":      dur.String(),
		"durationMs":    dur.Milliseconds(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ErrWriter(w, err)
	}
}

// RemoveDocumentByID handles DELETE /remove-document-by-id?index=<indexName>&id=<documentID>
func (ht *HTTP) RemoveDocumentByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.Header().Set("Allow", http.MethodDelete)
		ErrWriter(w, fmt.Errorf("unsupported method"))
		return
	}

	// 1) parse index name
	indexName := r.URL.Query().Get("index")
	if indexName == "" {
		ErrWriter(w, fmt.Errorf("`index` query parameter is required"))
		return
	}

	// 2) parse document ID
	docID := r.URL.Query().Get("id")
	if docID == "" {
		ErrWriter(w, fmt.Errorf("`id` query parameter is required"))
		return
	}

	// 3) lookup controller
	ht.mu.RLock()
	sec, ok := ht.controllers[indexName]
	ht.mu.RUnlock()
	if !ok {
		ErrWriter(w, fmt.Errorf("index %q not found", indexName))
		return
	}

	// 4) perform removal
	sec.RemoveDocumentByID(docID)

	// 5) send success response
	resp := map[string]interface{}{
		"status":     "success",
		"statusCode": 200,
		"index":      indexName,
		"removedID":  docID,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// SaveControllerRequest is the payload for saving a controller to disk.
type SaveControllerRequest struct {
	IndexName string `json:"indexName"`
}

// LoadControllerRequest is the payload for loading a controller from disk.
type LoadControllerRequest struct {
	IndexName string `json:"indexName"`
}

// SaveController persists all shard files for the named controller.
func (ht *HTTP) SaveController(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		ErrWriter(w, fmt.Errorf("unsupported method"))
		return
	}
	var req SaveControllerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ErrWriter(w, fmt.Errorf("invalid JSON payload: %w", err))
		return
	}
	if req.IndexName == "" {
		ErrWriter(w, fmt.Errorf("`indexName` is required"))
		return
	}
	ht.mu.RLock()
	sec, ok := ht.controllers[req.IndexName]
	ht.mu.RUnlock()
	if !ok {
		ErrWriter(w, fmt.Errorf("index %q not found", req.IndexName))
		return
	}
	if err := sec.SaveAllShards(req.IndexName); err != nil {
		ErrWriter(w, fmt.Errorf("failed to save controller: %w", err))
		return
	}
	resp := map[string]interface{}{
		"status":     "success",
		"statusCode": 200,
		"indexName":  req.IndexName,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (ht *HTTP) LoadController(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		ErrWriter(w, fmt.Errorf("unsupported method"))
		return
	}

	// Decode payload
	var req LoadControllerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ErrWriter(w, fmt.Errorf("invalid JSON payload: %w", err))
		return
	}
	if req.IndexName == "" {
		ErrWriter(w, fmt.Errorf("`indexName` is required"))
		return
	}

	// Ensure slot exists
	ht.mu.Lock()
	_, exists := ht.controllers[req.IndexName]
	if !exists {
		ht.controllers[req.IndexName] = &search.SearchEngineController{}
	}
	ht.mu.Unlock()

	// Discover shard count
	dataDir := filepath.Join("/data", req.IndexName)
	shardCount := 0
	for {
		shardPrefix := fmt.Sprintf("shard-%d", shardCount)
		dir := filepath.Join(dataDir, shardPrefix)
		engineFile := fmt.Sprintf("%s.engine.gob", dir)
		if _, err := os.Stat(engineFile); os.IsNotExist(err) {
			break
		}
		shardCount++
	}
	if shardCount == 0 {
		ErrWriter(w, fmt.Errorf("no shard files found with prefix %q", req.IndexName))
		return
	}

	// Parallel load
	start := time.Now()
	engines := make([]*search.SearchEngine, shardCount)
	var wg sync.WaitGroup
	var loadErr error
	var once sync.Once

	for id := 0; id < shardCount; id++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			shardPrefix := fmt.Sprintf("shard-%d", shardID)
			dir := filepath.Join(dataDir, shardPrefix)
			eng, err := search.LoadAll(dir)
			if err != nil {
				once.Do(func() {
					loadErr = fmt.Errorf("failed to load shard %d: %w", shardID, err)
				})
				return
			}
			engines[shardID] = eng
		}(id)
	}
	wg.Wait()

	if loadErr != nil {
		ErrWriter(w, loadErr)
		return
	}

	// Reconstruct controller
	first := engines[0]
	controller := search.NewSearchEngineController(
		first.IndexFields,
		first.Filters,
		first.PageSize,
		shardCount,
	)
	controller.Engines = engines
	controller.SearchEngineCount = shardCount

	ht.mu.Lock()
	ht.controllers[req.IndexName] = controller
	ht.mu.Unlock()

	duration := time.Since(start)

	// Response
	resp := map[string]interface{}{
		"status":     "success",
		"statusCode": 200,
		"indexName":  req.IndexName,
		"shards":     shardCount,
		"duration":   duration.String(),
		"durationMs": duration.Milliseconds(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// Health is a simple health‐check endpoint that returns how long it took
// the handler to run (i.e. “ping” duration).
func (ht *HTTP) Health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		ErrWriter(w, fmt.Errorf("unsupported method"))
		return
	}

	start := time.Now()
	// (no real work to do here)

	duration := time.Since(start)
	resp := map[string]interface{}{
		"status":     "ok",
		"duration":   duration.String(),
		"durationMs": duration.Milliseconds(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ErrWriter(w, err)
	}
}

func main() {
	// Initialize the handler with its internal controller map
	ht := NewHTTP()

	// Create a ServeMux and register your endpoints
	mux := http.NewServeMux()
	mux.HandleFunc("/create-index", ht.CreateIndex)
	mux.HandleFunc("/search", ht.Search)
	mux.HandleFunc("/add-to-index", ht.AddToIndex)
	mux.HandleFunc("/add-or-update-document", ht.AddOrUpdateDocument)
	mux.HandleFunc("/bulk-add-or-update-document", ht.AddOrUpdateDocumentInBulk)
	mux.HandleFunc("/remove-document-by-id", ht.RemoveDocumentByID)
	mux.HandleFunc("/save-controller", ht.SaveController)
	mux.HandleFunc("/load-controller", ht.LoadController)
	mux.HandleFunc("/health", ht.Health)

	// Configure and start the server
	addr := ":8080"
	log.Printf("Starting server on %s…", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}

	/*
				go test -race -count=1 -cover ./...

									CREATE INDEX:
									curl -X POST http://localhost:8080/create-index \
									  -H "Content-Type: application/json" \
									  -d '{
									    "indexName":   "products",
									    "filePath":    "$HOME/Documents/GoProjects/search/search/products.json",
									    "indexFields": ["name","tags"],
									    "filters":     ["year","category"],
									    "pageCount":   25,
									    "workers":     8
									  }'

									  curl -X POST http://localhost:8080/create-index \
					          -H "Content-Type: application/json" \
					          -d '{
					            "indexName":   "products",
					            "filePath":    "search/products.json",
					            "indexFields": ["name","tags"],
					            "filters":     ["year"]
					          }'

									ADD TO INDEX
									  curl -X POST http://localhost:8080/add-to-index \
							  -H "Content-Type: application/json" \
							  -d '{
							    "indexName":"products",
							    "filePath":"$HOME/Documents/GoProjects/search/search/products2.json"
							  }'

									  SEARCH:
									  curl "http://localhost:8080/search?index=products&q=laptop&page=1&filter=year:2020,category:electronics"
									  curl "http://localhost:8080/search?index=products&q=optim&page=0"


									  ---


									  docker build -t searchengine:latest .
		docker run -d \
		  -p 8080:8080 \
		  -v data:/data \
		  --name searchengine \
		  searchengine:latest
	*/
}
