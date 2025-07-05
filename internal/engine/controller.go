package engine

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// SearchEngineController manages multiple SearchEngine shards, providing
// parallel indexing, searching, persistence, and document updates.
type SearchEngineController struct {
	Engines           []*SearchEngine // individual shard instances
	SearchEngineCount int             // number of shards
	IndexFields       []string        // document fields to index
	Filters           map[string]bool // fields available for filtering
	PageSize          int             // approximate results per page in each shard
	NumberOfTotalDocs int64           // Number of total documents in the engine
	mu                sync.RWMutex
}

// CombinedResponse represents the merged search results from multiple shards.
// It encapsulates the selected set of documents (chosen by priority: multi-term, exact, prefix, then fuzzy),
// and carries metadata about whether any of the returned results came from a prefix-based search
// along with the maximum prefix length encountered during merging.
type CombinedResponse struct {
	Docs        []Document // The final list of documents, sorted by descending ScoreWeight.
	IsMultiTerm bool       // True if the search is performed for multi term search.
}

// NewSearchEngineController constructs a controller with the given configuration.
// It creates `searchEngineCount` shards, each initialized with the same indexFields,
// filters, and pageSize. Returns the controller ready for indexing and searching.
func NewSearchEngineController(indexFields []string,
	filters map[string]bool,
	pageSize,
	searchEngineCount int) *SearchEngineController {
	var seList []*SearchEngine
	for i := 0; i < searchEngineCount; i++ {
		se := NewSearchEngine(indexFields, filters, pageSize, i)
		seList = append(seList, se)
	}
	return &SearchEngineController{
		Engines:           seList,
		SearchEngineCount: searchEngineCount,
		IndexFields:       indexFields,
		Filters:           filters,
		PageSize:          pageSize,
		NumberOfTotalDocs: 0,
	}
}

func (sec *SearchEngineController) LoadAllShards(indexName string) error {
	baseDir := os.Getenv("INDEX_DATA_DIR")
	if baseDir == "" {
		baseDir = "./data"
	}

	dataDir := filepath.Join(baseDir, indexName)
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
		return fmt.Errorf("no shard files found with prefix %q", indexName)
	}

	engines := make([]*SearchEngine, shardCount)
	var wg sync.WaitGroup
	var loadErr error
	var once sync.Once

	for id := 0; id < shardCount; id++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			shardPrefix := fmt.Sprintf("shard-%d", shardID)
			dir := filepath.Join(dataDir, shardPrefix)
			eng, err := LoadAll(dir, shardID)
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
		return loadErr
	}

	first := engines[0]
	sec.Engines = engines
	sec.Filters = first.Filters
	sec.PageSize = first.PageSize
	sec.SearchEngineCount = shardCount
	sec.IndexFields = first.IndexFields

	for _, engine := range sec.Engines {
		sec.NumberOfTotalDocs += int64(len(engine.Documents))
	}

	fmt.Println("TotalDocs:", sec.NumberOfTotalDocs)
	return nil
}

// SaveAllShards persists every shard to disk under /data/<indexName>/shard-<ID>/.
// It creates the directory if needed, then calls each shard's SaveAll() to write its files.
func (sec *SearchEngineController) SaveAllShards(indexName string) error {
	baseDir := os.Getenv("INDEX_DATA_DIR")
	if baseDir == "" {
		baseDir = "./data"
	}
	dir := filepath.Join(baseDir, indexName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	for shardID, engine := range sec.Engines {
		shardPrefix := fmt.Sprintf("shard-%d", shardID)
		if err := engine.SaveAll(filepath.Join(dir, shardPrefix)); err != nil {
			return fmt.Errorf("failed to save shard %d: %w", shardID, err)
		}
	}
	return nil
}

// Index distributes documents evenly across all shards and runs Index() in parallel.
// Documents slice is chunked by ceil(len/docs / shardCount), and each goroutine
// indexes its segment, then waits for all to complete.
func (sec *SearchEngineController) Index2(docs []map[string]interface{}) {
	sec.mu.Lock()
	sec.NumberOfTotalDocs += int64(len(docs))
	sec.mu.Unlock()
	chunkSize := (len(docs) + sec.SearchEngineCount - 1) / sec.SearchEngineCount

	var wg sync.WaitGroup

	for i := 0; i < sec.SearchEngineCount; i++ {
		dataStart := chunkSize * i
		if dataStart >= len(docs) {
			break
		}
		dataEnd := dataStart + chunkSize
		if dataEnd > len(docs) {
			dataEnd = len(docs)
		}

		wg.Add(1)

		go func(engineIdx, start, end int) {
			defer wg.Done()
			sec.Engines[engineIdx].Index(engineIdx, docs[start:end])
		}(i, dataStart, dataEnd)
	}

	wg.Wait()
}

func (sec *SearchEngineController) Index(docs []map[string]interface{}) {
	// Update total docs count
	sec.mu.Lock()
	sec.NumberOfTotalDocs += int64(len(docs))
	sec.mu.Unlock()

	// Prepare shards in round-robin fashion
	shards := make([][]map[string]interface{}, sec.SearchEngineCount)
	for i, doc := range docs {
		shardIdx := i % sec.SearchEngineCount
		shards[shardIdx] = append(shards[shardIdx], doc)
	}

	var wg sync.WaitGroup
	for engineIdx, shardDocs := range shards {
		if len(shardDocs) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, items []map[string]interface{}) {
			defer wg.Done()
			sec.Engines[idx].Index(idx, items)
		}(engineIdx, shardDocs)
	}

	wg.Wait()
}

// Search executes the query on all shards in parallel, then merges results.
// Parameters: query string, page number, and filter map. Returns sorted Documents.
func (sec *SearchEngineController) Search(query string, page int, filters map[string][]interface{}, searchStep int) []Document {
	resultsChan := make(chan *SearchResult, sec.SearchEngineCount)
	var wg sync.WaitGroup

	for _, se := range sec.Engines {
		wg.Add(1)
		go func(e *SearchEngine) {
			defer wg.Done()
			result := e.Search(query, page, filters, searchStep)
			resultsChan <- result
		}(se)
	}
	wg.Wait()
	close(resultsChan)

	res := CombineResults(resultsChan)
	if res == nil {
		return []Document{}
	} else {
		if len(res.Docs) == 0 && searchStep < 2 && res.IsMultiTerm {
			// fmt.Println("sec.Search", searchStep)
			return sec.Search(query, page, filters, searchStep+1)
		}
	}
	return res.Docs
}

// CombineResults merges shard SearchResults by priority: multi-term, prefixOrExact, then fuzzy.
// It collects docs into the first non-empty category, sorts by ScoreWeight desc, and returns.
func CombineResults(resultsChan <-chan *SearchResult) *CombinedResponse {
	var multi, prefix, fuzzy []Document

	isMultiTerm := false
	for res := range resultsChan {
		if res == nil {
			continue
		}
		isMultiTerm = res.IsMultiTerm
		switch {
		case res.IsMultiTerm:
			multi = append(multi, res.Docs...)
		case res.IsPrefixOrExact:
			prefix = append(prefix, res.Docs...)
		case res.IsFuzzy:
			fuzzy = append(fuzzy, res.Docs...)
		}
	}
	var returnResult []Document
	switch {
	case len(multi) > 0:
		returnResult = multi
	case len(prefix) > 0:
		returnResult = prefix
	case len(fuzzy) > 0:
		returnResult = fuzzy
	default:
		resp := CombinedResponse{
			Docs:        returnResult,
			IsMultiTerm: isMultiTerm,
		}
		return &resp
	}

	sort.Slice(returnResult, func(i, j int) bool {
		return returnResult[i].ScoreWeight > returnResult[j].ScoreWeight
	})

	resp := CombinedResponse{
		Docs:        returnResult,
		IsMultiTerm: isMultiTerm,
	}
	return &resp
}

// RemoveDocumentByID removes the given docID from all shards in parallel.
func (sec *SearchEngineController) RemoveDocumentByID(docID string) {
	var wg sync.WaitGroup
	for _, engine := range sec.Engines {
		wg.Add(1)
		go func(se *SearchEngine) {
			defer wg.Done()
			se.removeDocumentByID(docID)
		}(engine)
	}
	wg.Wait()
	sec.mu.Lock()
	sec.NumberOfTotalDocs--
	sec.mu.Unlock()
}

// AddOrUpdateDocument either updates an existing document in its shard,
// or inserts it into a random shard if new. Guarantees a single copy.
func (sec *SearchEngineController) AddOrUpdateDocument(doc map[string]interface{}) {
	docID := fmt.Sprintf("%v", doc["id"])
	isExist := false
	seIndex := 0
	for k, engine := range sec.Engines {
		engine.mu.RLock()
		_, ok := engine.Documents[docID]
		engine.mu.RUnlock()
		if ok {
			isExist = true
			seIndex = k
			break
		}
	}

	if isExist {
		sec.Engines[seIndex].removeDocumentByID(docID)
		sec.Engines[seIndex].addDocument(doc)
	} else {
		luckyShard := rand.Intn(len(sec.Engines))
		sec.Engines[luckyShard].addDocument(doc)
		sec.mu.Lock()
		sec.NumberOfTotalDocs++
		sec.mu.Unlock()
	}
}
