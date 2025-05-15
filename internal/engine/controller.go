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
	PageSize          int             // results per page
	Weights           map[string]int  // optional per-field weights
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
	}
}

// SaveAllShards persists every shard to disk under /data/<indexName>/shard-<ID>/.
// It creates the directory if needed, then calls each shard's SaveAll() to write its files.
func (sec *SearchEngineController) SaveAllShards(indexName string) error {
	dir := filepath.Join("/data", indexName)
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
func (sec *SearchEngineController) Index(docs []map[string]interface{}) {
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

// Search executes the query on all shards in parallel, then merges results.
// Parameters: query string, page number, and filter map. Returns sorted Documents.
func (sec *SearchEngineController) Search(query string, page int, filters map[string][]interface{}) []Document {
	resultsChan := make(chan *SearchResult, sec.SearchEngineCount)
	var wg sync.WaitGroup

	for _, se := range sec.Engines {
		wg.Add(1)
		go func(e *SearchEngine) {
			defer wg.Done()
			result := e.Search(query, page, filters)
			resultsChan <- result
		}(se)
	}
	wg.Wait()
	close(resultsChan)

	return CombineResults(resultsChan)
}

// CombineResults merges shard SearchResults by priority: multi-term, exact, prefix, then fuzzy.
// It collects docs into the first non-empty category, sorts by ScoreWeight desc, and returns.
func CombineResults(resultsChan <-chan *SearchResult) []Document {
	var multi, exact, prefix, fuzzy []Document
	for res := range resultsChan {
		if res == nil {
			continue
		}
		switch {
		case res.IsMultiTerm:
			multi = append(multi, res.Docs...)
		case res.IsExact:
			exact = append(exact, res.Docs...)
		case res.IsPrefix:
			prefix = append(prefix, res.Docs...)
		case res.IsFuzzy:
			fuzzy = append(fuzzy, res.Docs...)
		}
	}
	var returnResult []Document
	switch {
	case len(multi) > 0:
		returnResult = multi
	case len(exact) > 0:
		returnResult = exact
	case len(prefix) > 0:
		returnResult = prefix
	case len(fuzzy) > 0:
		returnResult = fuzzy
	default:
		return nil
	}

	sort.Slice(returnResult, func(i, j int) bool {
		return returnResult[i].ScoreWeight > returnResult[j].ScoreWeight
	})

	return returnResult
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
	}
}
