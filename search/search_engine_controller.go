package search

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type SearchEngineController struct {
	Engines           []*SearchEngine
	SearchEngineCount int
	IndexFields       []string
	Filters           map[string]bool
	PageSize          int
	Weights           map[string]int
}

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

// // SaveAllShards persists every shard (SearchEngine) in the controller to disk in parallel.
// // It will spawn one goroutine per shard, each calling that engine’s SaveAll with prefix "<prefix>-<shardID>".
// // If any shard fails, it captures the first error and returns it after all saves complete.
// func (sec *SearchEngineController) SaveAllShards(prefix string) error {
// 	var wg sync.WaitGroup
// 	var once sync.Once
// 	var saveErr error

// 	for shardID, engine := range sec.Engines {
// 		wg.Add(1)
// 		go func(id int, eng *SearchEngine) {
// 			defer wg.Done()
// 			shardPrefix := fmt.Sprintf("%s-%d", prefix, id)
// 			if err := eng.SaveAll(shardPrefix); err != nil {
// 				once.Do(func() {
// 					saveErr = fmt.Errorf("failed to save shard %d: %w", id, err)
// 				})
// 			}
// 		}(shardID, engine)
// 	}

// 	wg.Wait()
// 	return saveErr
// }

// SaveAllShards persists every shard (SearchEngine) in the controller to disk.
// It will call each engine’s SaveAll method using a prefix of `<indexName>-shard-<shardID>`.
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

// CombineResults filters and prioritizes search results:
//  1. Multi-term results
//  2. Exact-term results
//  3. Prefix-term results
//  4. Fuzzy-term results
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
