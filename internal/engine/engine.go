package engine

import (
	"encoding/gob"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mg52/search/internal/pkg/keys"
	"github.com/mg52/search/internal/pkg/trie"
)

// Document represents a search result with an ID and its computed weight.
type Document struct {
	ID          string
	Data        map[string]interface{}
	ScoreWeight int
}

// SearchResult encapsulates the result of a search on one shard.
type SearchResult struct {
	Docs         []Document
	IsMultiTerm  bool
	IsExact      bool
	IsPrefix     bool
	IsFuzzy      bool
	PrefixLength int
}

// TokenFieldPair associates a token with its originating field (unused).
type TokenFieldPair struct {
	Token string
	Field string
}

// stopWords lists common words to exclude from indexing.
var stopWords = map[string]bool{
	// "is": true, "a": true, "the": true, "and": true, "or": true, "to": true, "in": true, "of": true,
}

// enginePayload serializes the core SearchEngine state.
type enginePayload struct {
	Data        map[string]map[string]int
	DocData     map[string]map[string]bool
	Documents   map[string]map[string]interface{}
	ScoreIndex  map[string][]Document
	FilterDocs  map[string]map[string]bool
	IndexFields []string
	Filters     map[string]bool
	PageSize    int
	Keys        *keys.Keys
	Trie        *trie.Trie
}

// SearchEngine maintains an inverted index, documents, and search structures.
type SearchEngine struct {
	ShardID     int
	Data        map[string]map[string]int         // term -> docID -> int
	DocData     map[string]map[string]bool        // docID -> term -> true
	Documents   map[string]map[string]interface{} // docID -> fields
	ScoreIndex  map[string][]Document             // term -> Documents (ordered by weight desc)
	FilterDocs  map[string]map[string]bool        // filter -> docID e.g., map["year:2019"]map["doc1"] = true
	Keys        *keys.Keys
	Trie        *trie.Trie
	IndexFields []string
	Filters     map[string]bool // filters for fields
	PageSize    int             // size of the documents in a search response.
	mu          sync.RWMutex
}

// NewSearchEngine initializes a new search engine.
func NewSearchEngine(
	indexFields []string,
	filters map[string]bool,
	pageSize, shardID int) *SearchEngine {
	return &SearchEngine{
		ShardID:     shardID,
		Data:        make(map[string]map[string]int),
		DocData:     make(map[string]map[string]bool),
		Documents:   make(map[string]map[string]interface{}),
		IndexFields: indexFields,
		Filters:     filters,
		ScoreIndex:  make(map[string][]Document),
		Keys:        keys.NewKeys(),
		Trie:        trie.NewTrie(),
		FilterDocs:  make(map[string]map[string]bool),
		PageSize:    pageSize,
	}
}

func init() {
	gob.Register(enginePayload{})
	gob.Register(Document{})
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register(&trie.Trie{})
	gob.Register(&trie.TrieNode{})
}

// SaveAll writes the engine state to disk as gob files with the given prefix.
// It serializes the core payload; keys and trie may be saved separately if needed.
func (se *SearchEngine) SaveAll(path string) error {
	se.mu.RLock()
	payload := enginePayload{
		Data:        se.Data,
		DocData:     se.DocData,
		Documents:   se.Documents,
		FilterDocs:  se.FilterDocs,
		IndexFields: se.IndexFields,
		Filters:     se.Filters,
		PageSize:    se.PageSize,
		Keys:        se.Keys,
		Trie:        se.Trie,
	}
	se.mu.RUnlock()

	engineFile := path + ".engine.gob"
	f, err := os.Create(engineFile)
	if err != nil {
		return fmt.Errorf("create %s: %w", engineFile, err)
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(payload); err != nil {
		f.Close()
		return fmt.Errorf("encode engine payload: %w", err)
	}
	f.Close()

	return nil
}

// LoadAll loads a SearchEngine from gob files with the given prefix.
// Missing files are skipped, resulting in fresh defaults for those parts.
func LoadAll(path string, shardID int) (*SearchEngine, error) {
	se := &SearchEngine{
		Data:        make(map[string]map[string]int),
		DocData:     make(map[string]map[string]bool),
		Documents:   make(map[string]map[string]interface{}),
		ScoreIndex:  make(map[string][]Document),
		FilterDocs:  make(map[string]map[string]bool),
		Keys:        keys.NewKeys(),
		Trie:        trie.NewTrie(),
		IndexFields: nil,
		Filters:     make(map[string]bool),
		PageSize:    0,
		ShardID:     shardID,
	}

	engineFile := path + ".engine.gob"
	if f, err := os.Open(engineFile); err == nil {
		var payload enginePayload
		dec := gob.NewDecoder(f)
		if err := dec.Decode(&payload); err != nil {
			f.Close()
			return nil, fmt.Errorf("decode engine payload: %w", err)
		}
		f.Close()

		se.Data = payload.Data
		se.DocData = payload.DocData
		se.Documents = payload.Documents
		// se.ScoreIndex = payload.ScoreIndex
		se.FilterDocs = payload.FilterDocs
		se.IndexFields = payload.IndexFields
		se.Filters = payload.Filters
		se.PageSize = payload.PageSize
		se.Keys = payload.Keys
		se.Trie = payload.Trie
	}

	se.BuildScoreIndex()

	return se, nil
}

// Index wraps BuildDocumentIndex, BuildScoreIndex, and InsertDocs with timing logs.
func (se *SearchEngine) Index(shardID int, docs []map[string]interface{}) {
	fmt.Println("Insert documents starting...")
	start := time.Now()
	se.InsertDocs(docs)
	duration := time.Since(start)
	fmt.Printf("Insert documents took: %s\n", duration)

	fmt.Println("BuildDocumentIndex starting...")
	start = time.Now()
	se.BuildDocumentIndex(docs)
	duration = time.Since(start)
	fmt.Printf("BuildDocumentIndex took: %s\n", duration)

	fmt.Println("BuildScoreIndex starting...")
	start = time.Now()
	se.BuildScoreIndex()
	duration = time.Since(start)
	fmt.Printf("BuildScoreIndex took: %s\n", duration)
}

// ProcessQuery analyzes a search query string and categorizes tokens based on match type.
// It returns a map where the keys represent the match category:
// - "exact": tokens that exactly match entries in the GlobalKeys store.
// - "prefix": tokens that match prefixes found in the GlobalTrie.
// - "fuzzy": tokens that approximately match entries in GlobalKeys within a given edit distance.
func (se *SearchEngine) ProcessQuery(query string, prefixCount int) (map[string][]string, int) {
	queryTokens := Tokenize(query)
	tokenCount := len(queryTokens)
	if tokenCount == 0 {
		return nil, 0
	}

	result := make(map[string][]string)
	result["raw"] = queryTokens
	if tokenCount == 1 {
		guessArr := se.Trie.SearchPrefix(queryTokens[0], prefixCount)
		if guessArr != nil {
			result["prefix"] = append(result["prefix"], guessArr...)
		} else {
			fuzzyMatch := false
			for key := range se.Keys.GetData() {
				if FuzzyMatch(key, queryTokens[0], 1) {
					result["fuzzy"] = append(result["fuzzy"], key)
					fuzzyMatch = true
					break
				}
			}
			if !fuzzyMatch {
				result["fuzzy"] = append(result["fuzzy"], queryTokens[0])
			}
		}
	} else {
		lastWord := queryTokens[tokenCount-1]
		for _, exactWords := range queryTokens[:tokenCount-1] {
			_, ok := se.Keys.GetData()[exactWords]
			if ok {
				result["exact"] = append(result["exact"], exactWords)
			} else {
				guessArr := se.Trie.SearchPrefix(exactWords, 1)
				if guessArr != nil {
					result["exact"] = append(result["exact"], guessArr[0])
				} else {
					fuzzyMatch := false
					for key := range se.Keys.GetData() {
						if FuzzyMatch(key, exactWords, 1) {
							result["exact"] = append(result["exact"], key)
							fuzzyMatch = true
							break
						}
					}
					if !fuzzyMatch {
						result["exact"] = append(result["exact"], lastWord)
					}
				}
			}
		}
		guessArr := se.Trie.SearchPrefix(lastWord, prefixCount)
		if guessArr != nil {
			result["prefix"] = append(result["prefix"], guessArr...)
		} else {
			fuzzyMatch := false
			for key := range se.Keys.GetData() {
				if FuzzyMatch(key, lastWord, 1) {
					result["fuzzy"] = append(result["fuzzy"], key)
					fuzzyMatch = true
					break
				}
			}
			if !fuzzyMatch {
				result["fuzzy"] = append(result["fuzzy"], lastWord)
			}
		}
	}

	return result, tokenCount
}

// BuildScoreIndex constructs the ScoreIndex by sorting each term's postings by weight.
func (se *SearchEngine) BuildScoreIndex() {
	workers := runtime.NumCPU()
	var wg sync.WaitGroup

	keysCh := make(chan string, len(se.Keys.GetData()))

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range keysCh {
				var tempArr []Document
				for docID, weight := range se.Data[key] {
					tempArr = append(tempArr, Document{ID: docID, ScoreWeight: weight, Data: se.Documents[docID]})
				}
				sort.Slice(tempArr, func(i, j int) bool {
					return tempArr[i].ScoreWeight > tempArr[j].ScoreWeight
				})
				se.mu.Lock()
				se.ScoreIndex[key] = tempArr
				se.mu.Unlock()
			}
		}()
	}

	for key := range se.Keys.GetData() {
		keysCh <- key
	}
	close(keysCh)

	wg.Wait()
}

// InsertDocs stores raw documents for retrieval without reindexing.
func (se *SearchEngine) InsertDocs(docs []map[string]interface{}) {
	for i, doc := range docs {
		if i%100_000 == 0 || i == len(docs)-1 {
			fmt.Println("InsertDocs:", i)
		}

		docID := fmt.Sprintf("%v", doc["id"])

		se.mu.Lock()
		if _, exists := se.Documents[docID]; !exists {
			se.Documents[docID] = make(map[string]interface{})
			for k, v := range doc {
				se.Documents[docID][k] = v
			}
		}
		se.mu.Unlock()
	}
}

// BuildDocumentIndex tokenizes each document and updates the inverted index and filters.
func (se *SearchEngine) BuildDocumentIndex(docs []map[string]interface{}) {
	for i, doc := range docs {
		if i%100_000 == 0 || i == len(docs)-1 {
			fmt.Println("BuildDocumentIndex Document:", i)
		}

		docID := fmt.Sprintf("%v", doc["id"])

		var allTokens []string
		for _, weightField := range se.IndexFields {
			if value, exists := doc[weightField]; exists {
				switch v := value.(type) {
				case string:
					tokens := Tokenize(v)
					allTokens = append(allTokens, tokens...)
				case []string:
					for _, item := range v {
						tokens := Tokenize(item)
						allTokens = append(allTokens, tokens...)
					}
				case []interface{}:
					for _, item := range v {
						if str, ok := item.(string); ok {
							tokens := Tokenize(str)
							allTokens = append(allTokens, tokens...)
						}
					}
				}
			}
		}

		se.mu.RLock()
		docDataMap, ok := se.DocData[docID]
		se.mu.RUnlock()
		if !ok {
			docDataMap = make(map[string]bool)
			se.mu.Lock()
			se.DocData[docID] = docDataMap
			se.mu.Unlock()
		}
		for _, token := range allTokens {
			docDataMap[token] = true
			se.addToDocumentIndex(token, docID, len(allTokens))
		}

		for field := range se.Filters {
			if value, exists := doc[field]; exists {
				switch v := value.(type) {
				case int, int8, int16, int32, int64, float32, float64:
					filterKey := fmt.Sprintf("%s:%v", field, v)
					se.mu.Lock()
					if se.FilterDocs[filterKey] == nil {
						se.FilterDocs[filterKey] = make(map[string]bool)
					}
					se.FilterDocs[filterKey][docID] = true
					se.mu.Unlock()
				case string:
					filterKey := fmt.Sprintf("%s:%s", field, v)
					se.mu.Lock()
					if se.FilterDocs[filterKey] == nil {
						se.FilterDocs[filterKey] = make(map[string]bool)
					}
					se.FilterDocs[filterKey][docID] = true
					se.mu.Unlock()
				}
			}
		}
	}
}

// addToDocumentIndex adds weight for term→docID and updates Keys/Trie if needed.
func (se *SearchEngine) addToDocumentIndex(
	term, docID string, length int,
) {
	// TODO: Popularity can be added in this function as a new parameter
	// then popularity can be added to the normalizedWeight as a bias.
	normalizedWeight := (100_000 / length)

	se.mu.Lock()

	docMap, ok := se.Data[term]
	if !ok {
		se.Keys.Insert(term)
		se.Trie.Insert(term)
		docMap = make(map[string]int)
		se.Data[term] = docMap
	}
	docMap[docID] += normalizedWeight

	se.mu.Unlock()
}

// SearchOneTermWithoutFilter returns a page of results for a single term.
func (se *SearchEngine) SearchOneTermWithoutFilter(query string, page int) []Document {
	se.mu.RLock()
	defer se.mu.RUnlock()

	lastIndex := page * se.PageSize
	if lastIndex >= len(se.ScoreIndex[query]) {
		return nil
	}
	stop := lastIndex + se.PageSize
	if stop > len(se.ScoreIndex[query]) {
		stop = len(se.ScoreIndex[query])
	}
	return se.ScoreIndex[query][lastIndex:stop]
}

// ApplyFilter returns the set of docIDs matching the given filter criteria.
// It supports single or multiple values per filter field.
func (se *SearchEngine) ApplyFilter(filters map[string][]interface{}) map[string]bool {
	if len(filters) == 1 {
		for key, values := range filters {
			if len(values) == 1 {
				se.mu.RLock()
				filteredDocs := se.FilterDocs[fmt.Sprintf("%s:%v", key, values[0])]
				se.mu.RUnlock()
				return filteredDocs
			}
			filteredDocsFinal := make(map[string]bool)
			for _, value := range values {
				se.mu.RLock()
				for docID := range se.FilterDocs[fmt.Sprintf("%s:%v", key, value)] {
					filteredDocsFinal[docID] = true
				}
				se.mu.RUnlock()
			}
			return filteredDocsFinal
		}
	} else {
		filteredDocsFinal := make(map[string]bool)
		for k, v := range filters {
			se.mu.RLock()
			for docID := range se.FilterDocs[fmt.Sprintf("%s:%v", k, v)] {
				filteredDocsFinal[docID] = true
			}
			se.mu.RUnlock()
		}
		return filteredDocsFinal
	}
	return nil
}

// SearchOneTermWithFilter retrieves a page of documents matching a term, applying filter constraints.
// It returns at most PageSize documents from the filtered set.
func (se *SearchEngine) SearchOneTermWithFilter(query string, filters map[string][]interface{}, page int) []Document {
	filteredDocs := se.ApplyFilter(filters)
	lastIndex := page * se.PageSize
	if lastIndex >= len(filteredDocs) {
		return nil
	}

	se.mu.RLock()
	termDocs := se.ScoreIndex[query]
	se.mu.RUnlock()
	if lastIndex >= len(termDocs) {
		return nil
	}

	var finalDocs []Document
	counter := 0

	// TODO: saving last index or returning last index can improve the performance.
	for i := 0; i < len(termDocs); i++ {
		if _, ok := filteredDocs[termDocs[i].ID]; ok {
			finalDocs = append(finalDocs, termDocs[i])
			counter++
			if counter == lastIndex+se.PageSize {
				return finalDocs[lastIndex:]
			}
		}
	}

	if lastIndex >= len(finalDocs) {
		return nil
	}

	return finalDocs[lastIndex:]
}

// TODO: if we want to order by popularity,
// we need to make multi term search by checking ALL terms given
// in the input in the ScoreIndex and because it is ordered by final score, we can return
// page count amount of documents if there is matching for all terms' ScoreIndex.

// SearchMultipleTermsWithoutFilter is a public wrapper for multi-term searches without filters.
func (se *SearchEngine) SearchMultipleTermsWithoutFilter(queriesMap map[string][]string, page int) []Document {
	if len(queriesMap["raw"]) == 2 {
		if len(queriesMap["raw"][1]) <= 2 {
			return se.SearchDoubleTermsWithPrefixWithoutFilter(queriesMap, page)
		} else {
			return se.SearchDoubleTermsWithoutFilter(queriesMap, page)
		}
	} else {
		if len(queriesMap["raw"][len(queriesMap["raw"])-1]) <= 2 {
			return se.SearchMoreThanDoubleTermsWithPrefixWithoutFilter(queriesMap, page)
		} else {
			return se.SearchMoreThanDoubleTermsWithoutFilter(queriesMap, page)
		}
	}
}

// SearchDoubleTermsWithPrefixWithoutFilter is a public double-term search function with prefix without filters.
func (se *SearchEngine) SearchDoubleTermsWithPrefixWithoutFilter(queriesMap map[string][]string, page int) []Document {
	var finalDocs []Document

	firstTerm := queriesMap["exact"][0]
	secondTerm := queriesMap["raw"][1]
	se.mu.RLock()
	defer se.mu.RUnlock()

	startIndex := page * se.PageSize
	stopIndex := startIndex + se.PageSize

	for _, doc := range se.ScoreIndex[firstTerm] {
		totalScore := doc.ScoreWeight
		hasPrefix := false
		for term, _ := range se.DocData[doc.ID] {
			if strings.HasPrefix(term, secondTerm) {
				hasPrefix = true
				totalScore += se.Data[term][doc.ID]
			}
		}

		if !hasPrefix {
			continue
		}

		finalDocs = append(finalDocs, Document{
			ID:          doc.ID,
			Data:        se.Documents[doc.ID],
			ScoreWeight: totalScore,
		})
		if len(finalDocs) >= stopIndex {
			break
		}
	}

	if startIndex > len(finalDocs) {
		return nil
	}
	if stopIndex > len(finalDocs) {
		stopIndex = len(finalDocs)
	}

	return finalDocs[startIndex:stopIndex]
}

// SearchDoubleTermsWithoutFilter is a public double-term search function without filters.
func (se *SearchEngine) SearchDoubleTermsWithoutFilter(queriesMap map[string][]string, page int) []Document {
	var finalDocs []Document

	firstTerm := queriesMap["exact"][0]
	se.mu.RLock()
	scoreIndexLength := se.ScoreIndex[firstTerm]
	se.mu.RUnlock()
	// TODO: magic number, make it configurative
	if len(scoreIndexLength) < 250 {
		return se.SearchDoubleTermsWithPrefixWithoutFilter(queriesMap, page)
	}

	otherTerms := append(queriesMap["prefix"], queriesMap["fuzzy"]...)
	se.mu.RLock()
	defer se.mu.RUnlock()

	startIndex := page * se.PageSize
	stopIndex := startIndex + se.PageSize

	for _, doc := range se.ScoreIndex[firstTerm] {
		foundOther := false
		otherWeight := 0
		for _, term := range otherTerms {
			if otherData, ok := se.Data[term][doc.ID]; ok {
				foundOther = true
				otherWeight += otherData
				// break
			}
		}
		if !foundOther {
			continue
		}

		totalScore := doc.ScoreWeight + otherWeight

		finalDocs = append(finalDocs, Document{
			ID:          doc.ID,
			Data:        se.Documents[doc.ID],
			ScoreWeight: totalScore,
		})
		if len(finalDocs) >= stopIndex {
			break
		}
	}

	if startIndex > len(finalDocs) {
		return nil
	}
	if stopIndex > len(finalDocs) {
		stopIndex = len(finalDocs)
	}

	return finalDocs[startIndex:stopIndex]
}

// SearchMoreThanDoubleTermsWithPrefixWithoutFilter is a public multi-term search function with prefix for without filters.
func (se *SearchEngine) SearchMoreThanDoubleTermsWithPrefixWithoutFilter(queriesMap map[string][]string, page int) []Document {
	exactTerms := queriesMap["exact"]
	secondTerm := queriesMap["raw"][len(queriesMap["raw"])-1]

	var finalDocs []Document

	inputTerm := exactTerms[0]

	se.mu.RLock()
	defer se.mu.RUnlock()

	startIndex := page * se.PageSize
	stopIndex := startIndex + se.PageSize

	for _, doc := range se.ScoreIndex[inputTerm] {
		inAllExact := true
		allOtherExactWeights := 0
		for i := 1; i < len(exactTerms); i++ {
			otherExactWeight, ok := se.Data[exactTerms[i]][doc.ID]
			if !ok {
				inAllExact = false
				break
			}
			allOtherExactWeights += otherExactWeight
		}
		if !inAllExact {
			continue
		}

		totalScore := doc.ScoreWeight + allOtherExactWeights

		hasPrefix := false
		for term, _ := range se.DocData[doc.ID] {
			if strings.HasPrefix(term, secondTerm) {
				hasPrefix = true
				totalScore += se.Data[term][doc.ID]
			}
		}

		if !hasPrefix {
			continue
		}

		finalDocs = append(finalDocs, Document{
			ID:          doc.ID,
			Data:        se.Documents[doc.ID],
			ScoreWeight: totalScore,
		})
		if len(finalDocs) >= stopIndex {
			break
		}
	}

	if startIndex > len(finalDocs) {
		return nil
	}
	if stopIndex > len(finalDocs) {
		stopIndex = len(finalDocs)
	}

	return finalDocs[startIndex:stopIndex]
}

// SearchMoreThanDoubleTermsWithoutFilter is a public multi-term search function for without filters.
func (se *SearchEngine) SearchMoreThanDoubleTermsWithoutFilter(queriesMap map[string][]string, page int) []Document {
	exactTerms := queriesMap["exact"]
	otherTerms := append(queriesMap["prefix"], queriesMap["fuzzy"]...)

	se.mu.RLock()
	defer se.mu.RUnlock()
	if len(exactTerms) == 0 {
		return nil
	}

	var finalDocs []Document

	inputTerm := exactTerms[0]
	startIndex := page * se.PageSize
	stopIndex := startIndex + se.PageSize

	for _, doc := range se.ScoreIndex[inputTerm] {
		inAllExact := true
		allOtherExactWeights := 0
		for i := 1; i < len(exactTerms); i++ {
			otherExactWeight, ok := se.Data[exactTerms[i]][doc.ID]
			if !ok {
				inAllExact = false
				break
			}
			allOtherExactWeights += otherExactWeight
		}
		if !inAllExact {
			continue
		}

		totalScore := doc.ScoreWeight + allOtherExactWeights

		foundOther := false
		otherWeight := 0
		for _, term := range otherTerms {
			if otherData, ok := se.Data[term][doc.ID]; ok {
				foundOther = true
				otherWeight += otherData
				// break
			}
		}
		if !foundOther {
			continue
		}

		totalScore += otherWeight

		finalDocs = append(finalDocs, Document{
			ID:          doc.ID,
			Data:        se.Documents[doc.ID],
			ScoreWeight: totalScore,
		})
		if len(finalDocs) >= stopIndex {
			break
		}
	}

	if startIndex > len(finalDocs) {
		return nil
	}
	if stopIndex > len(finalDocs) {
		stopIndex = len(finalDocs)
	}

	return finalDocs[startIndex:stopIndex]
}

// SearchMultipleTermsWithFilter is a public wrapper for multi-term searches with filters.
func (se *SearchEngine) SearchMultipleTermsWithFilter(queriesMap map[string][]string, filters map[string][]interface{}, page int) []Document {
	if len(queriesMap["raw"]) == 2 {
		if len(queriesMap["raw"][1]) <= 2 {
			return se.SearchDoubleTermsWithPrefixWithFilter(queriesMap, filters, page)
		} else {
			return se.SearchDoubleTermsWithFilter(queriesMap, filters, page)
		}
	} else if len(queriesMap["raw"]) > 2 {
		if len(queriesMap["raw"][len(queriesMap["raw"])-1]) <= 2 {
			return se.SearchMoreThanDoubleTermsWithPrefixWithFilter(queriesMap, filters, page)
		} else {
			return se.SearchMoreThanDoubleTermsWithFilter(queriesMap, filters, page)
		}
	}
	return nil
}

// SearchDoubleTermsWithPrefixWithFilter is a public double-term search function with prefix with filters.
func (se *SearchEngine) SearchDoubleTermsWithPrefixWithFilter(
	queriesMap map[string][]string, filters map[string][]interface{}, page int) []Document {
	filteredDocs := se.ApplyFilter(filters)
	var finalDocs []Document

	firstTerm := queriesMap["exact"][0]
	secondTerm := queriesMap["raw"][1]
	se.mu.RLock()
	defer se.mu.RUnlock()

	startIndex := page * se.PageSize
	stopIndex := startIndex + se.PageSize

	for _, doc := range se.ScoreIndex[firstTerm] {
		if _, ok := filteredDocs[doc.ID]; !ok {
			continue
		}
		totalScore := doc.ScoreWeight
		hasPrefix := false
		for term, _ := range se.DocData[doc.ID] {
			if strings.HasPrefix(term, secondTerm) {
				hasPrefix = true
				totalScore += se.Data[term][doc.ID]
			}
		}

		if !hasPrefix {
			continue
		}

		finalDocs = append(finalDocs, Document{
			ID:          doc.ID,
			Data:        se.Documents[doc.ID],
			ScoreWeight: totalScore,
		})
		if len(finalDocs) >= stopIndex {
			break
		}
	}

	if startIndex > len(finalDocs) {
		return nil
	}
	if stopIndex > len(finalDocs) {
		stopIndex = len(finalDocs)
	}

	return finalDocs[startIndex:stopIndex]
}

// SearchDoubleTermsWithFilter is a public double-term search function with filters.
func (se *SearchEngine) SearchDoubleTermsWithFilter(
	queriesMap map[string][]string, filters map[string][]interface{}, page int) []Document {
	filteredDocs := se.ApplyFilter(filters)
	var finalDocs []Document

	firstTerm := queriesMap["exact"][0]
	otherTerms := append(queriesMap["prefix"], queriesMap["fuzzy"]...)
	se.mu.RLock()
	defer se.mu.RUnlock()

	startIndex := page * se.PageSize
	stopIndex := startIndex + se.PageSize

	for _, doc := range se.ScoreIndex[firstTerm] {
		if _, ok := filteredDocs[doc.ID]; !ok {
			continue
		}
		foundOther := false
		otherWeight := 0
		for _, term := range otherTerms {
			if otherData, ok := se.Data[term][doc.ID]; ok {
				foundOther = true
				otherWeight += otherData
				// break
			}
		}
		if !foundOther {
			continue
		}

		totalScore := doc.ScoreWeight + otherWeight

		finalDocs = append(finalDocs, Document{
			ID:          doc.ID,
			Data:        se.Documents[doc.ID],
			ScoreWeight: totalScore,
		})
		if len(finalDocs) >= stopIndex {
			break
		}
	}

	if startIndex > len(finalDocs) {
		return nil
	}
	if stopIndex > len(finalDocs) {
		stopIndex = len(finalDocs)
	}

	return finalDocs[startIndex:stopIndex]
}

// SearchMoreThanDoubleTermsWithPrefixWithFilter is a public multi-term search function with prefix for with filters.
func (se *SearchEngine) SearchMoreThanDoubleTermsWithPrefixWithFilter(
	queriesMap map[string][]string, filters map[string][]interface{}, page int) []Document {
	filteredDocs := se.ApplyFilter(filters)
	exactTerms := queriesMap["exact"]
	secondTerm := queriesMap["raw"][len(queriesMap["raw"])-1]

	var finalDocs []Document

	inputTerm := exactTerms[0]

	se.mu.RLock()
	defer se.mu.RUnlock()

	startIndex := page * se.PageSize
	stopIndex := startIndex + se.PageSize

	for _, doc := range se.ScoreIndex[inputTerm] {
		if _, ok := filteredDocs[doc.ID]; !ok {
			continue
		}
		inAllExact := true
		allOtherExactWeights := 0
		for i := 1; i < len(exactTerms); i++ {
			otherExactWeight, ok := se.Data[exactTerms[i]][doc.ID]
			if !ok {
				inAllExact = false
				break
			}
			allOtherExactWeights += otherExactWeight
		}
		if !inAllExact {
			continue
		}

		totalScore := doc.ScoreWeight + allOtherExactWeights

		hasPrefix := false
		for term, _ := range se.DocData[doc.ID] {
			if strings.HasPrefix(term, secondTerm) {
				hasPrefix = true
				totalScore += se.Data[term][doc.ID]
			}
		}

		if !hasPrefix {
			continue
		}

		finalDocs = append(finalDocs, Document{
			ID:          doc.ID,
			Data:        se.Documents[doc.ID],
			ScoreWeight: totalScore,
		})
		if len(finalDocs) >= stopIndex {
			break
		}
	}

	if startIndex > len(finalDocs) {
		return nil
	}
	if stopIndex > len(finalDocs) {
		stopIndex = len(finalDocs)
	}

	return finalDocs[startIndex:stopIndex]
}

// SearchMoreThanDoubleTermsWithFilter is a public multi-term search function for with filters.
func (se *SearchEngine) SearchMoreThanDoubleTermsWithFilter(
	queriesMap map[string][]string, filters map[string][]interface{}, page int) []Document {
	filteredDocs := se.ApplyFilter(filters)
	exactTerms := queriesMap["exact"]
	otherTerms := append(queriesMap["prefix"], queriesMap["fuzzy"]...)

	se.mu.RLock()
	defer se.mu.RUnlock()
	if len(exactTerms) == 0 {
		return nil
	}

	var finalDocs []Document

	inputTerm := exactTerms[0]
	startIndex := page * se.PageSize
	stopIndex := startIndex + se.PageSize

	for _, doc := range se.ScoreIndex[inputTerm] {
		if _, ok := filteredDocs[doc.ID]; !ok {
			continue
		}
		inAllExact := true
		allOtherExactWeights := 0
		for i := 1; i < len(exactTerms); i++ {
			otherExactWeight, ok := se.Data[exactTerms[i]][doc.ID]
			if !ok {
				inAllExact = false
				break
			}
			allOtherExactWeights += otherExactWeight
		}
		if !inAllExact {
			continue
		}

		totalScore := doc.ScoreWeight + allOtherExactWeights

		foundOther := false
		otherWeight := 0
		for _, term := range otherTerms {
			if otherData, ok := se.Data[term][doc.ID]; ok {
				foundOther = true
				otherWeight += otherData
				// break
			}
		}
		if !foundOther {
			continue
		}

		totalScore += otherWeight

		finalDocs = append(finalDocs, Document{
			ID:          doc.ID,
			Data:        se.Documents[doc.ID],
			ScoreWeight: totalScore,
		})
		if len(finalDocs) >= stopIndex {
			break
		}
	}

	if startIndex > len(finalDocs) {
		return nil
	}
	if stopIndex > len(finalDocs) {
		stopIndex = len(finalDocs)
	}

	return finalDocs[startIndex:stopIndex]
}

func (se *SearchEngine) searchWithTokens(queryTokens map[string][]string, page int, filters map[string][]interface{}, tokenCount int) *SearchResult {
	if len(filters) == 0 {
		if tokenCount == 1 {
			if len(queryTokens["prefix"]) > 0 {
				var finalDocs []Document
				for _, query := range queryTokens["prefix"] {
					finalDocs = append(finalDocs, se.SearchOneTermWithoutFilter(query, page)...)
				}
				return &SearchResult{
					Docs:        finalDocs,
					IsMultiTerm: false,
					IsFuzzy:     false,
					IsPrefix:    true,
					IsExact:     false,
				}
			}
			if len(queryTokens["fuzzy"]) > 0 {
				resultDocs := se.SearchOneTermWithoutFilter(queryTokens["fuzzy"][0], page)
				return &SearchResult{
					Docs:        resultDocs,
					IsMultiTerm: false,
					IsFuzzy:     true,
					IsPrefix:    false,
					IsExact:     false,
				}
			}
		} else {
			resultDocs := se.SearchMultipleTermsWithoutFilter(queryTokens, page)
			return &SearchResult{
				Docs:        resultDocs,
				IsMultiTerm: true,
				IsFuzzy:     false,
				IsPrefix:    false,
				IsExact:     false,
			}
		}
	} else {
		if tokenCount == 1 {
			if len(queryTokens["prefix"]) > 0 {
				var finalDocs []Document
				for _, query := range queryTokens["prefix"] {
					finalDocs = append(finalDocs, se.SearchOneTermWithFilter(query, filters, page)...)
				}
				return &SearchResult{
					Docs:        finalDocs,
					IsMultiTerm: false,
					IsFuzzy:     false,
					IsPrefix:    true,
					IsExact:     false,
				}
			}
			if len(queryTokens["fuzzy"]) > 0 {
				resultDocs := se.SearchOneTermWithFilter(queryTokens["fuzzy"][0], filters, page)
				return &SearchResult{
					Docs:        resultDocs,
					IsMultiTerm: false,
					IsFuzzy:     true,
					IsPrefix:    false,
					IsExact:     false,
				}
			}
		} else {
			resultDocs := se.SearchMultipleTermsWithFilter(queryTokens, filters, page)
			return &SearchResult{
				Docs:        resultDocs,
				IsMultiTerm: true,
				IsFuzzy:     false,
				IsPrefix:    false,
				IsExact:     false,
			}
		}
	}

	return nil
}

// Search executes a full query (possibly multi-term) with optional filters,
// selecting the correct search path (exact, prefix, fuzzy, or multi-term).
func (se *SearchEngine) Search(query string, page int, filters map[string][]interface{}, prefixCount int) *SearchResult {
	queryTokensMap, tokenCount := se.ProcessQuery(query, prefixCount)

	if len(queryTokensMap["prefix"]) <= 5 {
		fmt.Println("ShardID:", se.ShardID, "ProcessQuery:", queryTokensMap)
	} else {
		fmt.Println("ShardID:", se.ShardID, "ProcessQuery Lengths:",
			len(queryTokensMap["exact"]),
			len(queryTokensMap["prefix"]),
			len(queryTokensMap["fuzzy"]))
	}

	if queryTokensMap == nil {
		return nil
	}

	result := se.searchWithTokens(queryTokensMap, page, filters, tokenCount)
	result.PrefixLength = len(queryTokensMap["prefix"])
	return result
}

// addScoreIndex inserts or updates the score entry for a document across multiple tokens,
// keeping each token’s ScoreIndex slice sorted in descending order by ScoreWeight.
func (se *SearchEngine) addScoreIndex(tokens []string, docID string) {
	for _, token := range tokens {
		se.mu.RLock()
		score := se.Data[token][docID]
		docs, ok := se.ScoreIndex[token]
		se.mu.RUnlock()
		if ok {
			// Binary search to find the correct index for descending order
			index := sort.Search(len(docs), func(i int) bool {
				return docs[i].ScoreWeight <= score
			})

			se.mu.Lock()
			se.ScoreIndex[token] = append(docs, Document{})
			copy(se.ScoreIndex[token][index+1:], docs[index:])
			se.ScoreIndex[token][index] = Document{
				ID:          docID,
				ScoreWeight: score,
				Data:        se.Documents[docID],
			}
			se.mu.Unlock()
		} else {
			se.mu.Lock()
			se.ScoreIndex[token] = []Document{{
				ID:          docID,
				ScoreWeight: score,
				Data:        se.Documents[docID],
			}}
			se.mu.Unlock()
		}
	}
}

// addDocument adds a generic document to the search engine and updates the index.
func (se *SearchEngine) addDocument(doc map[string]interface{}) {
	docID := fmt.Sprintf("%v", doc["id"])

	se.BuildDocumentIndex([]map[string]interface{}{doc})

	se.mu.Lock()
	se.Documents[docID] = make(map[string]interface{})
	for k, v := range doc {
		se.Documents[docID][k] = v
	}
	se.mu.Unlock()

	for _, weightField := range se.IndexFields {
		if value, exists := doc[weightField]; exists {
			switch v := value.(type) {
			case string:
				tokens := Tokenize(v)
				se.addScoreIndex(tokens, docID)
			case []string:
				for _, item := range v {
					tokens := Tokenize(item)
					se.addScoreIndex(tokens, docID)
				}
			case []interface{}:
				for _, item := range v {
					if str, ok := item.(string); ok {
						tokens := Tokenize(str)
						se.addScoreIndex(tokens, docID)
					}
				}
			}
		}
	}
}

// removeDocumentByID removes a document from the inverted index and the document list.
func (se *SearchEngine) removeDocumentByID(docID string) {
	se.mu.Lock()
	defer se.mu.Unlock()

	for term, docMap := range se.Data {
		if _, exists := docMap[docID]; exists {
			delete(docMap, docID)
			if len(docMap) == 0 {
				se.Keys.Remove(term)
				se.Trie.Remove(term)
				delete(se.Data, term)
			}
		}
	}

	for term, docs := range se.ScoreIndex {
		for i, doc := range docs {
			if doc.ID == docID {
				se.ScoreIndex[term] = append(se.ScoreIndex[term][:i], se.ScoreIndex[term][i+1:]...)
			}
		}
	}

	for filter, docMap := range se.FilterDocs {
		if _, exists := docMap[docID]; exists {
			delete(docMap, docID)
			if len(docMap) == 0 {
				delete(se.FilterDocs, filter)
			}
		}
	}

	delete(se.Documents, docID)
}

// Tokenize splits text into words, removes non-alphanumeric characters, and excludes stopwords.
func Tokenize(content string) []string {
	nonAlphaNumeric := regexp.MustCompile(`[^a-zA-Z0-9]+`)

	words := strings.Fields(content)
	var tokens []string
	for _, word := range words {
		word = nonAlphaNumeric.ReplaceAllString(strings.ToLower(word), "")
		if word != "" && !stopWords[word] {
			tokens = append(tokens, word)
		}
	}
	return tokens
}

// FuzzyMatch checks if two strings are within a given Levenshtein distance.
func FuzzyMatch(a, b string, maxDistance int) bool {
	return levenshteinDistance(a, b) <= maxDistance
}

// LevenshteinDistance calculates the edit distance between two strings.
func levenshteinDistance(a, b string) int {
	m, n := len(a), len(b)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	for i := 0; i <= m; i++ {
		for j := 0; j <= n; j++ {
			if i == 0 {
				dp[i][j] = j
			} else if j == 0 {
				dp[i][j] = i
			} else if a[i-1] == b[j-1] {
				dp[i][j] = dp[i-1][j-1]
			} else {
				dp[i][j] = 1 + min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1])
			}
		}
	}
	return dp[m][n]
}

// Min helper function.
func min(a, b, c int) int {
	if a < b && a < c {
		return a
	} else if b < c {
		return b
	}
	return c
}
