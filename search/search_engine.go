package search

import (
	"encoding/gob"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type Document struct {
	ID          string
	ScoreWeight int
}

type SearchResult struct {
	Docs        []Document
	IsMultiTerm bool
	IsExact     bool
	IsPrefix    bool
	IsFuzzy     bool
}

type TokenFieldPair struct {
	Token string
	Field string
}

// Predefined list of stopwords
var stopWords = map[string]bool{
	"is": true, "a": true, "the": true, "and": true, "or": true, "to": true, "in": true, "of": true,
}

// bundle up just the fields we want to persist
type enginePayload struct {
	Data             map[string]map[string]int
	Documents        map[string]map[string]interface{}
	ScoreIndex       map[string][]Document
	FilterDocs       map[string]map[string]bool
	IndexFields      []string
	Filters          map[string]bool
	PageSize         int
	TotalFieldCounts map[string]int
	Keys             *Keys
	Trie             *Trie
}

// SearchEngine holds documents, the inverted index, and weights configuration.
type SearchEngine struct {
	ShardID          int
	Data             map[string]map[string]int         // term -> docID -> int
	Documents        map[string]map[string]interface{} // docID -> fields
	ScoreIndex       map[string][]Document             // term -> Documents (ordered by weight desc)
	FilterDocs       map[string]map[string]bool        // filter -> docID e.g., map["year:2019"]map["doc1"] = true
	Keys             *Keys
	Trie             *Trie
	IndexFields      []string
	Filters          map[string]bool // filters for fields
	PageSize         int             // size of the documents in a search response.
	TotalFieldCounts map[string]int  // cumulative sum of token/counts per field
	mu               sync.RWMutex
}

// NewSearchEngine initializes a new search engine with custom weights.
func NewSearchEngine(
	indexFields []string,
	filters map[string]bool,
	pageSize, shardID int) *SearchEngine {
	return &SearchEngine{
		ShardID:     shardID,
		Data:        make(map[string]map[string]int),
		Documents:   make(map[string]map[string]interface{}),
		IndexFields: indexFields,
		Filters:     filters,
		ScoreIndex:  make(map[string][]Document),
		Keys:        NewKeys(),
		Trie:        NewTrie(),
		FilterDocs:  make(map[string]map[string]bool),
		PageSize:    pageSize,
	}
}

func init() {
	gob.Register(enginePayload{})
	gob.Register(Document{})
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register(&Trie{})
	gob.Register(&TrieNode{})
}

// SaveAll persists the entire SearchEngine under three files:
//
//	<prefix>.engine.gob   – enginePayload
//	<prefix>.keys.gob     – k.Data
//	<prefix>.trie.gob     – t.Root/children
func (se *SearchEngine) SaveAll(path string) error {
	// 1) save the core engine fields
	se.mu.RLock()
	payload := enginePayload{
		Data:             se.Data,
		Documents:        se.Documents,
		ScoreIndex:       se.ScoreIndex,
		FilterDocs:       se.FilterDocs,
		IndexFields:      se.IndexFields,
		Filters:          se.Filters,
		PageSize:         se.PageSize,
		TotalFieldCounts: se.TotalFieldCounts,
		Keys:             se.Keys,
		Trie:             se.Trie,
	}
	se.mu.RUnlock()

	// engine state
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

	// // 2) save keys
	// keysFile := prefix + ".keys.gob"
	// if err := se.Keys.Save(keysFile); err != nil {
	// 	return fmt.Errorf("save keys: %w", err)
	// }

	// // 3) save trie
	// trieFile := prefix + ".trie.gob"
	// if err := se.Trie.Save(trieFile); err != nil {
	// 	return fmt.Errorf("save trie: %w", err)
	// }

	return nil
}

// LoadAll restores a SearchEngine from three files with the given prefix.
// If any file is missing, it initializes that piece fresh and continues.
func LoadAll(path string) (*SearchEngine, error) {
	se := &SearchEngine{
		Data:             make(map[string]map[string]int),
		Documents:        make(map[string]map[string]interface{}),
		ScoreIndex:       make(map[string][]Document),
		FilterDocs:       make(map[string]map[string]bool),
		Keys:             NewKeys(),
		Trie:             NewTrie(),
		IndexFields:      nil,
		Filters:          make(map[string]bool),
		PageSize:         0,
		TotalFieldCounts: make(map[string]int),
	}

	// 1) engine payload
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
		se.Documents = payload.Documents
		se.ScoreIndex = payload.ScoreIndex
		se.FilterDocs = payload.FilterDocs
		se.IndexFields = payload.IndexFields
		se.Filters = payload.Filters
		se.PageSize = payload.PageSize
		se.TotalFieldCounts = payload.TotalFieldCounts
		se.Keys = payload.Keys
		se.Trie = payload.Trie
	}

	return se, nil
}

func (se *SearchEngine) Index(shardID int, docs []map[string]interface{}) {
	fmt.Println("BuildDocumentIndex starting...")
	start := time.Now()
	se.BuildDocumentIndex(docs)
	duration := time.Since(start)
	fmt.Printf("BuildDocumentIndex took: %s\n", duration)

	fmt.Println("BuildScoreIndex starting...")
	start = time.Now()
	se.BuildScoreIndex()
	duration = time.Since(start)
	fmt.Printf("BuildScoreIndex took: %s\n", duration)

	fmt.Println("Insert documents starting...")
	start = time.Now()
	se.InsertDocs(docs)
	duration = time.Since(start)
	fmt.Printf("Insert documents took: %s\n", duration)
}

// ProcessQuery analyzes a search query string and categorizes tokens based on match type.
// It returns a map where the keys represent the match category:
// - "exact": tokens that exactly match entries in the GlobalKeys store.
// - "prefix": tokens that match prefixes found in the GlobalTrie.
// - "fuzzy": tokens that approximately match entries in GlobalKeys within a given edit distance.
func (sec *SearchEngine) ProcessQuery(query string) map[string][]string {
	queryTokens := tokenize(query)
	if len(queryTokens) == 0 {
		return nil
	}

	result := make(map[string][]string)

	for _, query := range queryTokens {
		if _, exists := sec.Keys.GetData()[query]; exists {
			result["exact"] = append(result["exact"], query)
			continue
		} else {
			guessArr := sec.Trie.SearchPrefix(query)
			if guessArr != nil {
				result["prefix"] = append(result["prefix"], guessArr...)
			} else {
				fuzzyMatch := false
				for key := range sec.Keys.GetData() {
					if FuzzyMatch(key, query, 2) {
						result["fuzzy"] = append(result["fuzzy"], key)
						fuzzyMatch = true
						break
					}
				}
				if !fuzzyMatch {
					result["fuzzy"] = append(result["fuzzy"], query)
				}
			}
		}
	}

	return result
}

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
					tempArr = append(tempArr, Document{ID: docID, ScoreWeight: weight})
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

func (se *SearchEngine) BuildDocumentIndex(docs []map[string]interface{}) {
	for i, doc := range docs {
		if i%100_000 == 0 || i == len(docs)-1 {
			fmt.Println("BuildDocumentIndex Document:", i)
		}

		docID := fmt.Sprintf("%v", doc["id"])
		se.mu.RLock()
		_, ok := se.Documents[docID]
		se.mu.RUnlock()
		if ok {
			fmt.Println("Document already exist, removing and readding, DocID:", i)
			se.removeDocumentByID(docID)
		}

		var allTokens []string
		for _, weightField := range se.IndexFields {
			if value, exists := doc[weightField]; exists {
				switch v := value.(type) {
				case string:
					tokens := tokenize(v)
					allTokens = append(allTokens, tokens...)
				case []string:
					for _, item := range v {
						tokens := tokenize(item)
						allTokens = append(allTokens, tokens...)
					}
				case []interface{}:
					for _, item := range v {
						if str, ok := item.(string); ok {
							tokens := tokenize(str)
							allTokens = append(allTokens, tokens...)
						}
					}
				}
			}
		}

		for _, token := range allTokens {
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

func (se *SearchEngine) addToDocumentIndex(
	term, docID string, length int,
) {
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

func (se *SearchEngine) searchAllWithoutFilter(exactTerms, otherTerms []string, page int) []Document {
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
		for i := 1; i < len(exactTerms); i++ {
			if _, ok := se.Data[exactTerms[i]][doc.ID]; !ok {
				inAllExact = false
				break
			}
		}
		if !inAllExact {
			continue
		}

		if len(otherTerms) > 0 {
			foundOther := false
			for _, term := range otherTerms {
				if _, ok := se.Data[term][doc.ID]; ok {
					foundOther = true
					break
				}
			}
			if !foundOther {
				continue
			}
		}

		totalScore := doc.ScoreWeight

		for i := 1; i < len(exactTerms); i++ {
			weight := se.Data[exactTerms[i]][doc.ID]
			totalScore += weight
		}

		for _, term := range otherTerms {
			weight := se.Data[term][doc.ID]
			totalScore += weight
		}

		finalDocs = append(finalDocs, Document{
			ID:          doc.ID,
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

func (se *SearchEngine) searchAllWithFilter(exactTerms, otherTerms []string, filteredDocs map[string]bool, page int) []Document {
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
		for i := 1; i < len(exactTerms); i++ {
			if _, ok := se.Data[exactTerms[i]][doc.ID]; !ok {
				inAllExact = false
				break
			}
		}
		if !inAllExact {
			continue
		}

		if len(otherTerms) > 0 {
			foundOther := false
			for _, term := range otherTerms {
				if _, ok := se.Data[term][doc.ID]; ok {
					foundOther = true
					break
				}
			}
			if !foundOther {
				continue
			}
		}

		totalScore := doc.ScoreWeight

		for i := 1; i < len(exactTerms); i++ {
			weight := se.Data[exactTerms[i]][doc.ID]
			totalScore += weight
		}

		for _, term := range otherTerms {
			weight := se.Data[term][doc.ID]
			totalScore += weight
		}

		finalDocs = append(finalDocs, Document{
			ID:          doc.ID,
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

func (se *SearchEngine) SearchMultipleTermsWithoutFilter(queriesMap map[string][]string, page int) []Document {
	result := se.searchAllWithoutFilter(queriesMap["exact"], append(queriesMap["prefix"], queriesMap["fuzzy"]...), page)
	return result
}

func (se *SearchEngine) SearchMultipleTermsWithFilter(queriesMap map[string][]string, filters map[string][]interface{}, page int) []Document {
	filteredDocs := se.ApplyFilter(filters)
	result := se.searchAllWithFilter(queriesMap["exact"], append(queriesMap["prefix"], queriesMap["fuzzy"]...), filteredDocs, page)
	return result
}

func (se *SearchEngine) Search(query string, page int, filters map[string][]interface{}) *SearchResult {
	queryTokens := se.ProcessQuery(query)
	fmt.Println("ProcessQuery:", queryTokens)
	if queryTokens == nil {
		return nil
	}

	if len(filters) == 0 {
		if len(queryTokens["exact"]) == 1 &&
			len(queryTokens["prefix"]) == 0 &&
			len(queryTokens["fuzzy"]) == 0 {
			resultDocs := se.SearchOneTermWithoutFilter(queryTokens["exact"][0], page)
			return &SearchResult{
				Docs:        resultDocs,
				IsMultiTerm: false,
				IsFuzzy:     false,
				IsPrefix:    false,
				IsExact:     true,
			}
		}

		if len(queryTokens["exact"]) == 0 {
			if len(queryTokens["prefix"]) > 0 {
				resultDocs := se.SearchOneTermWithoutFilter(queryTokens["prefix"][0], page)
				return &SearchResult{
					Docs:        resultDocs,
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
		}

		resultDocs := se.SearchMultipleTermsWithoutFilter(queryTokens, page)
		return &SearchResult{
			Docs:        resultDocs,
			IsMultiTerm: true,
			IsFuzzy:     false,
			IsPrefix:    false,
			IsExact:     true,
		}
	} else {
		if len(queryTokens["exact"]) == 1 &&
			len(queryTokens["prefix"]) == 0 &&
			len(queryTokens["fuzzy"]) == 0 {
			resultDocs := se.SearchOneTermWithFilter(queryTokens["exact"][0], filters, page)
			return &SearchResult{
				Docs:        resultDocs,
				IsMultiTerm: false,
				IsFuzzy:     false,
				IsPrefix:    false,
				IsExact:     true,
			}
		}

		if len(queryTokens["exact"]) == 0 {
			if len(queryTokens["prefix"]) > 0 {
				resultDocs := se.SearchOneTermWithFilter(queryTokens["prefix"][0], filters, page)
				return &SearchResult{
					Docs:        resultDocs,
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
		}

		resultDocs := se.SearchMultipleTermsWithFilter(queryTokens, filters, page)
		return &SearchResult{
			Docs:        resultDocs,
			IsMultiTerm: true,
			IsFuzzy:     false,
			IsPrefix:    false,
			IsExact:     true,
		}
	}
}

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
			}
			se.mu.Unlock()
		} else {
			se.mu.Lock()
			se.ScoreIndex[token] = []Document{{
				ID:          docID,
				ScoreWeight: score,
			}}
			se.mu.Unlock()
		}
	}
}

// addDocument adds a generic document to the search engine and updates the index.
func (se *SearchEngine) addDocument(doc map[string]interface{}) {
	docID := fmt.Sprintf("%v", doc["id"])

	se.BuildDocumentIndex([]map[string]interface{}{doc})

	for _, weightField := range se.IndexFields {
		if value, exists := doc[weightField]; exists {
			switch v := value.(type) {
			case string:
				tokens := tokenize(v)
				se.addScoreIndex(tokens, docID)
			case []string:
				for _, item := range v {
					tokens := tokenize(item)
					se.addScoreIndex(tokens, docID)
				}
			case []interface{}:
				for _, item := range v {
					if str, ok := item.(string); ok {
						tokens := tokenize(str)
						se.addScoreIndex(tokens, docID)
					}
				}
			}
		}
	}

	se.mu.Lock()
	se.Documents[docID] = make(map[string]interface{})
	for k, v := range doc {
		se.Documents[docID][k] = v
	}
	se.mu.Unlock()
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
