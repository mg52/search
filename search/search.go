package search

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
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

// Predefined list of stopwords
var stopWords = map[string]bool{
	"is": true, "a": true, "the": true, "and": true, "or": true, "to": true, "in": true, "of": true,
}

type SearchEngineController struct {
	Engines           []*SearchEngine
	SearchEngineCount int
	Weights           map[string]int  // weights for fields
	Filters           map[string]bool // filters for fields
	PageSize          int
}

func NewSearchEngineController(weights map[string]int,
	filters map[string]bool,
	pageSize,
	searchEngineCount int) *SearchEngineController {
	var seList []*SearchEngine
	for i := 0; i < searchEngineCount; i++ {
		se := NewSearchEngine(weights, filters, pageSize)
		seList = append(seList, se)
	}
	return &SearchEngineController{
		Engines:           seList,
		SearchEngineCount: searchEngineCount,
		Weights:           weights,
		Filters:           filters,
		PageSize:          pageSize,
	}
}

// TODO: check for race conditions while indexing and searching
func (sec *SearchEngineController) Index(docs []map[string]interface{}) {
	chunkSize := (len(docs) + sec.SearchEngineCount - 1) / sec.SearchEngineCount
	for i := 0; i < sec.SearchEngineCount; i++ {
		dataStart := chunkSize * i
		if dataStart >= len(docs) {
			break
		}
		dataEnd := dataStart + chunkSize
		if dataEnd > len(docs) {
			dataEnd = len(docs)
		}

		sec.Engines[i].Index(i, docs[dataStart:dataEnd])
	}
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
				for key := range sec.Keys.GetData() {
					if FuzzyMatch(key, query, 2) {
						result["fuzzy"] = append(result["fuzzy"], key)
						break
					}
				}
			}
		}
	}

	return result
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
			_ = se.RemoveDocumentByID(docID)
		}(engine)
	}
	wg.Wait()
}

func (sec *SearchEngineController) AddOrUpdateDocument(doc map[string]interface{}) {
	docID := fmt.Sprintf("%v", doc["id"])
	isExist := false
	for _, engine := range sec.Engines {
		if _, ok := engine.Documents[docID]; ok {
			isExist = true
			break
		}
	}

	if isExist {
		sec.updateDocument(doc)
	} else {
		sec.addDocument(doc)
	}
}

func (sec *SearchEngineController) addDocument(doc map[string]interface{}) error {
	luckyShard := rand.Intn(len(sec.Engines))

	err := sec.Engines[luckyShard].addDocument(doc)
	return err
}

func (sec *SearchEngineController) updateDocument(doc map[string]interface{}) error {
	var wg sync.WaitGroup

	for _, engine := range sec.Engines {
		wg.Add(1)
		go func(se *SearchEngine) {
			defer wg.Done()
			_ = se.updateDocument(doc)
		}(engine)
	}

	wg.Wait()

	return nil
}

// TODO: saving index into disk?
// TODO: because we use 3 prefix result, we are missing some important matchs
// e.g., search by "skill smell le"
// there is a data skill smell learn, but it is missed since learn won't be predicted by searching le
// SearchEngine holds documents, the inverted index, and weights configuration.
type SearchEngine struct {
	Data               map[string]map[string]int // term -> docID -> int
	AverageFieldCounts map[string]int
	Documents          map[string]map[string]interface{} // docID -> fields
	ScoreIndex         map[string][]Document             // term -> Documents (ordered by weight desc)
	FilterDocs         map[string]map[string]bool        // filter -> docID e.g., map["year:2019"]map["doc1"] = true
	Keys               *Keys
	Trie               *Trie
	Weights            map[string]int  // weights for fields
	Filters            map[string]bool // filters for fields
	PageSize           int             // size of the documents in a search response.
	mu                 sync.RWMutex
}

// NewSearchEngine initializes a new search engine with custom weights.
func NewSearchEngine(
	weights map[string]int,
	filters map[string]bool,
	pageSize int) *SearchEngine {
	return &SearchEngine{
		Data:               make(map[string]map[string]int),
		Documents:          make(map[string]map[string]interface{}),
		Weights:            weights,
		Filters:            filters,
		AverageFieldCounts: make(map[string]int),
		ScoreIndex:         make(map[string][]Document),
		Keys:               NewKeys(),
		Trie:               NewTrie(),
		FilterDocs:         make(map[string]map[string]bool),
		PageSize:           pageSize,
	}
}

func (se *SearchEngine) CalculateAverageFieldCounts(docs []map[string]interface{}) {
	for _, doc := range docs {
		for field := range se.Weights {
			if value, exists := doc[field]; exists {
				switch v := value.(type) {
				case string:
					tokens := tokenize(v)
					se.AverageFieldCounts[field] += len(tokens)
				case []string:
					se.AverageFieldCounts[field] += len(v)
				case []interface{}:
					se.AverageFieldCounts[field] += len(v)
				}
			}
		}
	}
	for k := range se.AverageFieldCounts {
		se.AverageFieldCounts[k] = se.AverageFieldCounts[k] / len(docs)
		if se.AverageFieldCounts[k]%len(docs) != 0 {
			se.AverageFieldCounts[k] += 1
		}
	}
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

	// fmt.Println("SaveMapToFile starting...")
	// start = time.Now()
	// SaveMapToFile(se.Data, "sedata", shardID)
	// duration = time.Since(start)
	// fmt.Printf("SaveMapToFile took: %s\n", duration)
}

func SaveMapToFile(m map[string]map[string]int, path string, shardID int) error {
	file, err := os.Create(fmt.Sprintf("%s-%d", path, shardID))
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(m)
}

// LoadMapFromFile opens the given file, decodes its gob contents,
// and returns the map. If anything goes wrong, an error is returned.
func LoadMapFromFile(path string, shardID int) (map[string]map[string]int, error) {
	file, err := os.Open(fmt.Sprintf("%s-%d", path, shardID))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	var m map[string]map[string]int
	if err := decoder.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
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
		if _, ok := se.Documents[docID]; ok {
			// already indexed document
			se.mu.RUnlock()
			continue
		}
		se.mu.RUnlock()

		for field, weight := range se.Weights {
			if value, exists := doc[field]; exists {
				switch v := value.(type) {
				case string:
					tokens := tokenize(v)
					for k, token := range tokens {
						se.addToDocumentIndex(token, docID, field, weight, k, len(tokens))
					}
				case []string:
					for k, item := range v {
						tokens := tokenize(item)
						for _, token := range tokens {
							se.addToDocumentIndex(token, docID, field, weight, k, len(v))
						}
					}
				case []interface{}:
					for k, item := range v {
						if str, ok := item.(string); ok {
							tokens := tokenize(str)
							for _, token := range tokens {
								se.addToDocumentIndex(token, docID, field, weight, k, len(v))
							}
						}
					}
				}
			}
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

func (se *SearchEngine) addToDocumentIndex(term, docID, field string, weight, index, length int) {
	normalizedWeight := 100 / length

	se.mu.Lock()
	defer se.mu.Unlock()

	docMap, exists := se.Data[term]
	if !exists {
		se.Keys.Insert(term)
		se.Trie.Insert(term)
		docMap = make(map[string]int)
		se.Data[term] = docMap
	}
	if docMap[docID] == 0 {
		docMap[docID] = 1_000_000
	}
	if weight == 2 {
		docMap[docID] += (normalizedWeight * 1000)
	} else {
		docMap[docID] += normalizedWeight
	}
}

// Tokenize splits text into words, removes non-alphanumeric characters, and excludes stopwords.
func tokenize(content string) []string {
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

func (se *SearchEngine) searchAll(exactTerms, otherTerms []string) []Document {
	se.mu.RLock()
	defer se.mu.RUnlock()
	if len(exactTerms) == 0 {
		return nil
	}

	var finalImportantDocs []Document
	var finalDocs []Document

	inputTerm := exactTerms[0]
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

		qualifiesImportant := true
		hasOtherImportant := false
		sumExact := 0

		importantWeight := (doc.ScoreWeight / 1_000) % 1_000
		normalWeight := doc.ScoreWeight % 1_000
		if importantWeight > 0 {
			sumExact += 2*importantWeight + normalWeight
		} else {
			qualifiesImportant = false
			sumExact += normalWeight
		}

		for i := 1; i < len(exactTerms); i++ {
			weight := se.Data[exactTerms[i]][doc.ID]
			importantWeight := (weight / 1_000) % 1_000
			normalWeight := weight % 1_000
			if importantWeight > 0 {
				sumExact += 2*importantWeight + normalWeight
			} else {
				qualifiesImportant = false
				sumExact += normalWeight
			}
		}

		sumOther := 0
		for _, term := range otherTerms {
			if weight, ok := se.Data[term][doc.ID]; ok {
				importantWeight := (weight / 1_000) % 1_000
				normalWeight := weight % 1_000
				if importantWeight > 0 {
					hasOtherImportant = true
					sumOther += 2*importantWeight + normalWeight
				} else {
					sumOther += normalWeight
				}
			}
		}

		if len(otherTerms) > 0 && !hasOtherImportant {
			qualifiesImportant = false
		}

		totalScore := sumExact + sumOther

		if qualifiesImportant {
			finalImportantDocs = append(finalImportantDocs, Document{
				ID:          doc.ID,
				ScoreWeight: totalScore * 1000,
			})
		} else {
			finalDocs = append(finalDocs, Document{
				ID:          doc.ID,
				ScoreWeight: totalScore,
			})
		}
	}
	return append(finalImportantDocs, finalDocs...)
}

func (se *SearchEngine) searchAllWithFilter(exactTerms, otherTerms []string, filteredDocs map[string]bool) []Document {
	se.mu.RLock()
	defer se.mu.RUnlock()
	if len(exactTerms) == 0 {
		return nil
	}

	var finalImportantDocs []Document
	var finalDocs []Document

	inputTerm := exactTerms[0]
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

		qualifiesImportant := true
		hasOtherImportant := false
		sumExact := 0

		importantWeight := (doc.ScoreWeight / 1_000) % 1_000
		normalWeight := doc.ScoreWeight % 1_000
		if importantWeight > 0 {
			sumExact += 2*importantWeight + normalWeight
		} else {
			qualifiesImportant = false
			sumExact += normalWeight
		}

		for i := 1; i < len(exactTerms); i++ {
			weight := se.Data[exactTerms[i]][doc.ID]
			importantWeight := (weight / 1_000) % 1_000
			normalWeight := weight % 1_000
			if importantWeight > 0 {
				sumExact += 2*importantWeight + normalWeight
			} else {
				qualifiesImportant = false
				sumExact += normalWeight
			}
		}

		sumOther := 0
		for _, term := range otherTerms {
			if weight, ok := se.Data[term][doc.ID]; ok {
				importantWeight := (weight / 1_000) % 1_000
				normalWeight := weight % 1_000
				if importantWeight > 0 {
					hasOtherImportant = true
					sumOther += 2*importantWeight + normalWeight
				} else {
					sumOther += normalWeight
				}
			}
		}

		if len(otherTerms) > 0 && !hasOtherImportant {
			qualifiesImportant = false
		}

		totalScore := sumExact + sumOther

		if qualifiesImportant {
			finalImportantDocs = append(finalImportantDocs, Document{
				ID:          doc.ID,
				ScoreWeight: totalScore * 100,
			})
		} else {
			finalDocs = append(finalDocs, Document{
				ID:          doc.ID,
				ScoreWeight: totalScore,
			})
		}
	}
	// TODO: Add pagination
	return append(finalImportantDocs, finalDocs...)
}

func (se *SearchEngine) searchAllParallel(exactTerms, otherTerms []string) []Document {
	if len(exactTerms) == 0 {
		return nil
	}

	var finalImportantDocs []Document
	var finalDocs []Document

	inputTerm := exactTerms[0]

	var wg sync.WaitGroup
	var mu sync.Mutex
	workerCount := runtime.NumCPU()
	docChan := make(chan Document, workerCount)

	workerFn := func() {
		defer wg.Done()
		var localImportant []Document
		var localNormal []Document

		for doc := range docChan {
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

			qualifiesImportant := true
			hasOtherImportant := false
			sumExact := 0

			importantWeight := (doc.ScoreWeight / 1_000) % 1_000
			normalWeight := doc.ScoreWeight % 1_000
			if importantWeight > 0 {
				sumExact += 2*importantWeight + normalWeight
			} else {
				qualifiesImportant = false
				sumExact += normalWeight
			}

			for i := 1; i < len(exactTerms); i++ {
				weight := se.Data[exactTerms[i]][doc.ID]
				importantWeight := (weight / 1_000) % 1_000
				normalWeight := weight % 1_000
				if importantWeight > 0 {
					sumExact += 2*importantWeight + normalWeight
				} else {
					qualifiesImportant = false
					sumExact += normalWeight
				}
			}

			sumOther := 0
			for _, term := range otherTerms {
				if weight, ok := se.Data[term][doc.ID]; ok {
					importantWeight := (weight / 1_000) % 1_000
					normalWeight := weight % 1_000
					if importantWeight > 0 {
						hasOtherImportant = true
						sumOther += 2*importantWeight + normalWeight
					} else {
						sumOther += normalWeight
					}
				}
			}

			if len(otherTerms) > 0 && !hasOtherImportant {
				qualifiesImportant = false
			}

			totalScore := sumExact + sumOther

			if qualifiesImportant {
				finalImportantDocs = append(finalImportantDocs, Document{
					ID:          doc.ID,
					ScoreWeight: totalScore * 100,
				})
			} else {
				finalDocs = append(finalDocs, Document{
					ID:          doc.ID,
					ScoreWeight: totalScore,
				})
			}
		}

		mu.Lock()
		finalImportantDocs = append(finalImportantDocs, localImportant...)
		finalDocs = append(finalDocs, localNormal...)
		mu.Unlock()
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go workerFn()
	}

	for _, doc := range se.ScoreIndex[inputTerm] {
		docChan <- doc
	}
	close(docChan)
	wg.Wait()

	return append(finalImportantDocs, finalDocs...)
}

func (se *SearchEngine) searchAllParallelWithFilter(exactTerms, otherTerms []string, filteredDocs map[string]bool) []Document {
	if len(exactTerms) == 0 {
		return nil
	}

	var finalImportantDocs []Document
	var finalDocs []Document

	inputTerm := exactTerms[0]

	var wg sync.WaitGroup
	var mu sync.Mutex
	workerCount := runtime.NumCPU()
	docChan := make(chan Document, workerCount)

	workerFn := func() {
		defer wg.Done()
		var localImportant []Document
		var localNormal []Document

		for doc := range docChan {
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

			qualifiesImportant := true
			hasOtherImportant := false
			sumExact := 0

			importantWeight := (doc.ScoreWeight / 1_000) % 1_000
			normalWeight := doc.ScoreWeight % 1_000
			if importantWeight > 0 {
				sumExact += 2*importantWeight + normalWeight
			} else {
				qualifiesImportant = false
				sumExact += normalWeight
			}

			for i := 1; i < len(exactTerms); i++ {
				weight := se.Data[exactTerms[i]][doc.ID]
				importantWeight := (weight / 1_000) % 1_000
				normalWeight := weight % 1_000
				if importantWeight > 0 {
					sumExact += 2*importantWeight + normalWeight
				} else {
					qualifiesImportant = false
					sumExact += normalWeight
				}
			}

			sumOther := 0
			for _, term := range otherTerms {
				if weight, ok := se.Data[term][doc.ID]; ok {
					importantWeight := (weight / 1_000) % 1_000
					normalWeight := weight % 1_000
					if importantWeight > 0 {
						hasOtherImportant = true
						sumOther += 2*importantWeight + normalWeight
					} else {
						sumOther += normalWeight
					}
				}
			}

			if len(otherTerms) > 0 && !hasOtherImportant {
				qualifiesImportant = false
			}

			totalScore := sumExact + sumOther

			if qualifiesImportant {
				finalImportantDocs = append(finalImportantDocs, Document{
					ID:          doc.ID,
					ScoreWeight: totalScore * 100,
				})
			} else {
				finalDocs = append(finalDocs, Document{
					ID:          doc.ID,
					ScoreWeight: totalScore,
				})
			}
		}

		mu.Lock()
		finalImportantDocs = append(finalImportantDocs, localImportant...)
		finalDocs = append(finalDocs, localNormal...)
		mu.Unlock()
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go workerFn()
	}

	for _, doc := range se.ScoreIndex[inputTerm] {
		docChan <- doc
	}
	close(docChan)
	wg.Wait()

	return append(finalImportantDocs, finalDocs...)
}

func (se *SearchEngine) SearchMultipleTermsWithoutFilter(queriesMap map[string][]string, page int) []Document {
	result := se.searchAll(queriesMap["exact"], append(queriesMap["prefix"], queriesMap["fuzzy"]...))
	// TODO: Pagination can be added here.
	lastIndex := page * se.PageSize
	if lastIndex >= len(result) {
		return nil
	}
	stop := lastIndex + se.PageSize
	if stop > len(result) {
		stop = len(result)
	}
	return result[lastIndex:stop]
}

func (se *SearchEngine) SearchMultipleTermsWithFilter(queriesMap map[string][]string, filters map[string][]interface{}, page int) []Document {
	filteredDocs := se.ApplyFilter(filters)
	result := se.searchAllWithFilter(queriesMap["exact"], append(queriesMap["prefix"], queriesMap["fuzzy"]...), filteredDocs)
	// TODO: Pagination can be added here.
	lastIndex := page * se.PageSize
	if lastIndex >= len(result) {
		return nil
	}
	stop := lastIndex + se.PageSize
	if stop > len(result) {
		stop = len(result)
	}
	return result[lastIndex:stop]
}

// OneTermSearch performs a single-term search across the current shard of the search engine.
// It supports optional filters and categorizes the term by match type (exact, prefix, or fuzzy).
//
// The function prioritizes matches in the following order: exact > prefix > fuzzy.
// It also checks that all query terms exist in the shard before proceeding.
//
// Parameters:
//   - queryTokens (map[string][]string): A map containing categorized query terms under the keys
//     "exact", "prefix", and "fuzzy".
//   - page (int): The page number for paginated results.
//   - filters (map[string][]interface{}): A map of filters where each key is a field name and the value is a list of accepted values.
//
// Returns:
// - []Document: A list of documents matching the query and filters. Returns nil if no match is possible or keys don't exist in the shard.
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

func (se *SearchEngine) addOrUpdateScoreIndex(tokens []string, docID string) {
	se.mu.Lock()
	defer se.mu.Unlock()
	for _, token := range tokens {
		if docs, ok := se.ScoreIndex[token]; ok {
			isAlreadyAdded := false
			for _, doc := range docs {
				if doc.ID == docID {
					isAlreadyAdded = true
					break
				}
			}

			if isAlreadyAdded {
				continue
			}
			score := se.Data[token][docID]
			// Binary search to find the correct index for descending order
			index := sort.Search(len(docs), func(i int) bool {
				return docs[i].ScoreWeight <= score
			})

			se.ScoreIndex[token] = append(docs, Document{})
			copy(se.ScoreIndex[token][index+1:], docs[index:])
			se.ScoreIndex[token][index] = Document{
				ID:          docID,
				ScoreWeight: score,
			}
		} else {
			se.ScoreIndex[token] = []Document{{
				ID:          docID,
				ScoreWeight: se.Data[token][docID],
			}}
		}
	}
}

// addDocument adds a generic document to the search engine and updates the index.
func (se *SearchEngine) addDocument(doc map[string]interface{}) error {
	docID := fmt.Sprintf("%v", doc["id"])

	se.mu.RLock()
	if _, ok := se.Documents[docID]; ok {
		se.mu.RUnlock()
		return fmt.Errorf("Document %s is already added to the search engine", docID)
	}
	se.mu.RUnlock()

	se.BuildDocumentIndex([]map[string]interface{}{doc})

	for field := range se.Weights {
		if value, exists := doc[field]; exists {
			switch v := value.(type) {
			case string:
				tokens := tokenize(v)
				se.addOrUpdateScoreIndex(tokens, docID)
			case []string:
				for _, item := range v {
					tokens := tokenize(item)
					se.addOrUpdateScoreIndex(tokens, docID)
				}
			case []interface{}:
				for _, item := range v {
					if str, ok := item.(string); ok {
						tokens := tokenize(str)
						se.addOrUpdateScoreIndex(tokens, docID)
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

	return nil
}

// updateDocument updates a generic document to the search engine and updates the index.
func (se *SearchEngine) updateDocument(doc map[string]interface{}) error {
	docID := fmt.Sprintf("%v", doc["id"])

	se.mu.RLock()
	if _, ok := se.Documents[docID]; !ok {
		se.mu.RUnlock()
		return fmt.Errorf("Document %s does not exist in this Search Engine", docID)
	}
	se.mu.RUnlock()

	_ = se.RemoveDocumentByID(docID)

	se.BuildDocumentIndex([]map[string]interface{}{doc})

	for field := range se.Weights {
		if value, exists := doc[field]; exists {
			switch v := value.(type) {
			case string:
				tokens := tokenize(v)
				se.addOrUpdateScoreIndex(tokens, docID)
			case []string:
				for _, item := range v {
					tokens := tokenize(item)
					se.addOrUpdateScoreIndex(tokens, docID)
				}
			case []interface{}:
				for _, item := range v {
					if str, ok := item.(string); ok {
						tokens := tokenize(str)
						se.addOrUpdateScoreIndex(tokens, docID)
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

	return nil
}

// removeDocumentByID removes a document from the inverted index and the document list.
func (se *SearchEngine) RemoveDocumentByID(docID string) error {
	se.mu.Lock()
	defer se.mu.Unlock()

	if _, ok := se.Documents[docID]; !ok {
		return fmt.Errorf("Document %s does not exist in this Search Engine", docID)
	}

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

	return nil
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
