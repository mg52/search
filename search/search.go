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

type TrieNode struct {
	children    map[rune]*TrieNode
	childrenArr []rune
	isEnd       bool
}

type Trie struct {
	root *TrieNode
	mu   sync.RWMutex
}

func NewTrie() *Trie {
	return &Trie{root: &TrieNode{children: make(map[rune]*TrieNode)}}
}

func (t *Trie) Insert(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	node := t.root
	for _, ch := range key {
		if _, exists := node.children[ch]; !exists {
			node.children[ch] = &TrieNode{children: make(map[rune]*TrieNode)}
			node.childrenArr = append(node.childrenArr, ch)
		}
		node = node.children[ch]
	}
	node.isEnd = true
}

func (t *Trie) SearchPrefix(prefix string) []string {
	node := t.root
	for _, ch := range prefix {
		if _, exists := node.children[ch]; !exists {
			return nil
		}
		node = node.children[ch]
	}

	var results []string
	t.collectWords(node, prefix, &results)
	return results
}

func (t *Trie) collectWords(node *TrieNode, prefix string, results *[]string) {
	if node.isEnd {
		*results = append(*results, prefix)
	}
	for _, ch := range node.childrenArr {
		if len(*results) >= 5 {
			break
		}
		t.collectWords(node.children[ch], prefix+string(ch), results)
	}
}

// Remove deletes `key` from the trie.
// Returns an error if `key` was not found.
func (t *Trie) Remove(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	runes := []rune(key)
	node := t.root
	for _, ch := range runes {
		child, ok := node.children[ch]
		if !ok {
			return fmt.Errorf("key %q not found in trie", key)
		}
		node = child
	}
	if !node.isEnd {
		return fmt.Errorf("key %q not found in trie", key)
	}

	t.removeNode(t.root, runes, 0)
	return nil
}

// removeNode walks down to depth, unmarks or deletes, and
// returns true if the caller should delete its reference
func (t *Trie) removeNode(node *TrieNode, runes []rune, depth int) bool {
	if depth == len(runes) {
		node.isEnd = false
	} else {
		ch := runes[depth]
		child := node.children[ch]
		if shouldDelete := t.removeNode(child, runes, depth+1); shouldDelete {
			delete(node.children, ch)
			for i, c := range node.childrenArr {
				if c == ch {
					node.childrenArr = append(node.childrenArr[:i], node.childrenArr[i+1:]...)
					break
				}
			}
		}
	}
	return !node.isEnd && len(node.children) == 0
}

// Update renames a key: it removes oldKey (erroring if missing) and inserts newKey.
func (t *Trie) Update(oldKey, newKey string) error {
	if err := t.Remove(oldKey); err != nil {
		return err
	}
	t.Insert(newKey)
	return nil
}

type Keys struct {
	data map[string]struct{}
	mu   sync.RWMutex
}

func NewKeys() *Keys {
	return &Keys{data: make(map[string]struct{})}
}

func (k *Keys) Insert(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.data[key] = struct{}{}
}

func (k *Keys) Remove(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.data, key)
}

type Document struct {
	ID          string
	ScoreWeight int
}

// Predefined list of stopwords
var stopWords = map[string]bool{
	"is": true, "a": true, "the": true, "and": true, "or": true, "to": true, "in": true, "of": true,
}

type SearchEngineController struct {
	Data              map[string]map[string]bool // term -> docID -> bool
	Engines           []*SearchEngine
	GlobalTrie        *Trie
	GlobalKeys        *Keys
	SearchEngineCount int
	Weights           map[string]int  // weights for fields
	Filters           map[string]bool // filters for fields
	PageSize          int
	GlobalDocIDList   map[string]bool
	mu                sync.Mutex
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
		Data:              make(map[string]map[string]bool),
		Engines:           seList,
		SearchEngineCount: searchEngineCount,
		Weights:           weights,
		Filters:           filters,
		PageSize:          pageSize,
		GlobalTrie:        NewTrie(),
		GlobalKeys:        NewKeys(),
		GlobalDocIDList:   make(map[string]bool),
	}
}

// TODO: check for race conditions while indexing and searching
func (sec *SearchEngineController) Index(docs []map[string]interface{}) {
	sec.generateGlobalKeysAndTrie(docs)
	chunkSize := (len(docs) + sec.SearchEngineCount - 1) / sec.SearchEngineCount
	for i := 0; i < sec.SearchEngineCount; i++ {
		dataStart := chunkSize * i
		dataEnd := dataStart + chunkSize
		if dataEnd > len(docs) {
			dataEnd = len(docs)
		}

		sec.Engines[i].Index(docs[dataStart:dataEnd])
	}
}

// ProcessQuery analyzes a search query string and categorizes tokens based on match type.
// It returns a map where the keys represent the match category:
// - "exact": tokens that exactly match entries in the GlobalKeys store.
// - "prefix": tokens that match prefixes found in the GlobalTrie.
// - "fuzzy": tokens that approximately match entries in GlobalKeys within a given edit distance.
func (sec *SearchEngineController) ProcessQuery(query string) map[string][]string {
	queryTokens := tokenize(query)
	if len(queryTokens) == 0 {
		return nil
	}

	result := make(map[string][]string)

	for _, query := range queryTokens {
		if _, exists := sec.GlobalKeys.data[query]; exists {
			result["exact"] = append(result["exact"], query)
			continue
		} else {
			guessArr := sec.GlobalTrie.SearchPrefix(query)
			if guessArr != nil {
				result["prefix"] = append(result["prefix"], guessArr...)
			} else {
				for key := range sec.GlobalKeys.data {
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
	queryTokens := sec.ProcessQuery(query)
	fmt.Println("ProcessQuery:", queryTokens)
	if queryTokens == nil {
		return nil
	}

	resultsChan := make(chan []Document, sec.SearchEngineCount)
	var wg sync.WaitGroup

	if (len(queryTokens["exact"]) == 1 &&
		len(queryTokens["prefix"]) == 0 &&
		len(queryTokens["fuzzy"]) == 0) ||
		(len(queryTokens["exact"]) == 0 &&
			(len(queryTokens["prefix"]) > 0 ||
				len(queryTokens["fuzzy"]) > 0)) {
		for _, se := range sec.Engines {
			wg.Add(1)
			go func(e *SearchEngine) {
				defer wg.Done()
				result := e.OneTermSearch(queryTokens, page, filters)
				resultsChan <- result
			}(se)
		}

		wg.Wait()
		close(resultsChan)

		var finalResult []Document
		for res := range resultsChan {
			finalResult = append(finalResult, res...)
		}

		sort.Slice(finalResult, func(i, j int) bool {
			return finalResult[i].ScoreWeight > finalResult[j].ScoreWeight
		})

		return finalResult
	} else {
		for _, se := range sec.Engines {
			wg.Add(1)
			go func(e *SearchEngine) {
				defer wg.Done()
				result := e.MultipleTermsSearch(queryTokens, filters)
				resultsChan <- result
			}(se)
		}
		wg.Wait()
		close(resultsChan)

		var finalResult []Document
		for res := range resultsChan {
			finalResult = append(finalResult, res...)
		}

		sort.Slice(finalResult, func(i, j int) bool {
			return finalResult[i].ScoreWeight > finalResult[j].ScoreWeight
		})

		lastIndex := page * sec.PageSize
		if lastIndex >= len(finalResult) {
			return nil
		}
		stop := lastIndex + sec.PageSize
		if stop > len(finalResult) {
			stop = len(finalResult)
		}

		return finalResult[lastIndex:stop]
	}
}

func (sec *SearchEngineController) RemoveDocumentByID(docID string) {
	for term, docMap := range sec.Data {
		if _, exists := docMap[docID]; exists {
			delete(docMap, docID)
			if len(docMap) == 0 {
				sec.GlobalKeys.Remove(term)
				sec.GlobalTrie.Remove(term)
				delete(sec.Data, term)
			}
		}
	}
	var wg sync.WaitGroup
	for _, engine := range sec.Engines {
		wg.Add(1)
		go func(se *SearchEngine) {
			defer wg.Done()
			se.RemoveDocumentByID(docID)
		}(engine)
	}
	wg.Wait()
}

func (sec *SearchEngineController) AddDocument(doc map[string]interface{}) error {
	sec.generateGlobalKeysAndTrie([]map[string]interface{}{doc})
	luckyShard := rand.Intn(len(sec.Engines))

	err := sec.Engines[luckyShard].AddDocument(doc)
	return err
}

func (sec *SearchEngineController) UpdateDocument(doc map[string]interface{}) error {
	sec.generateGlobalKeysAndTrie([]map[string]interface{}{doc})
	errCh := make(chan error, len(sec.Engines))
	var wg sync.WaitGroup

	for _, engine := range sec.Engines {
		wg.Add(1)
		go func(se *SearchEngine) {
			defer wg.Done()
			errCh <- se.UpdateDocument(doc)
		}(engine)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (sec *SearchEngineController) addToData(term, docID string) {
	sec.mu.Lock()
	defer sec.mu.Unlock()

	docMap, exists := sec.Data[term]
	if !exists {
		sec.GlobalKeys.Insert(term)
		sec.GlobalTrie.Insert(term)
		docMap = make(map[string]bool)
		sec.Data[term] = docMap
	}
	docMap[docID] = true
}

func (sec *SearchEngineController) generateGlobalKeysAndTrie(docs []map[string]interface{}) {
	for i, doc := range docs {
		if i%100_000 == 0 {
			fmt.Println("GenerateGlobalKeysAndTrie Document:", i)
		}

		docID := fmt.Sprintf("%v", doc["id"])

		for field := range sec.Weights {
			if value, exists := doc[field]; exists {
				switch v := value.(type) {
				case string:
					tokens := tokenize(v)
					for _, token := range tokens {
						sec.addToData(token, docID)
					}
				case []string:
					for _, item := range v {
						tokens := tokenize(item)
						for _, token := range tokens {
							sec.addToData(token, docID)
						}
					}
				case []interface{}:
					for _, item := range v {
						if str, ok := item.(string); ok {
							tokens := tokenize(str)
							for _, token := range tokens {
								sec.addToData(token, docID)
							}
						}
					}
				}
			}
		}
	}
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
	Weights            map[string]int  // weights for fields
	Filters            map[string]bool // filters for fields
	PageSize           int             // size of the documents in a search response.
	// TODO: use RWMutext??
	mu sync.Mutex
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

	keysCh := make(chan string, len(se.Keys.data))

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

	for key := range se.Keys.data {
		keysCh <- key
	}
	close(keysCh)

	wg.Wait()
}

func (se *SearchEngine) Index(docs []map[string]interface{}) {
	fmt.Println("BuildDocumentIndex starting...")
	start := time.Now()
	se.BuildDocumentIndex(docs)
	duration := time.Since(start)
	fmt.Printf("BuildDocumentIndex took: %s\n", duration)

	// fmt.Println("SaveMapToFile starting...")
	// start = time.Now()
	// SaveMapToFile(se.Data, "sedata")
	// duration = time.Since(start)
	// fmt.Printf("SaveMapToFile took: %s\n", duration)

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

func SaveMapToFile(m map[string]map[string]int, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(m)
}

func (se *SearchEngine) InsertDocs(docs []map[string]interface{}) {
	for i, doc := range docs {
		if i%100_000 == 0 || i == len(docs)-1 {
			fmt.Println("InsertDocs:", i)
		}

		docID := fmt.Sprintf("%v", doc["id"])

		if _, exists := se.Documents[docID]; !exists {
			se.mu.Lock()
			se.Documents[docID] = make(map[string]interface{})
			for k, v := range doc {
				se.Documents[docID][k] = v
			}
			se.mu.Unlock()
		}
	}
}

func (se *SearchEngine) BuildDocumentIndex(docs []map[string]interface{}) {
	for i, doc := range docs {
		if i%100_000 == 0 || i == len(docs)-1 {
			fmt.Println("BuildDocumentIndex Document:", i)
		}

		docID := fmt.Sprintf("%v", doc["id"])
		if _, ok := se.Documents[docID]; ok {
			// already indexed document
			continue
		}

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
					if se.FilterDocs[filterKey] == nil {
						se.FilterDocs[filterKey] = make(map[string]bool)
					}
					se.FilterDocs[filterKey][docID] = true
				case string:
					filterKey := fmt.Sprintf("%s:%s", field, v)
					if se.FilterDocs[filterKey] == nil {
						se.FilterDocs[filterKey] = make(map[string]bool)
					}
					se.FilterDocs[filterKey][docID] = true
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
				return se.FilterDocs[fmt.Sprintf("%s:%v", key, values[0])]
			}
			filteredDocsFinal := make(map[string]bool)
			for _, value := range values {
				for docID := range se.FilterDocs[fmt.Sprintf("%s:%v", key, value)] {
					filteredDocsFinal[docID] = true
				}
			}
			return filteredDocsFinal
		}
	} else {
		filteredDocsFinal := make(map[string]bool)
		for k, v := range filters {
			for docID := range se.FilterDocs[fmt.Sprintf("%s:%v", k, v)] {
				filteredDocsFinal[docID] = true
			}
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

	termDocs := se.ScoreIndex[query]
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

func (se *SearchEngine) searchAllWithFilter(exactTerms, otherTerms []string, filteredDocs map[string]bool) []Document {
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

func (se *SearchEngine) SearchMultipleTermsWithoutFilter(queriesMap map[string][]string) []Document {
	result := se.searchAll(queriesMap["exact"], append(queriesMap["prefix"], queriesMap["fuzzy"]...))
	return result
}

func (se *SearchEngine) SearchMultipleTermsWithFilter(queriesMap map[string][]string, filters map[string][]interface{}) []Document {
	filteredDocs := se.ApplyFilter(filters)
	result := se.searchAllWithFilter(queriesMap["exact"], append(queriesMap["prefix"], queriesMap["fuzzy"]...), filteredDocs)
	return result
}

func (se *SearchEngine) CheckKeysExistance(queryTokens map[string][]string) bool {
	for _, token := range queryTokens["exact"] {
		if _, ok := se.ScoreIndex[token]; !ok {
			return false
		}
	}
	if len(queryTokens["prefix"]) > 0 {
		prefixExistance := false
		for _, token := range queryTokens["prefix"] {
			if _, ok := se.ScoreIndex[token]; ok {
				prefixExistance = true
				break
			}
		}
		if !prefixExistance {
			return false
		}
	} else if len(queryTokens["fuzzy"]) > 0 {
		fuzzyExistance := false
		for _, token := range queryTokens["fuzzy"] {
			if _, ok := se.ScoreIndex[token]; ok {
				fuzzyExistance = true
				break
			}
		}
		if !fuzzyExistance {
			return false
		}
	}

	return true
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
func (se *SearchEngine) OneTermSearch(queryTokens map[string][]string, page int, filters map[string][]interface{}) []Document {
	if !se.CheckKeysExistance(queryTokens) {
		fmt.Println("Some queryTokens do not exist in this shard", queryTokens)
		return nil
	}

	if len(filters) == 0 {
		if len(queryTokens["exact"]) == 1 &&
			len(queryTokens["prefix"]) == 0 &&
			len(queryTokens["fuzzy"]) == 0 {
			resultDocs := se.SearchOneTermWithoutFilter(queryTokens["exact"][0], page)
			return resultDocs
		}

		if len(queryTokens["exact"]) == 0 {
			if len(queryTokens["prefix"]) > 0 {
				resultDocs := se.SearchOneTermWithoutFilter(queryTokens["prefix"][0], page)
				return resultDocs
			}
			if len(queryTokens["fuzzy"]) > 0 {
				resultDocs := se.SearchOneTermWithoutFilter(queryTokens["fuzzy"][0], page)
				return resultDocs
			}
		}
	} else {
		if len(queryTokens["exact"]) == 1 &&
			len(queryTokens["prefix"]) == 0 &&
			len(queryTokens["fuzzy"]) == 0 {
			resultDocs := se.SearchOneTermWithFilter(queryTokens["exact"][0], filters, page)
			return resultDocs
		}

		if len(queryTokens["exact"]) == 0 {
			if len(queryTokens["prefix"]) > 0 {
				resultDocs := se.SearchOneTermWithFilter(queryTokens["prefix"][0], filters, page)
				return resultDocs
			}
			if len(queryTokens["fuzzy"]) > 0 {
				resultDocs := se.SearchOneTermWithFilter(queryTokens["fuzzy"][0], filters, page)
				return resultDocs
			}
		}
	}
	return nil
}

func (se *SearchEngine) MultipleTermsSearch(queryTokens map[string][]string, filters map[string][]interface{}) []Document {
	if !se.CheckKeysExistance(queryTokens) {
		fmt.Println("queryTokens do not exist in this shard", queryTokens)
		return nil
	}

	if len(filters) == 0 {
		resultDocs := se.SearchMultipleTermsWithoutFilter(queryTokens)
		return resultDocs
	} else {
		resultDocs := se.SearchMultipleTermsWithFilter(queryTokens, filters)
		return resultDocs
	}
}

func (se *SearchEngine) addOrUpdateScoreIndex(tokens []string, docID string) {
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
				ScoreWeight: se.Data[token][docID],
			}}
			se.mu.Unlock()
		}
	}
}

// AddDocument adds a generic document to the search engine and updates the index.
func (se *SearchEngine) AddDocument(doc map[string]interface{}) error {
	docID := fmt.Sprintf("%v", doc["id"])

	if _, ok := se.Documents[docID]; ok {
		return fmt.Errorf("Document %s is already added to the search engine", docID)
	}

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

// UpdateDocument updates a generic document to the search engine and updates the index.
func (se *SearchEngine) UpdateDocument(doc map[string]interface{}) error {
	docID := fmt.Sprintf("%v", doc["id"])

	if _, ok := se.Documents[docID]; !ok {
		return fmt.Errorf("Document %s does not exist in this Search Engine", docID)
	}

	se.RemoveDocumentByID(docID)

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
	if _, ok := se.Documents[docID]; !ok {
		return fmt.Errorf("Document %s does not exist in this Search Engine", docID)
	}

	se.mu.Lock()
	defer se.mu.Unlock()

	for term, docMap := range se.Data {
		if _, exists := docMap[docID]; exists {
			delete(docMap, docID)
			if len(docMap) == 0 {
				se.Keys.Remove(term)
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

func InsertIntoScoreIndex(scoreIndex map[string][]Document, key string, doc Document) {
	docs := scoreIndex[key]

	// Binary search to find the correct index for descending order
	index := sort.Search(len(docs), func(i int) bool {
		return docs[i].ScoreWeight <= doc.ScoreWeight
	})

	// Extend the slice in-place
	scoreIndex[key] = append(docs, Document{})    // make space
	copy(scoreIndex[key][index+1:], docs[index:]) // shift elements
	scoreIndex[key][index] = doc                  // insert new document
}

// InsertSorted inserts doc into docs while keeping ScoreWeight descending
func InsertSorted(docs []Document, doc Document) []Document {
	index := sort.Search(len(docs), func(i int) bool {
		// Find first index where doc.ScoreWeight >= docs[i].ScoreWeight is false
		return docs[i].ScoreWeight <= doc.ScoreWeight
	})

	// Insert at the found index
	docs = append(docs, Document{})    // make space
	copy(docs[index+1:], docs[index:]) // shift
	docs[index] = doc                  // insert
	return docs
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
