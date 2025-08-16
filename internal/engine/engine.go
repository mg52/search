// Package engine implements a lightweight, in-memory, shardable search engine
// built on an inverted index with prefix (Trie) and fuzzy (SymSpell) matching.
// It is concurrency-safe for reads/writes via an internal RWMutex and is
// designed to be persisted/restored via a single gob payload.
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
	"github.com/mg52/search/internal/pkg/symspell"
	"github.com/mg52/search/internal/pkg/trie"
)

// Document represents a single search hit.
//
// ID is the string form of the original document's "id" field.
// Data holds the original document key/value pairs for retrieval.
// ScoreWeight is the term-weight score accumulated during indexing/search.
type Document struct {
	ID          string
	Data        map[string]interface{}
	ScoreWeight int
}

// SearchResult encapsulates the outcome of a query executed on a single shard.
//
// Docs            The page of results.
// IsMultiTerm     True when the input contained more than one token.
// IsPrefixOrExact True when the result path used exact or prefix expansion.
// IsFuzzy         True when the result path used fuzzy expansion.
// PrefixLength    Reserved field for prefix depth (not currently set here).
type SearchResult struct {
	Docs            []Document
	IsMultiTerm     bool
	IsPrefixOrExact bool
	IsFuzzy         bool
	PrefixLength    int
}

// stopWords lists very common words excluded from indexing.
var stopWords = map[string]bool{
	"a":   true,
	"the": true,
	"and": true,
}

// enginePayload is the gob-serializable snapshot of SearchEngine state.
//
// It intentionally mirrors the live fields used by SearchEngine so SaveAll/LoadAll
// can persist/restore the engine with minimal glue.
type enginePayload struct {
	Data        map[string]map[string]int
	Documents   map[string]map[string]interface{}
	ScoreIndex  map[string][]Document
	FilterDocs  map[string]map[string]bool
	IndexFields []string
	Filters     map[string]bool
	PageSize    int
	Keys        *keys.Keys
	Trie        *trie.Trie
	Symspell    *symspell.SymSpell
}

// SearchEngine maintains an inverted index plus auxiliary structures for
// prefix and fuzzy lookup. It is safe for concurrent use.
//
// Concurrency: guards internal maps/slices with mu (RWMutex).
// Sharding: ShardID identifies the shard this instance serves.
// Persistence: SaveAll/LoadAll snapshot/restore most fields via gob.
type SearchEngine struct {
	ShardID     int
	Data        map[string]map[string]int         // term -> docID -> weight
	Documents   map[string]map[string]interface{} // docID -> original document fields
	ScoreIndex  map[string][]Document             // term -> sorted []Document (by ScoreWeight desc)
	FilterDocs  map[string]map[string]bool        // "field:value" -> set(docID)
	Keys        *keys.Keys
	Trie        *trie.Trie
	Symspell    *symspell.SymSpell
	IndexFields []string
	Filters     map[string]bool // fields that may be used as filters
	PageSize    int             // max results to return per page
	mu          sync.RWMutex
}

// NewSearchEngine constructs a new, empty engine ready to index documents.
//
// indexFields defines which doc fields are tokenized and weighted.
// filters lists the fields eligible for structured filters.
// pageSize controls pagination size in search responses.
// shardID identifies this shard for logging/diagnostics only.
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
		Keys:        keys.NewKeys(),
		Trie:        trie.NewTrie(),
		Symspell:    symspell.NewSymSpell(),
		FilterDocs:  make(map[string]map[string]bool),
		PageSize:    pageSize,
	}
}

func init() {
	// Register types for gob persistence. This enables SaveAll/LoadAll to
	// encode/decode the engine payload and related types.
	gob.Register(enginePayload{})
	gob.Register(Document{})
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register(&trie.Trie{})
	gob.Register(&trie.TrieNode{})
	gob.Register(&symspell.SymSpell{})
}

// SaveAll writes the engine snapshot to a single gob file at path + ".engine.gob".
//
// Only serializes fields in enginePayload. Returns an error on I/O or encoding failures.
func (se *SearchEngine) SaveAll(path string) error {
	se.mu.RLock()
	defer se.mu.RUnlock()

	payload := enginePayload{
		Data:        se.Data,
		Documents:   se.Documents,
		FilterDocs:  se.FilterDocs,
		IndexFields: se.IndexFields,
		Filters:     se.Filters,
		PageSize:    se.PageSize,
		Keys:        se.Keys,
		Trie:        se.Trie,
		Symspell:    se.Symspell,
	}

	engineFile := path + ".engine.gob"
	f, err := os.Create(engineFile)
	if err != nil {
		return fmt.Errorf("create %s: %w", engineFile, err)
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(payload); err != nil {
		return fmt.Errorf("encode engine payload: %w", err)
	}
	return nil
}

// LoadAll restores an engine snapshot from path + ".engine.gob".
//
// Missing files are tolerated (engine returns empty/default state). ScoreIndex
// is rebuilt after load to restore sorted postings per term.
func LoadAll(path string, shardID int) (*SearchEngine, error) {
	se := &SearchEngine{
		Data:        make(map[string]map[string]int),
		Documents:   make(map[string]map[string]interface{}),
		ScoreIndex:  make(map[string][]Document),
		FilterDocs:  make(map[string]map[string]bool),
		Keys:        keys.NewKeys(),
		Trie:        trie.NewTrie(),
		Symspell:    symspell.NewSymSpell(),
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
		se.Documents = payload.Documents
		se.FilterDocs = payload.FilterDocs
		se.IndexFields = payload.IndexFields
		se.Filters = payload.Filters
		se.PageSize = payload.PageSize
		se.Keys = payload.Keys
		se.Trie = payload.Trie
		se.Symspell = payload.Symspell
	}

	se.BuildScoreIndex()

	return se, nil
}

// Index performs a full (re)index pass for the provided docs and logs timings.
//
// The steps are:
//  1. InsertDocs        — materialize raw documents
//  2. BuildDocumentIndex— tokenize/update inverted index & filters
//  3. BuildScoreIndex   — build per-term sorted postings
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

// ProcessQuery tokenizes a raw string and categorizes tokens by match strategy.
//
// Returns a map with keys:
//   - "raw":   tokenized inputs
//   - "exact": tokens known to Keys (or their prefix/fuzzy fallbacks when multi-term)
//   - "prefix": up to N prefix candidates (single-term path)
//   - "fuzzy":  fuzzy candidates (single-term path)
//
// The second return value is the token count. This helper is separate from the
// main Search path and can be used for UI explain/debug.
func (se *SearchEngine) ProcessQuery(query string) (map[string][]string, int) {
	queryTokens := Tokenize(query)
	tokenCount := len(queryTokens)
	if tokenCount == 0 {
		return nil, 0
	}

	result := make(map[string][]string)
	result["raw"] = queryTokens
	if tokenCount == 1 {
		guessArr := se.Trie.SearchPrefix(queryTokens[0], 3)
		if guessArr != nil {
			result["prefix"] = append(result["prefix"], guessArr...)
		} else {
			fuzzyWords := se.Symspell.FuzzySearch(queryTokens[0], 5)
			if len(fuzzyWords) > 0 {
				result["fuzzy"] = append(result["fuzzy"], fuzzyWords...)
			} else {
				result["fuzzy"] = append(result["fuzzy"], queryTokens[0])
			}
		}
	} else {
		// For multi-term, treat all but the last token as exact/fuzzy-corrected.
		for _, exactWord := range queryTokens[:tokenCount-1] {
			_, ok := se.Keys.GetData()[exactWord]
			if ok {
				result["exact"] = append(result["exact"], exactWord)
			} else {
				guessArr := se.Trie.SearchPrefix(exactWord, 1)
				if guessArr != nil {
					result["exact"] = append(result["exact"], guessArr[0])
				} else {
					fuzzyWords := se.Symspell.FuzzySearch(exactWord, 1)
					if len(fuzzyWords) > 0 {
						result["exact"] = append(result["exact"], fuzzyWords[0])
					} else {
						result["exact"] = append(result["exact"], exactWord)
					}
				}
			}
		}
	}

	return result, tokenCount
}

// BuildScoreIndex (re)constructs ScoreIndex by sorting each term's postings
// descending by weight. It parallelizes over available CPUs.
func (se *SearchEngine) BuildScoreIndex() {
	// Snapshot term keys to avoid concurrent map iteration.
	se.mu.RLock()
	keys := make([]string, 0, len(se.Data))
	for k := range se.Data {
		keys = append(keys, k)
	}
	se.mu.RUnlock()

	workers := runtime.NumCPU()
	var wg sync.WaitGroup
	keysCh := make(chan string, len(keys))

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range keysCh {
				// Read postings and doc payloads under RLock.
				var tempArr []Document
				se.mu.RLock()
				for docID, weight := range se.Data[key] {
					tempArr = append(tempArr, Document{
						ID:          docID,
						ScoreWeight: weight,
						Data:        se.Documents[docID],
					})
				}
				se.mu.RUnlock()

				sort.Slice(tempArr, func(i, j int) bool {
					return tempArr[i].ScoreWeight > tempArr[j].ScoreWeight
				})

				se.mu.Lock()
				se.ScoreIndex[key] = tempArr
				se.mu.Unlock()
			}
		}()
	}

	for _, k := range keys {
		keysCh <- k
	}
	close(keysCh)
	wg.Wait()
}

// InsertDocs materializes raw documents into the Documents store without reindexing.
//
// If a document with the same id already exists, it is left intact (no overwrite).
func (se *SearchEngine) InsertDocs(docs []map[string]interface{}) {
	for i, doc := range docs {
		if i%100_000 == 0 || i == len(docs)-1 {
			fmt.Println("InsertDocs:", i)
		}

		rawID, ok := doc["id"]
		if !ok || rawID == nil {
			// No usable ID; skip safely.
			continue
		}
		docID := fmt.Sprintf("%v", rawID)
		if docID == "" || docID == "<nil>" {
			continue
		}

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

// BuildDocumentIndex tokenizes index fields and updates the inverted index and filters.
//
// Indexing:
//   - Tokenization is done by Tokenize (lowercase, strip non-alnum, drop stopwords).
//   - Each token contributes a normalized weight = 100_000 / total_token_count.
//
// Filters:
//   - For each configured filter field, creates a key "field:value" -> set(docID).
func (se *SearchEngine) BuildDocumentIndex(docs []map[string]interface{}) {
	for i, doc := range docs {
		if i%100_000 == 0 || i == len(docs)-1 {
			fmt.Println("BuildDocumentIndex Document:", i)
		}

		rawID, ok := doc["id"]
		if !ok || rawID == nil {
			continue
		}
		docID := fmt.Sprintf("%v", rawID)
		if docID == "" || docID == "<nil>" {
			continue
		}

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

// addToDocumentIndex adds/updates a term->docID weight and seeds Keys/Trie/SymSpell
// when encountering the term for the first time.
//
// Weighting: normalizedWeight := 100_000 / document_token_count.
func (se *SearchEngine) addToDocumentIndex(
	term, docID string, length int,
) {
	// TODO: Consider adding a "popularity" prior and bias normalizedWeight accordingly.
	normalizedWeight := (100_000 / length)

	se.mu.Lock()

	docMap, ok := se.Data[term]
	if !ok {
		se.Keys.Insert(term)
		se.Trie.Insert(term)
		se.Symspell.AddWord(term)
		docMap = make(map[string]int)
		se.Data[term] = docMap
	}
	docMap[docID] += normalizedWeight

	se.mu.Unlock()
}

// SearchOneTermWithoutFilter retrieves a page of results for a single term
// using the prebuilt ScoreIndex. Returns nil when the requested page is empty.
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

// ApplyFilter returns the set of docIDs that satisfy the given filters.
// Semantics: OR within a field's values, AND across different fields.
// Example: {year:[2020,2021], type:["song"]} ⇒ docs where (year=2020 OR year=2021) AND (type="song").
func (se *SearchEngine) ApplyFilter(filters map[string][]interface{}) map[string]bool {
	if len(filters) == 0 {
		return nil
	}

	var result map[string]bool
	firstField := true

	for field, values := range filters {
		// Union of all values for this field
		fieldUnion := make(map[string]bool)
		for _, v := range values {
			key := fmt.Sprintf("%s:%v", field, v)

			se.mu.RLock()
			docs := se.FilterDocs[key]
			se.mu.RUnlock()

			for docID := range docs {
				fieldUnion[docID] = true
			}
		}

		// First field initializes the result; subsequent fields intersect.
		if firstField {
			result = fieldUnion
			firstField = false
			continue
		}

		// Intersection: keep only IDs present in both result and fieldUnion.
		for docID := range result {
			if !fieldUnion[docID] {
				delete(result, docID)
			}
		}

		// Early exit if intersection is empty.
		if len(result) == 0 {
			break
		}
	}

	return result
}

// SearchOneTermWithFilter retrieves a page of results for a single term while
// applying the filter constraints. Returns up to PageSize documents.
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

	// NOTE: This scans ScoreIndex[query] and counts only those in filtered set.
	// Returning/accepting a "resume cursor" would avoid rescanning earlier pages.
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

// SearchMultipleTerms executes a multi-term search with optional last-term prefix guesses.
//
// Strategy:
//   - Choose the rarest "first term" (smallest postings list) as the driving term.
//   - Require the document to contain all other "first terms".
//   - Optionally accept any of lastTermGuessArr (any-match).
//   - Accumulate total score across matched terms.
//
// Pagination is handled by slicing the accumulated results.
func (se *SearchEngine) SearchMultipleTerms(firstTerms []string, lastTermGuessArr []string, filters map[string][]interface{}, page int) []Document {
	filteredDocs := make(map[string]bool)
	if len(filters) > 0 {
		filteredDocs = se.ApplyFilter(filters)
	}

	var finalDocs []Document

	se.mu.RLock()
	defer se.mu.RUnlock()

	inputTerm := ""
	if len(firstTerms) == 1 {
		inputTerm = firstTerms[0]
	} else if len(firstTerms) > 1 {
		inputTerm = firstTerms[0]
		for _, exactTerm := range firstTerms[1:] {
			if len(se.ScoreIndex[exactTerm]) < len(se.ScoreIndex[inputTerm]) {
				inputTerm = exactTerm
			}
		}
	} else {
		return finalDocs
	}

	startIndex := page * se.PageSize
	stopIndex := startIndex + se.PageSize

	for _, doc := range se.ScoreIndex[inputTerm] {
		if len(filters) > 0 {
			if _, ok := filteredDocs[doc.ID]; !ok {
				continue
			}
		}

		totalScore := doc.ScoreWeight

		if len(firstTerms) > 1 {
			inAllExact := true
			allOtherExactWeights := 0
			for i := 0; i < len(firstTerms); i++ {
				if inputTerm == firstTerms[i] {
					continue
				}
				otherExactWeight, ok := se.Data[firstTerms[i]][doc.ID]
				if !ok {
					inAllExact = false
					break
				}
				allOtherExactWeights += otherExactWeight
			}
			if !inAllExact {
				continue
			}

			totalScore += allOtherExactWeights
		}

		if len(lastTermGuessArr) > 0 {
			foundOther := false
			otherWeight := 0
			for _, term := range lastTermGuessArr {
				if otherData, ok := se.Data[term][doc.ID]; ok {
					foundOther = true
					otherWeight += otherData
					break
				}
			}
			if !foundOther {
				continue
			}
			totalScore += otherWeight
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

	if startIndex >= len(finalDocs) {
		return nil
	}
	if stopIndex > len(finalDocs) {
		stopIndex = len(finalDocs)
	}

	return finalDocs[startIndex:stopIndex]
}

// Search executes a query (single or multi-term), selecting between exact/prefix/fuzzy
// strategies and honoring filters. searchStep controls fallback behavior for multi-term
// queries (see MultiTermSearch).
func (se *SearchEngine) Search(query string, page int, filters map[string][]interface{}, searchStep int) *SearchResult {
	queryTokens := Tokenize(query)
	if len(queryTokens) == 0 {
		return nil
	} else if len(queryTokens) == 1 {
		return se.SingleTermSearch(queryTokens, page, filters)
	} else {
		return se.MultiTermSearch(queryTokens, page, filters, searchStep)
	}
}

// SingleTermSearch resolves a single-token query via prefix (preferred) or fuzzy
// expansions, and applies filters if provided.
func (se *SearchEngine) SingleTermSearch(queryTokens []string, page int, filters map[string][]interface{}) *SearchResult {
	parsedQuery := make(map[string][]string)
	guessArr := se.Trie.SearchPrefix(queryTokens[0], 3)
	if guessArr != nil {
		parsedQuery["prefix"] = append(parsedQuery["prefix"], guessArr...)
	} else {
		fuzzyWords := se.Symspell.FuzzySearch(queryTokens[0], 3)
		if len(fuzzyWords) > 0 {
			parsedQuery["fuzzy"] = append(parsedQuery["fuzzy"], fuzzyWords...)
		} else {
			parsedQuery["fuzzy"] = append(parsedQuery["fuzzy"], queryTokens[0])
		}
	}
	var finalDocs []Document
	if len(filters) == 0 {
		if parsedQuery["prefix"] != nil {
			for _, query := range parsedQuery["prefix"] {
				finalDocs = append(finalDocs, se.SearchOneTermWithoutFilter(query, page)...)
			}
			return &SearchResult{
				Docs:            finalDocs,
				IsMultiTerm:     false,
				IsFuzzy:         false,
				IsPrefixOrExact: true,
			}
		} else if parsedQuery["fuzzy"] != nil {
			for _, query := range parsedQuery["fuzzy"] {
				finalDocs = append(finalDocs, se.SearchOneTermWithoutFilter(query, page)...)
			}
			return &SearchResult{
				Docs:            finalDocs,
				IsMultiTerm:     false,
				IsFuzzy:         true,
				IsPrefixOrExact: false,
			}
		}
	} else {
		if parsedQuery["prefix"] != nil {
			for _, query := range parsedQuery["prefix"] {
				finalDocs = append(finalDocs, se.SearchOneTermWithFilter(query, filters, page)...)
			}
			return &SearchResult{
				Docs:            finalDocs,
				IsMultiTerm:     false,
				IsFuzzy:         false,
				IsPrefixOrExact: true,
			}
		} else if parsedQuery["fuzzy"] != nil {
			for _, query := range parsedQuery["fuzzy"] {
				finalDocs = append(finalDocs, se.SearchOneTermWithFilter(query, filters, page)...)
			}
			return &SearchResult{
				Docs:            finalDocs,
				IsMultiTerm:     false,
				IsFuzzy:         true,
				IsPrefixOrExact: false,
			}
		}
	}
	return nil
}

// MultiTermSearch handles multi-token queries with staged fallbacks controlled
// by searchStep:
//
//	0 (primary): fuzzy-correct all but the last token (1 suggestion each);
//	             expand last token with up to 250 prefix candidates.
//	1 (fallback): for each of the first tokens, try up to 50 fuzzy variants,
//	             and restrict last token to 50 prefix candidates; return on first hit.
//	2 (fallback): ignore last-term guessing; require documents to match the
//	             fuzzy-corrected "first terms" only.
func (se *SearchEngine) MultiTermSearch(queryTokens []string, page int, filters map[string][]interface{}, searchStep int) *SearchResult {
	if searchStep == 0 {
		// Primary search: prefix-expand last term; fuzzy-correct earlier terms (1 each).
		lastTermGuessArr := se.Trie.SearchPrefix(queryTokens[len(queryTokens)-1], 250)
		var firstTerms []string
		for _, firstTerm := range queryTokens[:len(queryTokens)-1] {
			_, ok := se.Keys.GetData()[firstTerm]
			if ok {
				firstTerms = append(firstTerms, firstTerm)
			} else {
				fuzzyWords := se.Symspell.FuzzySearch(firstTerm, 1)
				if len(fuzzyWords) > 0 {
					firstTerms = append(firstTerms, fuzzyWords[0])
				} else {
					firstTerms = append(firstTerms, firstTerm)
				}
			}
		}
		finalDocs := se.SearchMultipleTerms(firstTerms, lastTermGuessArr, filters, page)
		return &SearchResult{
			Docs:            finalDocs,
			IsMultiTerm:     true,
			IsFuzzy:         false,
			IsPrefixOrExact: false,
		}
	} else if searchStep == 1 {
		// Secondary search: broaden fuzzies for earlier tokens; narrow last-term prefixes.
		lastTermGuessArr := se.Trie.SearchPrefix(queryTokens[len(queryTokens)-1], 50)
		for k, queryToken := range queryTokens[:len(queryTokens)-1] {
			firstTermFuzzies := se.Symspell.FuzzySearch(queryToken, 50)
			firstTerms := make([]string, len(queryTokens)-1)
			copy(firstTerms, queryTokens[:len(queryTokens)-1])
			for _, firstTermFuzzy := range firstTermFuzzies {
				firstTerms[k] = firstTermFuzzy
				finalDocs := se.SearchMultipleTerms(firstTerms, lastTermGuessArr, filters, page)
				if len(finalDocs) > 0 {
					return &SearchResult{
						Docs:            finalDocs,
						IsMultiTerm:     true,
						IsFuzzy:         false,
						IsPrefixOrExact: false,
					}
				}
			}
		}
		return &SearchResult{
			Docs:            []Document{},
			IsMultiTerm:     true,
			IsFuzzy:         false,
			IsPrefixOrExact: false,
		}
	} else if searchStep == 2 {
		// Tertiary search: drop last-term guessing; require only first-term matches.
		var firstTerms []string
		for _, firstTerm := range queryTokens[:len(queryTokens)-1] {
			_, ok := se.Keys.GetData()[firstTerm]
			if ok {
				firstTerms = append(firstTerms, firstTerm)
			} else {
				fuzzyWords := se.Symspell.FuzzySearch(firstTerm, 1)
				if len(fuzzyWords) > 0 {
					firstTerms = append(firstTerms, fuzzyWords[0])
				} else {
					firstTerms = append(firstTerms, firstTerm)
				}
			}
		}
		finalDocs := se.SearchMultipleTerms(firstTerms, nil, filters, page)
		return &SearchResult{
			Docs:            finalDocs,
			IsMultiTerm:     true,
			IsFuzzy:         false,
			IsPrefixOrExact: false,
		}
	}
	return nil
}

// addScoreIndex inserts or updates score entries for a document across tokens,
// keeping each token’s ScoreIndex slice sorted in descending ScoreWeight.
//
// This is used for incremental adds (see addDocument); full builds use BuildScoreIndex.
func (se *SearchEngine) addScoreIndex(tokens []string, docID string) {
	for _, token := range tokens {
		se.mu.RLock()
		score := se.Data[token][docID]
		docs, ok := se.ScoreIndex[token]
		se.mu.RUnlock()
		if ok {
			// Binary search for descending insert position.
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

// addDocument adds a single document and updates both the inverted index and
// the ScoreIndex incrementally. Intended for online ingestion.
func (se *SearchEngine) addDocument(doc map[string]interface{}) {
	rawID, ok := doc["id"]
	if !ok || rawID == nil {
		return
	}
	docID := fmt.Sprintf("%v", rawID)
	if docID == "" || docID == "<nil>" {
		return
	}

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

// removeDocumentByID deletes a document and cleans up all indexes.
//
// It removes the doc from term postings, ScoreIndex, DocData, filter sets,
// and the Documents store. If a term becomes empty, it is also purged from
// Keys/Trie/SymSpell.
func (se *SearchEngine) removeDocumentByID(docID string) {
	se.mu.Lock()
	defer se.mu.Unlock()

	for term, docMap := range se.Data {
		if _, exists := docMap[docID]; exists {
			delete(docMap, docID)
			if len(docMap) == 0 {
				se.Keys.Remove(term)
				se.Trie.Remove(term)
				se.Symspell.DeleteWord(term)
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

// Tokenize splits text into tokens by:
//  1. lowercasing,
//  2. stripping non-alphanumeric characters,
//  3. dropping stopwords.
//
// Example:
//
//	"The Iron-Maiden, 2024!" -> ["ironmaiden", "2024"]
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
