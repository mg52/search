// Package engine implements a lightweight, in-memory, shardable search engine
// built on an inverted index with prefix (Trie) and fuzzy (SymSpell) matching.
// It is concurrency-safe for reads/writes via an internal RWMutex and is
// designed to be persisted/restored via a single gob payload.
package engine

import (
	"context"
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

// Number of max prefix for each word/term.
const MaxPrefixTerms = 400

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
	Prefix      map[string][]string
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
	Prefix      map[string][]string
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
		Prefix:      make(map[string][]string),
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
		Prefix:      se.Prefix,
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
		Prefix:      make(map[string][]string),
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
		se.Prefix = payload.Prefix
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

// BuildScoreIndex (re)constructs ScoreIndex by sorting each term's postings
// descending by weight. It parallelizes over available CPUs.
func (se *SearchEngine) BuildScoreIndex() {
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
	defer se.mu.Unlock()

	docMap, ok := se.Data[term]
	if !ok {
		se.Keys.Insert(term)
		// se.Trie.Insert(term)
		se.Symspell.AddWord(term)
		docMap = make(map[string]int)
		se.Data[term] = docMap

		for i := 1; i < len(term); i++ {
			if len(se.Prefix[term[0:i]]) >= MaxPrefixTerms {
				continue
			}
			se.Prefix[term[0:i]] = append(se.Prefix[term[0:i]], term)
		}
		if len(se.Prefix[term]) < MaxPrefixTerms {
			se.Prefix[term] = append(se.Prefix[term], term)
		}
	}
	docMap[docID] += normalizedWeight
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
func (se *SearchEngine) SearchMultipleTerms(
	ctx context.Context,
	firstTerms []string,
	lastTermGuessArr []string,
	filters map[string][]interface{},
	page int,
) []Document {

	if len(firstTerms) == 0 {
		return nil
	}

	// ---- filters
	var filteredDocs map[string]struct{}
	if len(filters) > 0 {
		tmp := se.ApplyFilter(filters)
		filteredDocs = make(map[string]struct{}, len(tmp))
		for id := range tmp {
			filteredDocs[id] = struct{}{}
		}
	}

	se.mu.RLock()
	defer se.mu.RUnlock()

	// scoreIndex := se.ScoreIndex
	// data := se.Data
	// documents := se.Documents
	// pageSize := se.PageSize

	inputTerm := firstTerms[0]
	for _, t := range firstTerms[1:] {
		if len(se.ScoreIndex[t]) < len(se.ScoreIndex[inputTerm]) {
			inputTerm = t
		}
	}

	startIndex := page * se.PageSize
	stopIndex := startIndex + se.PageSize

	// ---- pre-resolve maps
	exactMaps := make([]map[string]int, 0, len(firstTerms)-1)
	for _, t := range firstTerms {
		if t != inputTerm {
			exactMaps = append(exactMaps, se.Data[t])
		}
	}

	guessMaps := make([]map[string]int, 0, len(lastTermGuessArr))
	for _, t := range lastTermGuessArr {
		if m := se.Data[t]; m != nil {
			guessMaps = append(guessMaps, m)
		}
	}

	// ---- collect hits
	type hit struct {
		id    string
		score int
	}
	hits := make([]hit, 0, stopIndex)

outer:
	for _, doc := range se.ScoreIndex[inputTerm] {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if filteredDocs != nil {
			if _, ok := filteredDocs[doc.ID]; !ok {
				continue
			}
		}

		totalScore := doc.ScoreWeight

		for _, m := range exactMaps {
			w, ok := m[doc.ID]
			if !ok {
				continue outer
			}
			totalScore += w
		}

		if len(guessMaps) > 0 {
			found := false
			for _, m := range guessMaps {
				if w, ok := m[doc.ID]; ok {
					totalScore += w
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		hits = append(hits, hit{id: doc.ID, score: totalScore})
		if len(hits) >= stopIndex {
			break // safe because scoreIndex is score-sorted
		}
	}

	if startIndex >= len(hits) {
		return nil
	}

	if stopIndex > len(hits) {
		stopIndex = len(hits)
	}

	// ---- hydrate only the page
	out := make([]Document, 0, stopIndex-startIndex)
	for _, h := range hits[startIndex:stopIndex] {
		out = append(out, Document{
			ID:          h.id,
			Data:        se.Documents[h.id],
			ScoreWeight: h.score,
		})
	}

	return out
}

// Search executes a query (single or multi-term), selecting between exact/prefix/fuzzy
// strategies and honoring filters. searchStep controls fallback behavior for multi-term
// queries (see MultiTermSearch).
func (se *SearchEngine) Search(ctx context.Context, query string, page int, filters map[string][]interface{}, searchStep int) *SearchResult {
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	queryTokens := Tokenize(query)
	if len(queryTokens) == 0 {
		return nil
	} else if len(queryTokens) == 1 {
		return se.SingleTermSearch(queryTokens, page, filters)
	} else {
		return se.MultiTermSearch(ctx, queryTokens, page, filters, searchStep)
	}
}

// SingleTermSearch resolves a single-token query via prefix (preferred) or fuzzy
// expansions, and applies filters if provided.
func (se *SearchEngine) SingleTermSearch(queryTokens []string, page int, filters map[string][]interface{}) *SearchResult {
	parsedQuery := make(map[string][]string)
	maxToken := 3
	se.mu.RLock()
	prefixTokens := se.Prefix[queryTokens[0]]
	se.mu.RUnlock()
	if len(prefixTokens) < maxToken {
		maxToken = len(prefixTokens)
	}
	se.mu.RLock()
	guessArr := se.Prefix[queryTokens[0]][:maxToken]
	se.mu.RUnlock()
	// guessArr := se.Trie.SearchPrefix(queryTokens[0], 3)
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
//	             expand last token with up to 50 prefix candidates.
//	1 (fallback): for each of the first tokens, try up to 50 fuzzy variants,
//	             and restrict last token to 50 prefix candidates; return on first hit.
//	2 (fallback): ignore last-term guessing; require documents to match the
//	             fuzzy-corrected "first terms" only.
func (se *SearchEngine) MultiTermSearch(ctx context.Context, queryTokens []string, page int, filters map[string][]interface{}, searchStep int) *SearchResult {
	switch searchStep {
	case 0:
		// 1st search: prefix-expand last term; fuzzy-correct earlier terms (1 each).
		// start := time.Now()
		se.mu.RLock()
		lastTermGuessArr := se.Prefix[queryTokens[len(queryTokens)-1]]
		se.mu.RUnlock()
		// lastTermGuessArr := se.Trie.SearchPrefix(queryTokens[len(queryTokens)-1], 250)
		// duration := time.Since(start)
		// fmt.Printf("Shard: %d, SearchPrefix %s, lastTermGuessArr: %v, duration: %s\n", se.ShardID, queryTokens[len(queryTokens)-1], lastTermGuessArr, duration)

		// start = time.Now()
		isFuzzy := false
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
				isFuzzy = true
			}
		}

		// duration = time.Since(start)
		// fmt.Printf("Shard: %d, FirstTerm: %s\n", se.ShardID, duration)

		// start = time.Now()
		finalDocs := se.SearchMultipleTerms(ctx, firstTerms, lastTermGuessArr, filters, page)
		// duration = time.Since(start)
		// fmt.Printf("Shard: %d, SearchMultipleTerms: %s\n", se.ShardID, duration)

		return &SearchResult{
			Docs:            finalDocs,
			IsMultiTerm:     true,
			IsFuzzy:         isFuzzy,
			IsPrefixOrExact: false,
		}
	case 1:
		// 2nd search: broaden fuzzies for earlier tokens; narrow last-term prefixes.
		// lastTermGuessArr := se.Trie.SearchPrefix(queryTokens[len(queryTokens)-1], 50)
		lastTermGuessArr := se.Symspell.FuzzySearch(queryTokens[len(queryTokens)-1], 50)

		for k, queryToken := range queryTokens[:len(queryTokens)-1] {
			firstTermFuzzies := se.Symspell.FuzzySearch(queryToken, 50)
			firstTerms := make([]string, len(queryTokens)-1)
			copy(firstTerms, queryTokens[:len(queryTokens)-1])
			for _, firstTermFuzzy := range firstTermFuzzies {
				firstTerms[k] = firstTermFuzzy
				finalDocs := se.SearchMultipleTerms(ctx, firstTerms, lastTermGuessArr, filters, page)
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
	case 2:
		// 3rd search: drop last-term guessing; require only first-term matches.
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
		finalDocs := se.SearchMultipleTerms(ctx, firstTerms, nil, filters, page)
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

func (se *SearchEngine) removeFromPrefix(term string) {
	for i := 1; i <= len(term); i++ {
		prefix := term[:i]

		list, ok := se.Prefix[prefix]
		if !ok {
			continue
		}

		// Find and remove "term" from the slice
		for j := 0; j < len(list); j++ {
			if list[j] == term {
				// swap-remove (order does NOT matter)
				list[j] = list[len(list)-1]
				list = list[:len(list)-1]
				break
			}
		}

		if len(list) == 0 {
			delete(se.Prefix, prefix)
		} else {
			se.Prefix[prefix] = list
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
				// se.Trie.Remove(term)
				se.removeFromPrefix(term)
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
