// Package engine implements a lightweight, in-memory search engine
// built on an inverted index with prefix (Trie) and fuzzy (SymSpell) matching.
// It is concurrency-safe for reads/writes via an internal RWMutex and is
// designed to be persisted/restored via a single gob payload.
package engine

import (
	"container/heap"
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

type minHeap []internalHit

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].score < h[j].score } // min-heap
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x any) {
	*h = append(*h, x.(internalHit))
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type internalHit struct {
	id    uint32
	score int
}

// TODO: make it a variable
// Number of max prefix for each word/term.
const MaxPrefixTerms = 400

type Document struct {
	ID    string
	Score int
}

type ReturnedDocument struct {
	ID    string
	Data  map[string]interface{}
	Score int
}

type SearchResult struct {
	Docs []ReturnedDocument
}

// TODO: make it a variable
// stopWords lists very common words excluded from indexing.
var stopWords = map[string]bool{
	"a":   true,
	"the": true,
	"and": true,
}

// enginePayload is the gob-serializable snapshot of SearchEngine state.
type enginePayload struct {
	Documents          map[uint32]map[string]interface{}
	DocDeleted         map[uint32]bool
	ExternalToInternal map[string]uint32
	InternalToExternal map[uint32]string
	NextInternalID     uint32
	IndexFields        []string
	Filters            map[string]bool
	ResultSize         int
}

// SearchEngine maintains an inverted index plus auxiliary structures for
// prefix and fuzzy lookup. It is safe for concurrent use.
type SearchEngine struct {
	// Core index
	DataMap    map[string]map[uint32]int // term -> internalDocID -> postings
	DocDeleted map[uint32]bool           // internalDocID -> deleted?

	// ID mapping: updates create a NEW internalDocID and delete the previous one
	ExternalToInternal map[string]uint32 // externalDocID -> current internalDocID
	InternalToExternal map[uint32]string // internalDocID -> externalDocID
	nextInternalID     uint32

	// Docs + filters
	Documents   map[uint32]map[string]interface{} // internalDocID -> doc fields
	FilterDocs  map[string]map[uint32]bool        // "field:value" -> set(internalDocID)
	Keys        *keys.Keys
	Trie        *trie.Trie
	Prefix      map[string][]string
	Symspell    *symspell.SymSpell
	IndexFields []string
	Filters     map[string]bool
	ResultSize  int

	mu sync.RWMutex
}

// NewSearchEngine constructs a new, empty engine ready to index documents.
func NewSearchEngine(indexFields []string, filters map[string]bool, resultSize int) *SearchEngine {
	return &SearchEngine{
		DataMap:            make(map[string]map[uint32]int),
		DocDeleted:         make(map[uint32]bool),
		ExternalToInternal: make(map[string]uint32),
		InternalToExternal: make(map[uint32]string),
		nextInternalID:     1,
		Documents:          make(map[uint32]map[string]interface{}),
		IndexFields:        indexFields,
		Filters:            filters,
		Keys:               keys.NewKeys(),
		Trie:               trie.NewTrie(),
		Prefix:             make(map[string][]string),
		Symspell:           symspell.NewSymSpell(),
		FilterDocs:         make(map[string]map[uint32]bool),
		ResultSize:         resultSize,
	}
}

func init() {
	gob.Register(enginePayload{})
	gob.Register(Document{})
	gob.Register(ReturnedDocument{})
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register(&trie.Trie{})
	gob.Register(&trie.TrieNode{})
	gob.Register(&symspell.SymSpell{})
}

// -------------------- Persistence --------------------

// SaveAll writes the engine snapshot to a single gob file at path + "/engine.gob".
func (se *SearchEngine) SaveAll(path string) error {
	se.mu.RLock()
	defer se.mu.RUnlock()

	payload := enginePayload{
		Documents:          se.Documents,
		DocDeleted:         se.DocDeleted,
		ExternalToInternal: se.ExternalToInternal,
		InternalToExternal: se.InternalToExternal,
		NextInternalID:     se.nextInternalID,
		IndexFields:        se.IndexFields,
		Filters:            se.Filters,
		ResultSize:         se.ResultSize,
	}

	engineFile := path + "/engine.gob"
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

// LoadAll restores documents + metadata, then rebuilds ALL derived structures
// (DataMap, FilterDocs, Keys, Prefix, Symspell) from Documents.
func LoadAll(path string) (*SearchEngine, error) {
	engineFile := path + "/engine.gob"
	f, err := os.Open(engineFile)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", engineFile, err)
	}
	defer f.Close()

	var payload enginePayload
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode engine payload: %w", err)
	}

	// Base engine (derived fields empty; we will rebuild)
	se := &SearchEngine{
		DataMap:            make(map[string]map[uint32]int),
		DocDeleted:         make(map[uint32]bool),
		ExternalToInternal: make(map[string]uint32),
		InternalToExternal: make(map[uint32]string),
		nextInternalID:     1,
		Documents:          make(map[uint32]map[string]interface{}),
		FilterDocs:         make(map[string]map[uint32]bool),
		Keys:               keys.NewKeys(),
		Trie:               trie.NewTrie(),
		Prefix:             make(map[string][]string),
		Symspell:           symspell.NewSymSpell(),
		IndexFields:        nil,
		Filters:            make(map[string]bool),
		ResultSize:         100,
	}

	// Restore docs + metadata
	se.Documents = payload.Documents
	se.DocDeleted = payload.DocDeleted
	se.ExternalToInternal = payload.ExternalToInternal
	se.InternalToExternal = payload.InternalToExternal
	se.nextInternalID = payload.NextInternalID
	se.IndexFields = payload.IndexFields
	se.Filters = payload.Filters
	se.ResultSize = payload.ResultSize

	// Safety: if NextInternalID missing/zero in older payloads
	if se.nextInternalID == 0 {
		var max uint32
		for id := range se.InternalToExternal {
			if id > max {
				max = id
			}
		}
		se.nextInternalID = max + 1
		if se.nextInternalID == 0 {
			se.nextInternalID = 1
		}
	}

	// -------- Rebuild derived structures from Documents --------
	//
	// We rebuild ONLY current (non-deleted) versions:
	// - If ExternalToInternal points to internalID X, and X is not deleted => index it.
	// - Old internal IDs remain in Documents but are tombstoned => skipped.

	// Helper to check if an internal docID is the current one for its externalID
	isCurrent := func(internalID uint32) bool {
		ext := se.InternalToExternal[internalID]
		if ext == "" {
			return false
		}
		cur, ok := se.ExternalToInternal[ext]
		return ok && cur == internalID
	}

	for internalID, doc := range se.Documents {
		if doc == nil {
			continue
		}
		if se.DocDeleted[internalID] {
			continue
		}
		if !isCurrent(internalID) {
			continue
		}

		// 1) Collect tokens from index fields
		var allTokens []string
		for _, field := range se.IndexFields {
			val, ok := doc[field]
			if !ok {
				continue
			}
			switch v := val.(type) {
			case string:
				allTokens = append(allTokens, Tokenize(v)...)
			case []string:
				for _, s := range v {
					allTokens = append(allTokens, Tokenize(s)...)
				}
			case []interface{}:
				for _, item := range v {
					if s, ok := item.(string); ok {
						allTokens = append(allTokens, Tokenize(s)...)
					}
				}
			}
		}

		// 2) Index tokens using existing helper (locks internally)
		for _, token := range allTokens {
			se.addToDocumentIndex(token, internalID, len(allTokens))
		}

		// 3) Rebuild filter docs
		for field := range se.Filters {
			val, ok := doc[field]
			if !ok {
				continue
			}

			switch v := val.(type) {
			case int, int8, int16, int32, int64, float32, float64:
				filterKey := fmt.Sprintf("%s:%v", field, v)
				se.mu.Lock()
				if se.FilterDocs[filterKey] == nil {
					se.FilterDocs[filterKey] = make(map[uint32]bool)
				}
				se.FilterDocs[filterKey][internalID] = true
				se.mu.Unlock()

			case string:
				filterKey := fmt.Sprintf("%s:%s", field, v)
				se.mu.Lock()
				if se.FilterDocs[filterKey] == nil {
					se.FilterDocs[filterKey] = make(map[uint32]bool)
				}
				se.FilterDocs[filterKey][internalID] = true
				se.mu.Unlock()
			}
		}
	}

	return se, nil
}

// -------------------- Indexing --------------------

// Index performs a full (re)index pass for the provided docs and logs timings.
//
// Steps:
//  1. InsertDocs        — assign internal IDs & store docs (updates create new internalID and delete old)
//  2. BuildDocumentIndex— tokenize/update inverted index & filters
func (se *SearchEngine) Index(docs []map[string]interface{}) {
	fmt.Println("Insert documents starting...")
	start := time.Now()
	se.InsertDocs(docs)
	fmt.Printf("Insert documents took: %s\n", time.Since(start))

	fmt.Println("BuildDocumentIndex starting...")
	start = time.Now()
	se.BuildDocumentIndex(docs)
	fmt.Printf("BuildDocumentIndex took: %s\n", time.Since(start))
}

// InsertDocs materializes raw documents into the Documents store without reindexing.
func (se *SearchEngine) InsertDocs(docs []map[string]interface{}) {
	workers := runtime.NumCPU()
	jobs := make(chan map[string]interface{}, workers*2)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for doc := range jobs {
			rawID, ok := doc["id"]
			if !ok || rawID == nil {
				continue
			}
			extID := fmt.Sprintf("%v", rawID)
			if extID == "" || extID == "<nil>" {
				continue
			}

			se.mu.Lock()

			// delete previous internal doc if exists
			if oldInternal, exists := se.ExternalToInternal[extID]; exists {
				se.DocDeleted[oldInternal] = true
			}

			// assign new internal id
			internal := se.nextInternalID
			se.nextInternalID++

			se.ExternalToInternal[extID] = internal
			se.InternalToExternal[internal] = extID

			se.Documents[internal] = make(map[string]interface{}, len(doc))
			for k, v := range doc {
				se.Documents[internal][k] = v
			}

			se.mu.Unlock()
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker()
	}

	for i, doc := range docs {
		if i%100_000 == 0 || i == len(docs)-1 {
			fmt.Println("InsertDocs:", i)
		}
		jobs <- doc
	}

	close(jobs)
	wg.Wait()
}

// BuildDocumentIndex tokenizes index fields and updates the inverted index and filters.
func (se *SearchEngine) BuildDocumentIndex(docs []map[string]interface{}) {
	workers := runtime.NumCPU()
	jobs := make(chan map[string]interface{}, workers*2)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()

		for doc := range jobs {
			rawID, ok := doc["id"]
			if !ok || rawID == nil {
				continue
			}
			extID := fmt.Sprintf("%v", rawID)
			if extID == "" || extID == "<nil>" {
				continue
			}

			// resolve current internal id for this external id
			se.mu.RLock()
			internal, ok := se.ExternalToInternal[extID]
			deleted := ok && se.DocDeleted[internal]
			indexFields := se.IndexFields
			filters := se.Filters
			se.mu.RUnlock()

			if !ok || deleted {
				continue
			}

			// ---- tokenize first (no locks) ----
			var allTokens []string
			for _, weightField := range indexFields {
				if value, exists := doc[weightField]; exists {
					switch v := value.(type) {
					case string:
						allTokens = append(allTokens, Tokenize(v)...)
					case []string:
						for _, item := range v {
							allTokens = append(allTokens, Tokenize(item)...)
						}
					case []interface{}:
						for _, item := range v {
							if str, ok := item.(string); ok {
								allTokens = append(allTokens, Tokenize(str)...)
							}
						}
					}
				}
			}

			// ---- index tokens ----
			for _, token := range allTokens {
				se.addToDocumentIndex(token, internal, len(allTokens))
			}

			// ---- filters ----
			for field := range filters {
				if value, exists := doc[field]; exists {
					switch v := value.(type) {
					case int, int8, int16, int32, int64, float32, float64:
						filterKey := fmt.Sprintf("%s:%v", field, v)
						se.mu.Lock()
						if se.FilterDocs[filterKey] == nil {
							se.FilterDocs[filterKey] = make(map[uint32]bool)
						}
						se.FilterDocs[filterKey][internal] = true
						se.mu.Unlock()
					case string:
						filterKey := fmt.Sprintf("%s:%s", field, v)
						se.mu.Lock()
						if se.FilterDocs[filterKey] == nil {
							se.FilterDocs[filterKey] = make(map[uint32]bool)
						}
						se.FilterDocs[filterKey][internal] = true
						se.mu.Unlock()
					}
				}
			}
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker()
	}

	for i, doc := range docs {
		if i%100_000 == 0 || i == len(docs)-1 {
			fmt.Println("BuildDocumentIndex Document:", i)
		}
		jobs <- doc
	}

	close(jobs)
	wg.Wait()
}

// addToDocumentIndex adds/updates a term->internalDocID posting and seeds Keys/SymSpell/Prefix.
func (se *SearchEngine) addToDocumentIndex(term string, internalDocID uint32, length int) {
	if length <= 0 {
		return
	}
	normalizedScore := (100_000 / length)

	se.mu.Lock()
	defer se.mu.Unlock()

	if se.DocDeleted[internalDocID] {
		return
	}

	docMap, ok := se.DataMap[term]
	if !ok {
		se.Keys.Insert(term)
		se.Symspell.AddWord(term)

		if se.Prefix == nil {
			se.Prefix = make(map[string][]string)
		}
		// if len(se.Prefix[term]) < MaxPrefixTerms {
		// 	se.Prefix[term] = append(se.Prefix[term], term)
		// }
		for i := 1; i < len(term); i++ {
			pfx := term[0:i]
			if len(se.Prefix[pfx]) >= MaxPrefixTerms {
				continue
			}
			se.Prefix[pfx] = append(se.Prefix[pfx], term)
		}

		docMap = make(map[uint32]int)
		se.DataMap[term] = docMap
	}

	docMap[internalDocID] += normalizedScore
}

// -------------------- Search --------------------

// func (se *SearchEngine) SearchOneTermWithoutFilter(query string) []ReturnedDocument {
// 	se.mu.RLock()
// 	defer se.mu.RUnlock()

// 	postingsByDoc := se.DataMap[query]
// 	if postingsByDoc == nil {
// 		return nil
// 	}

// 	hits := make([]internalHit, 0)

// 	for internalID, score := range postingsByDoc {
// 		if se.DocDeleted[internalID] {
// 			continue
// 		}

// 		hits = append(hits, internalHit{id: internalID, score: score})
// 	}

// 	if len(hits) == 0 {
// 		return nil
// 	}

// 	sort.Slice(hits, func(i, j int) bool { return hits[i].score > hits[j].score })

// 	end := se.ResultSize
// 	if end > len(hits) {
// 		end = len(hits)
// 	}

// 	out := make([]ReturnedDocument, 0, end)
// 	for _, hit := range hits[0:end] {
// 		extID := se.InternalToExternal[hit.id]
// 		out = append(out, ReturnedDocument{
// 			ID:    extID,
// 			Data:  se.Documents[hit.id],
// 			Score: hit.score,
// 		})
// 	}
// 	return out
// }

// SearchOneTermWithoutFilter retrieves results for a single term.
// Search holds RLock for the whole function to avoid concurrent map read/write panics.
func (se *SearchEngine) SearchOneTermWithoutFilter(query string) []ReturnedDocument {
	se.mu.RLock()
	defer se.mu.RUnlock()

	postings := se.DataMap[query]
	if postings == nil {
		return nil
	}

	k := se.ResultSize
	if k <= 0 {
		return nil
	}

	h := &minHeap{}
	heap.Init(h)

	for id, score := range postings {
		if se.DocDeleted[id] {
			continue
		}

		if h.Len() < k {
			heap.Push(h, internalHit{id: id, score: score})
		} else if (*h)[0].score < score {
			heap.Pop(h)
			heap.Push(h, internalHit{id: id, score: score})
		}
	}

	if h.Len() == 0 {
		return nil
	}

	// extract results (reverse to highest-first)
	n := h.Len()
	out := make([]ReturnedDocument, n)

	for i := n - 1; i >= 0; i-- {
		hit := heap.Pop(h).(internalHit)
		extID := se.InternalToExternal[hit.id]

		out[i] = ReturnedDocument{
			ID:    extID,
			Data:  se.Documents[hit.id],
			Score: hit.score,
		}
	}

	return out
}

// SearchOneTermWithFilter retrieves results for a single term, applying filters.
// If filters is nil/empty, it falls back to SearchOneTermWithoutFilter.
func (se *SearchEngine) SearchOneTermWithFilter(query string, filters map[string][]interface{}) []ReturnedDocument {
	if len(filters) == 0 {
		return se.SearchOneTermWithoutFilter(query)
	}

	allowed := se.ApplyFilter(filters)
	if len(allowed) == 0 {
		return nil
	}

	se.mu.RLock()
	defer se.mu.RUnlock()

	postingsByDoc := se.DataMap[query]
	if postingsByDoc == nil {
		return nil
	}

	hits := make([]internalHit, 0)

	for internalID, score := range postingsByDoc {
		if se.DocDeleted[internalID] {
			continue
		}
		if !allowed[internalID] {
			continue
		}
		hits = append(hits, internalHit{id: internalID, score: score})
	}

	if len(hits) == 0 {
		return nil
	}

	sort.Slice(hits, func(i, j int) bool { return hits[i].score > hits[j].score })

	end := se.ResultSize
	if end > len(hits) {
		end = len(hits)
	}

	out := make([]ReturnedDocument, 0, end)
	for _, hit := range hits[0:end] {
		out = append(out, ReturnedDocument{
			ID:    se.InternalToExternal[hit.id],
			Data:  se.Documents[hit.id],
			Score: hit.score,
		})
	}
	return out
}

// SearchMultipleTermsWithoutFilter executes AND across groups and OR within group.
// Search holds RLock for the whole function to avoid concurrent map read/write panics.
func (se *SearchEngine) SearchMultipleTermsWithoutFilter(termArrList [][]string) []ReturnedDocument {
	if len(termArrList) == 0 {
		return nil
	}

	se.mu.RLock()
	defer se.mu.RUnlock()

	groups := make([][]map[uint32]int, len(termArrList))
	groupSizes := make([]int, len(termArrList))

	for i, terms := range termArrList {
		if len(terms) == 0 {
			return nil
		}

		group := make([]map[uint32]int, 0, len(terms))
		sizeSum := 0

		for _, term := range terms {
			if m := se.DataMap[term]; m != nil {
				group = append(group, m)
				sizeSum += len(m)
			}
		}

		if len(group) == 0 {
			return nil
		}

		groups[i] = group
		groupSizes[i] = sizeSum
	}

	// Choose anchor group (most selective by rough size estimate)
	anchorIdx := 0
	anchorSize := groupSizes[0]
	for i := 1; i < len(groupSizes); i++ {
		if groupSizes[i] < anchorSize {
			anchorSize = groupSizes[i]
			anchorIdx = i
		}
	}
	anchorGroup := groups[anchorIdx]

	visited := make(map[uint32]struct{}, anchorSize)

	hits := make([]internalHit, 0)

	for _, anchorMap := range anchorGroup {
		for internalID, score := range anchorMap {
			if _, seen := visited[internalID]; seen {
				continue
			}
			visited[internalID] = struct{}{}

			if se.DocDeleted[internalID] {
				continue
			}

			total := score
			valid := true

			// AND across groups, OR within group
			for gi, group := range groups {
				if gi == anchorIdx {
					continue
				}

				found := false
				for _, m := range group {
					if s, ok := m[internalID]; ok {
						total += s
						found = true
						break
					}
				}

				if !found {
					valid = false
					break
				}
			}

			if !valid {
				continue
			}

			hits = append(hits, internalHit{id: internalID, score: total})
		}
	}
	if len(hits) == 0 {
		return nil
	}

	sort.Slice(hits, func(i, j int) bool { return hits[i].score > hits[j].score })

	end := se.ResultSize
	if end > len(hits) {
		end = len(hits)
	}

	out := make([]ReturnedDocument, 0, end)
	for _, hit := range hits[0:end] {
		out = append(out, ReturnedDocument{
			ID:    se.InternalToExternal[hit.id],
			Data:  se.Documents[hit.id],
			Score: hit.score,
		})
	}
	return out
}

// SearchMultiTermsWithFilter executes AND across groups and OR within group, applying filters.
// If filters is nil/empty, it falls back to SearchMultipleTermsWithoutFilter.
func (se *SearchEngine) SearchMultiTermsWithFilter(termArrList [][]string, filters map[string][]interface{}) []ReturnedDocument {
	if len(filters) == 0 {
		return se.SearchMultipleTermsWithoutFilter(termArrList)
	}

	allowed := se.ApplyFilter(filters)
	if len(allowed) == 0 {
		return nil
	}

	if len(termArrList) == 0 {
		return nil
	}

	se.mu.RLock()
	defer se.mu.RUnlock()

	groups := make([][]map[uint32]int, len(termArrList))
	groupSizes := make([]int, len(termArrList))

	for i, terms := range termArrList {
		if len(terms) == 0 {
			return nil
		}

		group := make([]map[uint32]int, 0, len(terms))
		sizeSum := 0

		for _, term := range terms {
			if m := se.DataMap[term]; m != nil {
				group = append(group, m)
				sizeSum += len(m)
			}
		}

		if len(group) == 0 {
			return nil
		}

		groups[i] = group
		groupSizes[i] = sizeSum
	}

	// Choose anchor group (most selective by rough size estimate)
	anchorIdx := 0
	anchorSize := groupSizes[0]
	for i := 1; i < len(groupSizes); i++ {
		if groupSizes[i] < anchorSize {
			anchorSize = groupSizes[i]
			anchorIdx = i
		}
	}
	anchorGroup := groups[anchorIdx]

	visited := make(map[uint32]struct{}, anchorSize)
	hits := make([]internalHit, 0)

	for _, anchorMap := range anchorGroup {
		for internalID, score := range anchorMap {
			if _, seen := visited[internalID]; seen {
				continue
			}
			visited[internalID] = struct{}{}

			if !allowed[internalID] {
				continue
			}
			if se.DocDeleted[internalID] {
				continue
			}

			total := score
			valid := true

			// AND across groups, OR within group
			for gi, group := range groups {
				if gi == anchorIdx {
					continue
				}

				found := false
				for _, m := range group {
					if s, ok := m[internalID]; ok {
						total += s
						found = true
						break
					}
				}

				if !found {
					valid = false
					break
				}
			}

			if !valid {
				continue
			}

			hits = append(hits, internalHit{id: internalID, score: total})
		}
	}

	if len(hits) == 0 {
		return nil
	}

	sort.Slice(hits, func(i, j int) bool { return hits[i].score > hits[j].score })

	end := se.ResultSize
	if end > len(hits) {
		end = len(hits)
	}

	out := make([]ReturnedDocument, 0, end)
	for _, hit := range hits[0:end] {
		out = append(out, ReturnedDocument{
			ID:    se.InternalToExternal[hit.id],
			Data:  se.Documents[hit.id],
			Score: hit.score,
		})
	}
	return out
}

// ApplyFilter returns the set of internal docIDs that satisfy the given filters.
// Semantics: OR within a field's values, AND across different fields.
func (se *SearchEngine) ApplyFilter(filters map[string][]interface{}) map[uint32]bool {
	if len(filters) == 0 {
		return nil
	}

	se.mu.RLock()
	defer se.mu.RUnlock()

	var result map[uint32]bool
	firstField := true

	for field, values := range filters {
		fieldUnion := make(map[uint32]bool)
		for _, v := range values {
			key := fmt.Sprintf("%s:%v", field, v)
			docs := se.FilterDocs[key]
			for internalID := range docs {
				if se.DocDeleted[internalID] {
					continue
				}
				fieldUnion[internalID] = true
			}
		}

		if firstField {
			result = fieldUnion
			firstField = false
			continue
		}

		for internalID := range result {
			if !fieldUnion[internalID] {
				delete(result, internalID)
			}
		}

		if len(result) == 0 {
			break
		}
	}

	return result
}

// Search executes a query (single or multi-term), selecting between exact/prefix/fuzzy strategies.
func (se *SearchEngine) Search(query string, filters map[string][]interface{}) *SearchResult {
	queryTokens := Tokenize(query)
	if len(queryTokens) == 0 {
		return nil
	} else if len(queryTokens) == 1 {
		return se.SingleTermSearch(queryTokens, filters)
	} else {
		return se.MultiTermSearch(queryTokens, filters)
	}
}

// SingleTermSearch resolves a single-token query via prefix (preferred) or fuzzy expansions.
// Filter behavior preserved: your original SingleTermSearch ignored filters in else-block.
func (se *SearchEngine) SingleTermSearch(queryTokens []string, filters map[string][]interface{}) *SearchResult {
	parsedQuery := make(map[string][]string)
	maxPrefixTokens := 3
	maxFuzzyTokens := 3

	se.mu.RLock()
	prefixTokens := append([]string(nil), se.Prefix[queryTokens[0]]...)
	se.mu.RUnlock()

	if len(prefixTokens) < maxPrefixTokens {
		maxPrefixTokens = len(prefixTokens)
	}
	guessArr := prefixTokens[:maxPrefixTokens]

	if len(guessArr) > 0 {
		parsedQuery["prefix"] = append(parsedQuery["prefix"], guessArr...)
	} else {
		fuzzyWords := se.Symspell.FuzzySearch(queryTokens[0], maxFuzzyTokens)
		if len(fuzzyWords) > 0 {
			parsedQuery["fuzzy"] = append(parsedQuery["fuzzy"], fuzzyWords...)
		}
	}

	var finalDocs []ReturnedDocument
	if exists := se.Keys.Exists(queryTokens[0]); exists {
		if len(filters) > 0 {
			finalDocs = append(finalDocs, se.SearchOneTermWithFilter(queryTokens[0], filters)...)
		} else {
			finalDocs = append(finalDocs, se.SearchOneTermWithoutFilter(queryTokens[0])...)
		}
	}
	if parsedQuery["prefix"] != nil {
		for _, q := range parsedQuery["prefix"] {
			if len(filters) > 0 {
				finalDocs = append(finalDocs, se.SearchOneTermWithFilter(q, filters)...)
			} else {
				finalDocs = append(finalDocs, se.SearchOneTermWithoutFilter(q)...)
			}
		}
		limit := se.ResultSize
		if len(finalDocs) < limit {
			limit = len(finalDocs)
		}
		return &SearchResult{Docs: finalDocs[0:limit]}
	}
	if parsedQuery["fuzzy"] != nil {
		for _, q := range parsedQuery["fuzzy"] {
			if len(filters) > 0 {
				finalDocs = append(finalDocs, se.SearchOneTermWithFilter(q, filters)...)
			} else {
				finalDocs = append(finalDocs, se.SearchOneTermWithoutFilter(q)...)
			}
		}
		limit := se.ResultSize
		if len(finalDocs) < limit {
			limit = len(finalDocs)
		}
		return &SearchResult{Docs: finalDocs[0:limit]}
	}

	limit := se.ResultSize
	if len(finalDocs) < limit {
		limit = len(finalDocs)
	}
	return &SearchResult{Docs: finalDocs[0:limit]}
}

// MultiTermSearch executes a multi-token query using grouped boolean search strategies.
// Filter behavior preserved (original multi-term path did not apply filters).
// First terms: Exact match + fuzzy(10)
// Last term: Exact match + prefix(40)
func (se *SearchEngine) MultiTermSearch(queryTokens []string, filters map[string][]interface{}) *SearchResult {
	lastQueryIndex := len(queryTokens) - 1
	rawFirstTerms := queryTokens[:lastQueryIndex]
	rawLastTerm := queryTokens[lastQueryIndex]

	termArrList := make([][]string, len(queryTokens))

	for k, firstTerm := range rawFirstTerms {
		if ok := se.Keys.Exists(firstTerm); ok {
			termArrList[k] = []string{firstTerm}
		}
		termArrList[k] = append(termArrList[k], se.Symspell.FuzzySearch(firstTerm, 10)...)
	}

	if ok := se.Keys.Exists(rawLastTerm); ok {
		termArrList[lastQueryIndex] = []string{rawLastTerm}
	}

	se.mu.RLock()
	prefixLen := len(se.Prefix[rawLastTerm])
	maxPrefix := 40
	if maxPrefix > prefixLen {
		maxPrefix = prefixLen
	}
	lastTermGuessArr := append([]string(nil), se.Prefix[rawLastTerm][0:maxPrefix]...)
	se.mu.RUnlock()

	termArrList[lastQueryIndex] = append(termArrList[lastQueryIndex], lastTermGuessArr...)

	var returnedDocuments []ReturnedDocument
	if len(filters) > 0 {
		returnedDocuments = se.SearchMultiTermsWithFilter(termArrList, filters)
	} else {
		returnedDocuments = se.SearchMultipleTermsWithoutFilter(termArrList)
	}

	return &SearchResult{Docs: returnedDocuments}
}

// AddOrUpdateDocument inserts or updates a single document.
// Semantics:
// - If extID is new: assign a new internal ID and index it.
// - If extID exists: mark old internal ID deleted, assign a new one, store+index new version.
//
// This is consistent with your batch InsertDocs/BuildDocumentIndex behavior.
func (se *SearchEngine) AddOrUpdateDocument(doc map[string]interface{}) error {
	if doc == nil {
		return fmt.Errorf("doc is nil")
	}

	rawID, ok := doc["id"]
	if !ok || rawID == nil {
		return fmt.Errorf("doc missing id field")
	}
	extID := fmt.Sprintf("%v", rawID)
	if extID == "" || extID == "<nil>" {
		return fmt.Errorf("invalid id value")
	}

	// 1) Resolve fields used for indexing/filtering (read-lock)
	se.mu.RLock()
	indexFields := append([]string(nil), se.IndexFields...)
	filters := se.Filters
	se.mu.RUnlock()

	// 2) Tokenize without holding locks
	var allTokens []string
	for _, weightField := range indexFields {
		if value, exists := doc[weightField]; exists {
			switch v := value.(type) {
			case string:
				allTokens = append(allTokens, Tokenize(v)...)
			case []string:
				for _, item := range v {
					allTokens = append(allTokens, Tokenize(item)...)
				}
			case []interface{}:
				for _, item := range v {
					if str, ok := item.(string); ok {
						allTokens = append(allTokens, Tokenize(str)...)
					}
				}
			}
		}
	}

	// 3) Create new internal doc + tombstone old (write-lock)
	var internal uint32
	se.mu.Lock()

	// tombstone previous internal doc if exists
	if oldInternal, exists := se.ExternalToInternal[extID]; exists {
		se.DocDeleted[oldInternal] = true
	}

	// assign new internal id
	internal = se.nextInternalID
	se.nextInternalID++

	se.ExternalToInternal[extID] = internal
	se.InternalToExternal[internal] = extID

	// store doc copy
	se.Documents[internal] = make(map[string]interface{}, len(doc))
	for k, v := range doc {
		se.Documents[internal][k] = v
	}

	se.mu.Unlock()

	// 4) Index tokens (this function locks internally per token)
	//    Score normalization uses total token count like your batch version.
	for _, token := range allTokens {
		se.addToDocumentIndex(token, internal, len(allTokens))
	}

	// 5) Update filters (write-lock per key, consistent with batch version)
	//    NOTE: We do not remove old internal IDs from filter sets (tombstone model).
	for field := range filters {
		value, exists := doc[field]
		if !exists {
			continue
		}

		switch v := value.(type) {
		case int, int8, int16, int32, int64, float32, float64:
			filterKey := fmt.Sprintf("%s:%v", field, v)
			se.mu.Lock()
			if se.FilterDocs[filterKey] == nil {
				se.FilterDocs[filterKey] = make(map[uint32]bool)
			}
			se.FilterDocs[filterKey][internal] = true
			se.mu.Unlock()

		case string:
			filterKey := fmt.Sprintf("%s:%s", field, v)
			se.mu.Lock()
			if se.FilterDocs[filterKey] == nil {
				se.FilterDocs[filterKey] = make(map[uint32]bool)
			}
			se.FilterDocs[filterKey][internal] = true
			se.mu.Unlock()
		}
	}

	return nil
}

// DeleteDocument tombstones the currently-active internal doc for the given external doc ID.
// It does NOT remove postings or filter entries; search excludes deleted docs via DocDeleted.
func (se *SearchEngine) DeleteDocument(externalID string) bool {
	if externalID == "" || externalID == "<nil>" {
		return false
	}

	se.mu.Lock()
	defer se.mu.Unlock()

	internal, ok := se.ExternalToInternal[externalID]
	if !ok {
		return false
	}

	se.DocDeleted[internal] = true

	return true
}

// Tokenize splits text into tokens by lowercasing, stripping non-alphanumeric, dropping stopwords.
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
