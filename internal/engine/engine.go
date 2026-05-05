// Package engine implements a lightweight, in-memory search engine
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
	"strings"
	"sync"
	"time"

	"github.com/mg52/search/internal/pkg/keys"
	"github.com/mg52/search/internal/pkg/symspell"
	"github.com/mg52/search/internal/pkg/trie"
)

type internalHit struct {
	id    uint32
	score int
}

// Specialized min-heap over []internalHit. Inlined sift operations avoid the
// interface dispatch and any-boxing overhead of container/heap, which matters
// in the per-document inner loops of search.

// heapPushHit appends hit and sifts it up. Returns the new slice header.
func heapPushHit(h []internalHit, hit internalHit) []internalHit {
	h = append(h, hit)
	i := len(h) - 1
	for i > 0 {
		parent := (i - 1) >> 1
		if h[parent].score <= h[i].score {
			break
		}
		h[parent], h[i] = h[i], h[parent]
		i = parent
	}
	return h
}

// heapReplaceTop overwrites the root and sifts it down — one operation
// instead of pop+push when the heap is already at capacity. len(h) > 0.
func heapReplaceTop(h []internalHit, hit internalHit) {
	h[0] = hit
	siftDownHit(h, 0, len(h))
}

func siftDownHit(h []internalHit, start, n int) {
	i := start
	for {
		left := 2*i + 1
		if left >= n {
			return
		}
		smallest := left
		if right := left + 1; right < n && h[right].score < h[left].score {
			smallest = right
		}
		if h[i].score <= h[smallest].score {
			return
		}
		h[i], h[smallest] = h[smallest], h[i]
		i = smallest
	}
}

// ---- filter bitset helpers ----
// Bitsets are indexed by internalDocID: word = id>>6, bit = id&63.
// filterBitSet grows bits as needed and sets the bit for id.
func filterBitSet(bits []uint64, id uint32) []uint64 {
	word := id >> 6
	for uint32(len(bits)) <= word {
		bits = append(bits, 0)
	}
	bits[word] |= 1 << (id & 63)
	return bits
}

func filterBitTest(bits []uint64, id uint32) bool {
	word := id >> 6
	return uint32(len(bits)) > word && bits[word]&(1<<(id&63)) != 0
}

// filterBitOr returns a new bitset that is the union of a and b.
func filterBitOr(a, b []uint64) []uint64 {
	if len(b) > len(a) {
		a, b = b, a
	}
	out := make([]uint64, len(a))
	copy(out, a)
	for i := range b {
		out[i] |= b[i]
	}
	return out
}

// filterBitAnd returns a new bitset that is the intersection of a and b.
func filterBitAnd(a, b []uint64) []uint64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = a[i] & b[i]
	}
	return out
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
	FilterBits  map[string][]uint64               // "field:value" -> bitset of internalDocIDs
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
		FilterBits:         make(map[string][]uint64),
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
		FilterBits:         make(map[string][]uint64),
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

		// 3) Rebuild filter bitsets
		for field := range se.Filters {
			val, ok := doc[field]
			if !ok {
				continue
			}

			var filterKey string
			switch val.(type) {
			case int, int8, int16, int32, int64, float32, float64:
				filterKey = fmt.Sprintf("%s:%v", field, val)
			case string:
				filterKey = fmt.Sprintf("%s:%s", field, val)
			default:
				continue
			}

			se.mu.Lock()
			se.FilterBits[filterKey] = filterBitSet(se.FilterBits[filterKey], internalID)
			se.mu.Unlock()
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
				value, exists := doc[field]
				if !exists {
					continue
				}

				var filterKey string
				switch value.(type) {
				case int, int8, int16, int32, int64, float32, float64:
					filterKey = fmt.Sprintf("%s:%v", field, value)
				case string:
					filterKey = fmt.Sprintf("%s:%s", field, value)
				default:
					continue
				}

				se.mu.Lock()
				se.FilterBits[filterKey] = filterBitSet(se.FilterBits[filterKey], internal)
				se.mu.Unlock()
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

// SearchOneTerm returns the top-k matching documents for a single term, ranked
// by score descending. If filters is non-empty, only documents passing the
// filter are considered. The whole function holds RLock to avoid concurrent
// map read/write panics with index updates.
func (se *SearchEngine) SearchOneTerm(query string, filters map[string][]interface{}) []ReturnedDocument {
	se.mu.RLock()
	defer se.mu.RUnlock()

	var allowed []uint64
	if len(filters) > 0 {
		allowed = se.applyFilterLocked(filters)
		if allowed == nil {
			return nil
		}
	}

	postings := se.DataMap[query]
	if len(postings) == 0 {
		return nil
	}

	k := se.ResultSize
	if k <= 0 {
		return nil
	}

	deleted := se.DocDeleted
	h := make([]internalHit, 0, k)

	for id, score := range postings {
		if deleted[id] {
			continue
		}
		if allowed != nil && !filterBitTest(allowed, id) {
			continue
		}

		if len(h) < k {
			h = heapPushHit(h, internalHit{id: id, score: score})
		} else if h[0].score < score {
			heapReplaceTop(h, internalHit{id: id, score: score})
		}
	}

	n := len(h)
	if n == 0 {
		return nil
	}

	// Extract in descending order via repeated heap-pop. Each pop yields the
	// minimum, so we fill out[n-1], out[n-2], ... out[0]. Pop is inlined: swap
	// root with last, sift down over the shrinking prefix.
	extMap := se.InternalToExternal
	docs := se.Documents
	out := make([]ReturnedDocument, n)
	for i := n - 1; i >= 0; i-- {
		hit := h[0]
		if i > 0 {
			h[0] = h[i]
			siftDownHit(h, 0, i)
		}
		out[i] = ReturnedDocument{
			ID:    extMap[hit.id],
			Data:  docs[hit.id],
			Score: hit.score,
		}
	}
	return out
}

// SearchMultiTerms returns the top-k matching documents for a multi-term query
// expressed as groups of synonyms. Semantics: AND across groups, OR within a
// group. If filters is non-empty, only documents passing the filter are
// considered. RLock is held for the whole function.
func (se *SearchEngine) SearchMultiTerms(termArrList [][]string, filters map[string][]interface{}) []ReturnedDocument {
	if len(termArrList) == 0 {
		return nil
	}

	se.mu.RLock()
	defer se.mu.RUnlock()

	var allowed []uint64
	if len(filters) > 0 {
		allowed = se.applyFilterLocked(filters)
		if allowed == nil {
			return nil
		}
	}

	k := se.ResultSize
	if k <= 0 {
		return nil
	}

	dataMap := se.DataMap
	groups := make([][]map[uint32]int, len(termArrList))
	groupSizes := make([]int, len(termArrList))

	for i, terms := range termArrList {
		if len(terms) == 0 {
			return nil
		}

		group := make([]map[uint32]int, 0, len(terms))
		sizeSum := 0

		for _, term := range terms {
			if m := dataMap[term]; m != nil {
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

	// Anchor on the smallest group — fewest candidates to enumerate.
	anchorIdx := 0
	anchorSize := groupSizes[0]
	for i := 1; i < len(groupSizes); i++ {
		if groupSizes[i] < anchorSize {
			anchorSize = groupSizes[i]
			anchorIdx = i
		}
	}
	anchorGroup := groups[anchorIdx]

	// Dedup is only needed when the anchor group has multiple posting maps
	// (synonyms can overlap on the same docID). One map = no overlap possible.
	var visited map[uint32]struct{}
	if len(anchorGroup) > 1 {
		visited = make(map[uint32]struct{}, anchorSize)
	}

	deleted := se.DocDeleted
	h := make([]internalHit, 0, k)

	for _, anchorMap := range anchorGroup {
		for internalID, score := range anchorMap {
			if visited != nil {
				if _, seen := visited[internalID]; seen {
					continue
				}
				visited[internalID] = struct{}{}
			}

			if deleted[internalID] {
				continue
			}
			if allowed != nil && !filterBitTest(allowed, internalID) {
				continue
			}

			total := score
			valid := true

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

			if len(h) < k {
				h = heapPushHit(h, internalHit{id: internalID, score: total})
			} else if h[0].score < total {
				heapReplaceTop(h, internalHit{id: internalID, score: total})
			}
		}
	}

	n := len(h)
	if n == 0 {
		return nil
	}

	extMap := se.InternalToExternal
	docs := se.Documents
	out := make([]ReturnedDocument, n)
	for i := n - 1; i >= 0; i-- {
		hit := h[0]
		if i > 0 {
			h[0] = h[i]
			siftDownHit(h, 0, i)
		}
		out[i] = ReturnedDocument{
			ID:    extMap[hit.id],
			Data:  docs[hit.id],
			Score: hit.score,
		}
	}
	return out
}

// applyFilterLocked resolves filters to a bitset without acquiring any lock.
// Caller must hold se.mu.RLock for the entire time the returned slice is used.
//
// Fast path (single field, single value): returns a direct reference into
// se.FilterBits — zero allocation. Multi-value OR and multi-field AND still
// allocate intermediate bitsets, but those cases are uncommon.
func (se *SearchEngine) applyFilterLocked(filters map[string][]interface{}) []uint64 {
	var result []uint64
	first := true

	for field, values := range filters {
		var fieldBits []uint64

		if len(values) == 1 {
			// Fast path: direct reference, no copy.
			key := fmt.Sprintf("%s:%v", field, values[0])
			fieldBits = se.FilterBits[key]
		} else {
			// Multi-value OR: must build a union (allocates once).
			for _, v := range values {
				key := fmt.Sprintf("%s:%v", field, v)
				bits := se.FilterBits[key]
				if len(bits) == 0 {
					continue
				}
				if fieldBits == nil {
					fieldBits = append([]uint64(nil), bits...)
				} else {
					fieldBits = filterBitOr(fieldBits, bits)
				}
			}
		}

		if len(fieldBits) == 0 {
			return nil
		}

		if first {
			result = fieldBits
			first = false
		} else {
			result = filterBitAnd(result, fieldBits)
			hasAny := false
			for _, w := range result {
				if w != 0 {
					hasAny = true
					break
				}
			}
			if !hasAny {
				return nil
			}
		}
	}

	return result
}

// ApplyFilter returns a bitset of internal docIDs that satisfy the given filters.
// Semantics: OR within a field's values, AND across different fields.
// Returns nil when filters produce no matches (or filters map is empty).
// For internal search paths use applyFilterLocked to avoid the copy.
func (se *SearchEngine) ApplyFilter(filters map[string][]interface{}) []uint64 {
	if len(filters) == 0 {
		return nil
	}

	se.mu.RLock()
	defer se.mu.RUnlock()

	var result []uint64
	first := true

	for field, values := range filters {
		var fieldUnion []uint64
		for _, v := range values {
			key := fmt.Sprintf("%s:%v", field, v)
			bits := se.FilterBits[key]
			if len(bits) == 0 {
				continue
			}
			if fieldUnion == nil {
				fieldUnion = append([]uint64(nil), bits...)
			} else {
				fieldUnion = filterBitOr(fieldUnion, bits)
			}
		}

		if fieldUnion == nil {
			return nil
		}

		if first {
			result = fieldUnion
			first = false
		} else {
			result = filterBitAnd(result, fieldUnion)
			hasAny := false
			for _, w := range result {
				if w != 0 {
					hasAny = true
					break
				}
			}
			if !hasAny {
				return nil
			}
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
		finalDocs = append(finalDocs, se.SearchOneTerm(queryTokens[0], filters)...)
	}
	if parsedQuery["prefix"] != nil {
		for _, q := range parsedQuery["prefix"] {
			finalDocs = append(finalDocs, se.SearchOneTerm(q, filters)...)
		}
		limit := se.ResultSize
		if len(finalDocs) < limit {
			limit = len(finalDocs)
		}
		return &SearchResult{Docs: finalDocs[0:limit]}
	}
	if parsedQuery["fuzzy"] != nil {
		for _, q := range parsedQuery["fuzzy"] {
			finalDocs = append(finalDocs, se.SearchOneTerm(q, filters)...)
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
// First terms: exact match + fuzzy(10). Last term: exact match + prefix(40).
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

	return &SearchResult{Docs: se.SearchMultiTerms(termArrList, filters)}
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

	// 5) Update filter bitsets (tombstone model: old internal IDs are never cleared).
	for field := range filters {
		value, exists := doc[field]
		if !exists {
			continue
		}

		var filterKey string
		switch value.(type) {
		case int, int8, int16, int32, int64, float32, float64:
			filterKey = fmt.Sprintf("%s:%v", field, value)
		case string:
			filterKey = fmt.Sprintf("%s:%s", field, value)
		default:
			continue
		}

		se.mu.Lock()
		se.FilterBits[filterKey] = filterBitSet(se.FilterBits[filterKey], internal)
		se.mu.Unlock()
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
