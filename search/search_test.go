package search

import (
	_ "embed"
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
)

//go:embed test_data/products.json
var testProductsJSON []byte

func TestOneTermSearchWithFilter(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	indexFields := []string{"name", "tags"}
	filters := map[string]bool{"year": true}
	sec := NewSearchEngineController(indexFields, filters, 10, 3)
	sec.Index(docs)

	filter := make(map[string][]interface{})
	filter["year"] = append(filter["year"], 2015)
	result := sec.Search("optimal", 0, filter)

	if len(result) != 4 {
		t.Errorf("Expected search result is 4, got %d", len(result))
	}
	if result[0].ID != "15" || result[0].ScoreWeight != 50000 {
		t.Errorf("Expected 1. document ID is 15, score is 50000, got ID: %v, Score: %d",
			result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "16" || result[1].ScoreWeight != 33333 {
		t.Errorf("Expected 2. document ID is 16, score is 33333, got ID: %v, Score: %d",
			result[1].ID, result[1].ScoreWeight)
	}
	if result[2].ID != "17" || result[2].ScoreWeight != 25000 {
		t.Errorf("Expected 3. document ID is 17, score is 25000, got ID: %v, Score: %d",
			result[2].ID, result[2].ScoreWeight)
	}
	if result[3].ID != "14" || result[3].ScoreWeight != 20000 {
		t.Errorf("Expected 4. document ID is 14, score is 20000, got ID: %v, Score: %d",
			result[3].ID, result[3].ScoreWeight)
	}
}

func TestOneTermSearchWithoutFilter(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	indexFields := []string{"name", "tags"}
	filters := map[string]bool{}
	sec := NewSearchEngineController(indexFields, filters, 10, 3)
	sec.Index(docs)

	filter := make(map[string][]interface{})
	result := sec.Search("optimal", 0, filter)

	if len(result) != 7 {
		t.Errorf("Expected search result is 7, got %d", len(result))
	}
	// 1st element must be fixed
	if result[0].ID != "15" || result[0].ScoreWeight != 50000 {
		t.Errorf("Expected document 1 to have ID 15 and score 50000, got ID %v, score %d",
			result[0].ID, result[0].ScoreWeight)
	}

	// Positions 2–5 can be any order of IDs 19, 16, 18, 13 but all must have score 33333
	expected := map[string]bool{
		"19": false,
		"16": false,
		"18": false,
		"13": false,
	}
	for i := 1; i <= 4; i++ {
		doc := result[i]
		if doc.ScoreWeight != 33333 {
			t.Errorf("Expected document %d to have score 33333, got %d", i+1, doc.ScoreWeight)
		}
		if _, ok := expected[doc.ID]; !ok {
			t.Errorf("Unexpected ID %v at position %d; expected one of [19,16,18,13]",
				doc.ID, i+1)
		}
		expected[doc.ID] = true
	}
	// Verify none of the expected IDs were missing
	for id, seen := range expected {
		if !seen {
			t.Errorf("Expected ID %v in positions 2–5 but it was missing", id)
		}
	}

	// 6th element must be ID 17 with score 25000
	if result[5].ID != "17" || result[5].ScoreWeight != 25000 {
		t.Errorf("Expected document 6 to have ID 17 and score 25000, got ID %v, score %d",
			result[5].ID, result[5].ScoreWeight)
	}

	// 7th element must be ID 14 with score 20000
	if result[6].ID != "14" || result[6].ScoreWeight != 20000 {
		t.Errorf("Expected document 7 to have ID 14 and score 20000, got ID %v, score %d",
			result[6].ID, result[6].ScoreWeight)
	}
}

func TestUpdateDocument(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	indexFields := []string{"name", "tags"}
	filters := map[string]bool{"year": true}
	sec := NewSearchEngineController(indexFields, filters, 10, 5)
	sec.Index(docs)

	filter := make(map[string][]interface{})
	filter["year"] = append(filter["year"], 2015)

	// Search before updating
	result := sec.Search("optimal", 0, filter)

	if len(result) != 4 {
		t.Errorf("Expected search result is 4, got %d", len(result))
	}
	if result[0].ID != "15" || result[0].ScoreWeight != 50000 {
		t.Errorf("Expected 1. document ID is 15, score is 50000, got ID: %v, Score: %d",
			result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "16" || result[1].ScoreWeight != 33333 {
		t.Errorf("Expected 2. document ID is 16, score is 33333, got ID: %v, Score: %d",
			result[1].ID, result[1].ScoreWeight)
	}
	if result[2].ID != "17" || result[2].ScoreWeight != 25000 {
		t.Errorf("Expected 3. document ID is 17, score is 25000, got ID: %v, Score: %d",
			result[2].ID, result[2].ScoreWeight)
	}
	if result[3].ID != "14" || result[3].ScoreWeight != 20000 {
		t.Errorf("Expected 4. document ID is 14, score is 20000, got ID: %v, Score: %d",
			result[3].ID, result[3].ScoreWeight)
	}

	updatedData := map[string]interface{}{
		"id":          "15",
		"name":        "Affordable book good",
		"description": "example during affordable break disruptive sent square cloud dress robust gone weight paper hold.",
		"tags":        []string{"optimal", "sleek", "new"},
		"year":        2015,
	}

	sec.AddOrUpdateDocument(updatedData)

	// Search after removing
	filter["year"] = append(filter["year"], 2015)
	result = sec.Search("optimal", 0, filter)

	if len(result) != 4 {
		t.Errorf("Expected search result is 4, got %d", len(result))
	}
	if result[0].ID != "16" || result[0].ScoreWeight != 33333 {
		t.Errorf("Expected 1. document ID is 16, score is 33333, got ID: %v, Score: %d",
			result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "17" || result[1].ScoreWeight != 25000 {
		t.Errorf("Expected 2. document ID is 17, score is 25000, got ID: %v, Score: %d",
			result[1].ID, result[1].ScoreWeight)
	}
	if result[2].ID != "14" || result[2].ScoreWeight != 20000 {
		t.Errorf("Expected 3. document ID is 14, score is 20000, got ID: %v, Score: %d",
			result[2].ID, result[2].ScoreWeight)
	}
	if result[3].ID != "15" || result[3].ScoreWeight != 16666 {
		t.Errorf("Expected 4. document ID is 15, score is 16666, got ID: %v, Score: %d",
			result[3].ID, result[3].ScoreWeight)
	}
}

func TestSearchEngineControllerFlow(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	// Create controller
	filtersConfig := map[string]bool{"year": true}
	sec := NewSearchEngineController(
		[]string{"name", "tags"},
		filtersConfig,
		10, // pageCount
		1,  // workers
	)
	sec.Index(docs)

	emptyFilters := make(map[string][]interface{})

	// 1) Search "kalemi" before update → expect no results
	res := sec.Search("kalemi", 0, emptyFilters)
	if len(res) != 0 {
		t.Errorf("expected 0 results for 'kalemi' before update, got %d: %#v", len(res), res)
	}

	// 2) Search "afford" before update → length and map-based check
	res = sec.Search("afford", 0, emptyFilters)
	if len(res) != 7 {
		t.Fatalf("expected 7 results for 'afford' before update, got %d", len(res))
	}
	expectedBefore := map[string]int{
		"11": 66666,
		"12": 50000,
		"15": 50000,
		"14": 40000,
		"13": 33333,
		"19": 33333,
		"17": 25000,
	}
	for _, doc := range res {
		wantScore, ok := expectedBefore[doc.ID]
		if !ok {
			t.Errorf("unexpected ID %q in 'afford' before update", doc.ID)
			continue
		}
		if doc.ScoreWeight != wantScore {
			t.Errorf("for ID %q expected score %d, got %d", doc.ID, wantScore, doc.ScoreWeight)
		}
		delete(expectedBefore, doc.ID)
	}
	if len(expectedBefore) != 0 {
		t.Errorf("missing docs in 'afford' before update: %+v", expectedBefore)
	}

	// 3) Update document ID "15"
	updatedDoc := map[string]interface{}{
		"id":          "15",
		"name":        "abece Affordable kalemite asdasdfsf",
		"description": "get value segment try week great real end high image ergonomic broad pass beat.",
		"tags":        []interface{}{"section", "rose", "sadvde435r"},
		"year":        2015,
	}
	sec.AddOrUpdateDocument(updatedDoc)

	// 4) Search "afford" after update → length and map-based check
	res = sec.Search("afford", 0, emptyFilters)
	if len(res) != 7 {
		t.Fatalf("expected 7 results for 'afford' after update, got %d", len(res))
	}
	expectedAfter := map[string]int{
		"11": 66666,
		"12": 50000,
		"14": 40000,
		"13": 33333,
		"19": 33333,
		"17": 25000,
		"15": 14285,
	}
	for _, doc := range res {
		wantScore, ok := expectedAfter[doc.ID]
		if !ok {
			t.Errorf("unexpected ID %q in 'afford' after update", doc.ID)
			continue
		}
		if doc.ScoreWeight != wantScore {
			t.Errorf("for ID %q expected score %d, got %d", doc.ID, wantScore, doc.ScoreWeight)
		}
		delete(expectedAfter, doc.ID)
	}
	if len(expectedAfter) != 0 {
		t.Errorf("missing docs in 'afford' after update: %+v", expectedAfter)
	}

	// 5) Search "kalemi" after update → expect exactly one result
	res = sec.Search("kalemi", 0, emptyFilters)
	if len(res) != 1 {
		t.Fatalf("expected 1 result for 'kalemi' after update, got %d", len(res))
	}
	if res[0].ID != "15" || res[0].ScoreWeight != 14285 {
		t.Errorf("for 'kalemi' after update expected {ID:\"15\",ScoreWeight:14285}, got %+v", res[0])
	}
}

func setupEngineWithKeys(keys []string) *SearchEngine {
	// Initialize engine
	se := &SearchEngine{
		Keys: NewKeys(),
		Trie: NewTrie(),
	}
	// Insert each key into Keys and Trie
	for _, k := range keys {
		se.Keys.Insert(k)
		se.Trie.Insert(k)
	}
	return se
}

func TestProcessQuery(t *testing.T) {
	tests := []struct {
		name     string
		keys     []string
		query    string
		expected map[string][]string
	}{
		{
			name:     "empty query",
			keys:     []string{"a", "b"},
			query:    "",
			expected: nil,
		},
		{
			name:  "exact match",
			keys:  []string{"foo", "bar"},
			query: "foo",
			expected: map[string][]string{
				"exact": {"foo"},
			},
		},
		{
			name:  "prefix match",
			keys:  []string{"apple", "apricot", "banana"},
			query: "ap",
			expected: map[string][]string{
				"prefix": {"apple", "apricot"},
			},
		},
		{
			name:  "fuzzy match within tolerance",
			keys:  []string{"kitten"},
			query: "kiten", // one deletion away
			expected: map[string][]string{
				"fuzzy": {"kitten"},
			},
		},
		{
			name:  "no match falls back to fuzzy with original",
			keys:  []string{"apple"},
			query: "xyz",
			expected: map[string][]string{
				"fuzzy": {"xyz"},
			},
		},
		{
			name:  "multiple tokens mixed types",
			keys:  []string{"apple", "apricot", "foo", "kite"},
			query: "ap foo ki",
			expected: map[string][]string{
				"exact":  {"foo"},
				"prefix": {"apple", "apricot", "kite"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se := setupEngineWithKeys(tt.keys)
			got := se.ProcessQuery(tt.query)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("ProcessQuery(%q):\n got: %#v\nwant: %#v", tt.query, got, tt.expected)
			}
		})
	}
}

func TestAddOrUpdateDocumentAndRemove(t *testing.T) {
	// seed rng so NewDoc goes to deterministic shard
	rand.Seed(42)

	ctrl := NewSearchEngineController(nil, nil, 10, 2)

	// 1) Add new document
	doc := map[string]interface{}{"id": "doc1", "foo": "bar"}
	ctrl.AddOrUpdateDocument(doc)

	// exactly one engine should contain it
	found := 0
	for _, eng := range ctrl.Engines {
		eng.mu.RLock()
		_, ok := eng.Documents["doc1"]
		eng.mu.RUnlock()
		if ok {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("expected doc1 in exactly 1 shard, found in %d", found)
	}

	// remember which one
	var idx int
	for i, eng := range ctrl.Engines {
		eng.mu.RLock()
		if _, ok := eng.Documents["doc1"]; ok {
			idx = i
		}
		eng.mu.RUnlock()
	}

	// 2) Update the same document
	updated := map[string]interface{}{"id": "doc1", "foo": "baz"}
	ctrl.AddOrUpdateDocument(updated)

	// still exactly one, and its contents should match updated
	found = 0
	for i, eng := range ctrl.Engines {
		eng.mu.RLock()
		val, ok := eng.Documents["doc1"]
		eng.mu.RUnlock()
		if ok {
			found++
			if i != idx {
				t.Errorf("expected update in same shard %d, got %d", idx, i)
			}
			if !reflect.DeepEqual(val, updated) {
				t.Errorf("shard %d: expected %+v, got %+v", i, updated, val)
			}
		}
	}
	if found != 1 {
		t.Fatalf("after update, expected doc1 in exactly 1 shard, found in %d", found)
	}

	// 3) RemoveDocumentByID should delete from all shards
	ctrl.RemoveDocumentByID("doc1")
	for i, eng := range ctrl.Engines {
		eng.mu.RLock()
		_, ok := eng.Documents["doc1"]
		eng.mu.RUnlock()
		if ok {
			t.Errorf("shard %d: expected doc1 removed, but still present", i)
		}
	}
}

func TestCombineResults(t *testing.T) {
	// helper to build SearchResult
	mk := func(isMulti, isExact, isPrefix, isFuzzy bool, docs ...Document) *SearchResult {
		return &SearchResult{
			IsMultiTerm: isMulti,
			IsExact:     isExact,
			IsPrefix:    isPrefix,
			IsFuzzy:     isFuzzy,
			Docs:        docs,
		}
	}
	// build some documents
	a := Document{"A", 10}
	b := Document{"B", 20}
	c := Document{"C", 5}

	tests := []struct {
		name     string
		input    []*SearchResult
		expected []Document
	}{
		{
			"multi wins",
			[]*SearchResult{
				mk(true, false, false, false, a, b),
				mk(false, true, false, false, c),
			},
			[]Document{b, a}, // sorted by weight desc
		},
		{
			"exact next",
			[]*SearchResult{
				mk(false, true, false, false, a),
				mk(false, false, true, false, b),
			},
			[]Document{a},
		},
		{
			"prefix next",
			[]*SearchResult{
				mk(false, false, true, false, b, c),
			},
			[]Document{b, c},
		},
		{
			"fuzzy fallback",
			[]*SearchResult{
				mk(false, false, false, true, c),
			},
			[]Document{c},
		},
		{
			"empty yields nil",
			[]*SearchResult{
				nil,
			},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan *SearchResult, len(tt.input))
			for _, r := range tt.input {
				ch <- r
			}
			close(ch)
			got := CombineResults(ch)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("CombineResults = %+v, want %+v", got, tt.expected)
			}
		})
	}
}

// TestAddToDocumentIndex verifies that addToDocumentIndex correctly updates Data, Keys, and Trie.
func TestAddToDocumentIndex(t *testing.T) {
	// create a fresh SearchEngine
	se := NewSearchEngine([]string{"name"}, nil, 10, 0)

	// add the same term twice with length=2
	se.addToDocumentIndex("foo", "1", 2)
	se.addToDocumentIndex("foo", "1", 2)

	// normalized weight = 100000/2 = 50000, twice = 100000
	wantWeight := 50000 * 2
	gotWeight := se.Data["foo"]["1"]
	if gotWeight != wantWeight {
		t.Errorf("expected Data[\"foo\"][\"1\"]= %d, got %d", wantWeight, gotWeight)
	}

	// Keys should contain "foo"
	if _, ok := se.Keys.GetData()["foo"]; !ok {
		t.Error("expected Keys to contain \"foo\"")
	}

	// Trie should return "foo" on prefix "f"
	pref := se.Trie.SearchPrefix("f")
	if !reflect.DeepEqual(pref, []string{"foo"}) {
		t.Errorf("expected Trie.SearchPrefix(\"f\")==[\"foo\"], got %v", pref)
	}
}

// TestBuildDocumentIndex verifies that BuildDocumentIndex tokenizes, indexes weights, and sets up filters.
func TestBuildDocumentIndex(t *testing.T) {
	// set up a SearchEngine that indexes "name" and "tags", with a "year" filter
	indexFields := []string{"name", "tags"}
	filters := map[string]bool{"year": true}
	se := NewSearchEngine(indexFields, filters, 10, 0)

	// one document with id "1":
	// name => "foo bar" → tokens ["foo","bar"]
	// tags => []interface{}{"baz"} → tokens ["baz"]
	// total tokens = 3 → normalized weight = 100000/3 = 33333
	// year => 2020.0 → filterKey "year:2020"
	docs := []map[string]interface{}{{
		"id":   "1",
		"name": "foo bar",
		"tags": []interface{}{"baz"},
		"year": 2020.0,
	}}

	se.BuildDocumentIndex(docs)

	// check Data weights
	wantData := map[string]int{
		"foo": 33333,
		"bar": 33333,
		"baz": 33333,
	}
	for term, want := range wantData {
		got := se.Data[term]["1"]
		if got != want {
			t.Errorf("Data[%q][\"1\"] = %d; want %d", term, got, want)
		}
	}

	// check Keys contains each term
	for term := range wantData {
		if _, ok := se.Keys.GetData()[term]; !ok {
			t.Errorf("Keys missing term %q", term)
		}
	}

	// check Trie can find each term via SearchPrefix
	for term := range wantData {
		pref := se.Trie.SearchPrefix(string(term[0]))
		found := false
		for _, tkn := range pref {
			if tkn == term {
				found = true
			}
		}
		if !found {
			t.Errorf("Trie.SearchPrefix(%q) did not include %q; got %v", string(term[0]), term, pref)
		}
	}

	// check FilterDocs
	wantFilter := "year:2020"
	if ds, ok := se.FilterDocs[wantFilter]; !ok {
		t.Errorf("FilterDocs missing key %q", wantFilter)
	} else if !ds["1"] {
		t.Errorf("FilterDocs[%q][\"1\"] = false; want true", wantFilter)
	}
}
