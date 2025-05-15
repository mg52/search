package engine

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	searchkeys "github.com/mg52/search/internal/pkg/keys"
	"github.com/mg52/search/internal/pkg/trie"
)

//go:embed testdata/products.json
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
		4,  // workers
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
		Keys: searchkeys.NewKeys(),
		Trie: trie.NewTrie(),
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

func TestSaveAllShards_WriteToDisk(t *testing.T) {
	// Prepare a temporary base directory and symlink it to /data
	tmp := t.TempDir()
	// Clean up any existing /data
	_ = os.RemoveAll("/data")
	// Symlink /data -> tmp
	if err := os.Symlink(tmp, "/data"); err != nil {
		t.Skipf("Unable to create /data symlink: %v", err)
	}
	defer os.Remove("/data")

	// Create a controller with 2 shards
	ctrl := NewSearchEngineController([]string{"name"}, nil, 1, 2)
	// Index a dummy document so SaveAll writes real payload
	docs := []map[string]interface{}{{"id": "doc1", "name": "foo"}}
	ctrl.Index(docs)

	// Call SaveAllShards
	idx := "testidx"
	if err := ctrl.SaveAllShards(idx); err != nil {
		t.Fatalf("SaveAllShards failed: %v", err)
	}

	// Check that /data/testidx/shard-0.engine.gob exists
	for i := 0; i < 2; i++ {
		path := filepath.Join(tmp, idx, "shard-"+fmt.Sprint(i)+".engine.gob")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected file %s, got error: %v", path, err)
		}
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

// helper to create a temporary engine and index some docs
func buildTestEngine(t *testing.T) *SearchEngine {
	// two fields indexed, one filter on "year"
	engine := NewSearchEngine(
		[]string{"name", "tags"},
		map[string]bool{"year": true},
		2, // pageSize
		0, // shardID unused
	)
	docs := []map[string]interface{}{
		{"id": "1", "name": "apple banana", "tags": []interface{}{"fruit"}, "year": 2020},
		{"id": "2", "name": "banana cherry", "tags": []interface{}{"fruit"}, "year": 2021},
		{"id": "3", "name": "cherry date", "tags": []interface{}{"dry"}, "year": 2020},
	}
	engine.Index(0, docs)
	return engine
}

func TestSaveLoadAll_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	prefix := filepath.Join(dir, "engine")
	engine := buildTestEngine(t)

	// save
	if err := engine.SaveAll(prefix); err != nil {
		t.Fatalf("SaveAll failed: %v", err)
	}
	// must have .engine.gob
	if _, err := os.Stat(prefix + ".engine.gob"); err != nil {
		t.Fatalf("expected engine file, got: %v", err)
	}

	// load
	loaded, err := LoadAll(prefix)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	// compare Data maps
	if !reflect.DeepEqual(loaded.Data, engine.Data) {
		t.Errorf("Data mismatch\ngot:  %+v\nwant: %+v", loaded.Data, engine.Data)
	}
	// compare Documents
	if !reflect.DeepEqual(loaded.Documents, engine.Documents) {
		t.Errorf("Documents mismatch\ngot:  %+v\nwant: %+v", loaded.Documents, engine.Documents)
	}
	// compare ScoreIndex keys
	if !reflect.DeepEqual(loaded.ScoreIndex, engine.ScoreIndex) {
		t.Errorf("ScoreIndex mismatch\ngot:  %+v\nwant: %+v", loaded.ScoreIndex, engine.ScoreIndex)
	}
	// compare FilterDocs
	if !reflect.DeepEqual(loaded.FilterDocs, engine.FilterDocs) {
		t.Errorf("FilterDocs mismatch\ngot:  %+v\nwant: %+v", loaded.FilterDocs, engine.FilterDocs)
	}
}

func TestSearchOneTermWithoutFilter(t *testing.T) {
	engine := buildTestEngine(t)
	// "banana" appears in both doc1 and doc2, same weight => order not guaranteed
	res := engine.SearchOneTermWithoutFilter("banana", 0)
	if len(res) != 2 {
		t.Fatalf("expected 2 results for banana, got %d", len(res))
	}
	seen := map[string]bool{res[0].ID: true, res[1].ID: true}
	if !seen["1"] || !seen["2"] {
		t.Errorf("banana search missing docs, got %v", res)
	}
}

func TestApplyFilter(t *testing.T) {
	engine := buildTestEngine(t)
	// filter year=2020 should select doc1 and doc3
	m := engine.ApplyFilter(map[string][]interface{}{"year": {2020}})
	want := map[string]bool{"1": true, "3": true}
	if !reflect.DeepEqual(m, want) {
		t.Errorf("ApplyFilter got %v, want %v", m, want)
	}
}

func TestSearchOneTermWithFilter(t *testing.T) {
	engine := buildTestEngine(t)
	// filter year=2020 and search "cherry": doc3 only
	res := engine.SearchOneTermWithFilter("cherry", map[string][]interface{}{"year": {2020}}, 0)
	if len(res) != 1 || res[0].ID != "3" {
		t.Errorf("expected doc3, got %v", res)
	}
}

func TestSearchMultipleTermsWithoutFilter(t *testing.T) {
	engine := buildTestEngine(t)
	// multi-term: exact matches ["apple","date"] → only doc3 has date but not apple; doc1 has apple but not date → none
	queries := map[string][]string{
		"exact":  {"apple", "date"},
		"prefix": nil,
		"fuzzy":  nil,
	}
	res := engine.SearchMultipleTermsWithoutFilter(queries, 0)
	if len(res) != 0 {
		t.Errorf("expected no docs, got %v", res)
	}
	// two terms ["banana","cherry"] → doc2 matches both
	queries["exact"] = []string{"banana", "cherry"}
	res = engine.SearchMultipleTermsWithoutFilter(queries, 0)
	if len(res) != 1 || res[0].ID != "2" {
		t.Errorf("expected doc2, got %v", res)
	}
}

func TestSearchMultipleTermsWithFilter(t *testing.T) {
	engine := buildTestEngine(t)
	// multi-term with filter year=2020: exact ["apple","banana"] only doc1
	queries := map[string][]string{"exact": {"apple", "banana"}, "prefix": nil, "fuzzy": nil}
	res := engine.SearchMultipleTermsWithFilter(queries, map[string][]interface{}{"year": {2020}}, 0)
	if len(res) != 1 || res[0].ID != "1" {
		t.Errorf("expected doc1, got %v", res)
	}
}

func TestEndToEnd_Search(t *testing.T) {
	engine := buildTestEngine(t)

	// exact one term
	sr := engine.Search("apple", 0, nil)
	if sr == nil || !sr.IsExact || len(sr.Docs) != 1 || sr.Docs[0].ID != "1" {
		t.Errorf("Search apple exact failed: %+v", sr)
	}

	// prefix
	sr = engine.Search("cher", 0, nil)
	if sr == nil || !sr.IsPrefix || len(sr.Docs) != 2 {
		t.Errorf("Search cher prefix failed: %+v", sr)
	}

	// fuzzy ("aple" -> apple)
	sr = engine.Search("aple", 0, nil)
	if sr == nil || !sr.IsFuzzy || len(sr.Docs) != 1 || sr.Docs[0].ID != "1" {
		t.Errorf("Search aple fuzzy failed: %+v", sr)
	}

	// multi-term
	sr = engine.Search("banana cherry", 0, nil)
	if sr == nil || !sr.IsMultiTerm || len(sr.Docs) != 1 || sr.Docs[0].ID != "2" {
		t.Errorf("Search multi failed: %+v", sr)
	}

	// with filter
	sr = engine.Search("banana", 0, map[string][]interface{}{"year": {2020}})
	if sr == nil || !sr.IsExact || len(sr.Docs) != 1 || sr.Docs[0].ID != "1" {
		t.Errorf("Search banana with filter failed: %+v", sr)
	}
}

func TestAddAndRemoveDocument(t *testing.T) {
	engine := buildTestEngine(t)
	// add single new doc
	doc4 := map[string]interface{}{"id": "4", "name": "egg fruit", "tags": []interface{}{"fresh"}, "year": 2022}
	engine.addDocument(doc4)
	if _, ok := engine.Documents["4"]; !ok {
		t.Fatal("addDocument did not insert doc4")
	}
	// ensure searchable
	sr := engine.Search("egg", 0, nil)
	if sr == nil || sr.Docs[0].ID != "4" {
		t.Errorf("Search egg after add failed: %+v", sr)
	}

	// remove
	engine.removeDocumentByID("4")
	if _, ok := engine.Documents["4"]; ok {
		t.Error("removeDocumentByID did not delete doc4")
	}
	sr = engine.Search("egg", 0, nil)
	// Accept either no SearchResult or an empty Docs slice
	if sr != nil && len(sr.Docs) != 0 {
		t.Errorf("expected no results after remove, got %+v", sr)
	}
}

func TestTokenize(t *testing.T) {
	input := "Hello, WORLD! 123 Go-Lang_test."
	got := Tokenize(input)
	want := []string{"hello", "world", "123", "golangtest"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("tokenize(%q) = %v; want %v", input, got, want)
	}
}

func TestLevenshteinDistance(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"a", "", 1},
		{"", "abc", 3},
		{"kitten", "sitting", 3},
		{"flaw", "lawn", 2},
	}
	for _, tc := range tests {
		if got := levenshteinDistance(tc.a, tc.b); got != tc.want {
			t.Errorf("levenshteinDistance(%q,%q) = %d; want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestFuzzyMatch(t *testing.T) {
	tests := []struct {
		a, b    string
		maxDist int
		want    bool
	}{
		{"kitten", "sitting", 3, true},
		{"kitten", "sitting", 2, false},
		{"flaw", "lawn", 2, true},
		{"flaw", "lawn", 1, false},
	}
	for _, tc := range tests {
		if got := FuzzyMatch(tc.a, tc.b, tc.maxDist); got != tc.want {
			t.Errorf("FuzzyMatch(%q,%q,%d) = %v; want %v", tc.a, tc.b, tc.maxDist, got, tc.want)
		}
	}
}
