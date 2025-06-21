package engine

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	searchkeys "github.com/mg52/search/internal/pkg/keys"
	"github.com/mg52/search/internal/pkg/trie"
)

//go:embed testdata/products.json
var testProductsJSON []byte

//go:embed testdata/products2.json
var testProducts2JSON []byte

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
				"prefix": {"foo"},
				"raw":    {"foo"},
			},
		},
		{
			name:  "prefix match",
			keys:  []string{"apple", "apricot", "banana"},
			query: "ap",
			expected: map[string][]string{
				"prefix": {"apple", "apricot"},
				"raw":    {"ap"},
			},
		},
		{
			name:  "fuzzy match within tolerance",
			keys:  []string{"kitten"},
			query: "kiten", // one deletion away
			expected: map[string][]string{
				"fuzzy": {"kitten"},
				"raw":   {"kiten"},
			},
		},
		{
			name:  "no match falls back to fuzzy with original",
			keys:  []string{"apple"},
			query: "xyz",
			expected: map[string][]string{
				"fuzzy": {"xyz"},
				"raw":   {"xyz"},
			},
		},
		{
			name:  "multiple tokens mixed types",
			keys:  []string{"apple", "apricot", "foo", "kite"},
			query: "ap foo ki",
			expected: map[string][]string{
				"exact":  {"apple", "foo"},
				"prefix": {"kite"},
				"raw":    {"ap", "foo", "ki"},
			},
		},
		{
			name:  "multiple tokens mixed types 2",
			keys:  []string{"apple", "apricot", "foo", "kite", "pepper"},
			query: "peppar f",
			expected: map[string][]string{
				"exact":  {"pepper"},
				"prefix": {"foo"},
				"raw":    {"peppar", "f"},
			},
		},
		{
			name:  "multiple tokens mixed types 3",
			keys:  []string{"apple", "apricot", "foo", "kite", "pepper", "fine"},
			query: "peppar f",
			expected: map[string][]string{
				"exact":  {"pepper"},
				"prefix": {"foo", "fine"},
				"raw":    {"peppar", "f"},
			},
		},
		{
			name:  "multiple tokens mixed types 4",
			keys:  []string{"apple", "apricot", "foo", "kite", "pepper", "fine"},
			query: "peppar fop",
			expected: map[string][]string{
				"exact": {"pepper"},
				"fuzzy": {"foo"},
				"raw":   {"peppar", "fop"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se := setupEngineWithKeys(tt.keys)
			got, _ := se.ProcessQuery(tt.query, 5)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("ProcessQuery(%q):\n got: %#v\nwant: %#v", tt.query, got, tt.expected)
			}
		})
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
	a := Document{"A", nil, 10}
	b := Document{"B", nil, 20}
	c := Document{"C", nil, 5}

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
			if !reflect.DeepEqual(got.Docs, tt.expected) {
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
	pref := se.Trie.SearchPrefix("f", 5)
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
		pref := se.Trie.SearchPrefix(string(term[0]), 5)
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

func buildTestEngine2(t *testing.T) *SearchEngine {
	// two fields indexed, one filter on "year"
	engine := NewSearchEngine(
		[]string{"name", "tags"},
		map[string]bool{"year": true},
		2, // pageSize
		0, // shardID unused
	)
	docs := []map[string]interface{}{
		{"id": "1", "name": "apple banana", "tags": []interface{}{"fruit"}, "year": 2020},
		{"id": "2", "name": "banana cherry date", "tags": []interface{}{"fruit"}, "year": 2021},
		{"id": "3", "name": "cherry date melon pear", "tags": []interface{}{"dry"}, "year": 2020},
	}
	engine.Index(0, docs)
	return engine
}

func TestSaveLoadAll_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	prefix := filepath.Join(dir, "engine")
	engine := buildTestEngine2(t)

	// save
	if err := engine.SaveAll(prefix); err != nil {
		t.Fatalf("SaveAll failed: %v", err)
	}
	// must have .engine.gob
	if _, err := os.Stat(prefix + ".engine.gob"); err != nil {
		t.Fatalf("expected engine file, got: %v", err)
	}

	// load
	loaded, err := LoadAll(prefix, 0)
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

func TestSearchMultipleTermsWithoutFilter_WithoutIndexing(t *testing.T) {
	engine := buildTestEngine(t)
	// multi-term: exact matches ["apple","date"] → only doc3 has date but not apple; doc1 has apple but not date → none
	queries := map[string][]string{
		"exact":  {"apple"},
		"prefix": {"date"},
		"fuzzy":  nil,
		"raw":    {"apple", "date"},
	}
	res := engine.SearchMultipleTermsWithoutFilter(queries, 0)
	if len(res) != 0 {
		t.Errorf("expected no docs, got %v", res)
	}
	// two terms ["banana","cherry"] → doc2 matches both
	// queries["exact"] = []string{"banana", "cherry"}
	queries = map[string][]string{
		"exact":  {"banana"},
		"prefix": {"cherry"},
		"fuzzy":  nil,
		"raw":    {"banana", "cherry"},
	}
	res = engine.SearchMultipleTermsWithoutFilter(queries, 0)
	if len(res) != 1 || res[0].ID != "2" {
		t.Errorf("expected doc2, got %v", res)
	}
}

func TestSearchMultipleTermsWithFilter(t *testing.T) {
	engine := buildTestEngine(t)
	// multi-term with filter year=2020: exact ["apple","banana"] only doc1
	queries := map[string][]string{"exact": {"apple"}, "prefix": {"banana"}, "fuzzy": nil, "raw": {"apple", "banana"}}
	res := engine.SearchMultipleTermsWithFilter(queries, map[string][]interface{}{"year": {2020}}, 0)
	if len(res) != 1 || res[0].ID != "1" {
		t.Errorf("expected doc1, got %v", res)
	}
}

func TestEndToEnd_Search(t *testing.T) {
	engine := buildTestEngine(t)

	// exact one term
	sr := engine.Search("apple", 0, nil, 5)
	if sr == nil || !sr.IsPrefix || len(sr.Docs) != 1 || sr.Docs[0].ID != "1" {
		t.Errorf("Search apple exact failed: %+v", sr)
	}

	// prefix
	sr = engine.Search("cher", 0, nil, 5)
	if sr == nil || !sr.IsPrefix || len(sr.Docs) != 2 {
		t.Errorf("Search cher prefix failed: %+v", sr)
	}

	// fuzzy ("aple" -> apple)
	sr = engine.Search("aple", 0, nil, 5)
	if sr == nil || !sr.IsFuzzy || len(sr.Docs) != 1 || sr.Docs[0].ID != "1" {
		t.Errorf("Search aple fuzzy failed: %+v", sr)
	}

	// multi-term
	sr = engine.Search("banana cherry", 0, nil, 5)
	if sr == nil || !sr.IsMultiTerm || len(sr.Docs) != 1 || sr.Docs[0].ID != "2" {
		t.Errorf("Search multi failed: %+v", sr)
	}

	// with filter
	sr = engine.Search("banana", 0, map[string][]interface{}{"year": {2020}}, 5)
	if sr == nil || !sr.IsPrefix || len(sr.Docs) != 1 || sr.Docs[0].ID != "1" {
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
	sr := engine.Search("egg", 0, nil, 5)
	if sr == nil || sr.Docs[0].ID != "4" {
		t.Errorf("Search egg after add failed: %+v", sr)
	}

	// remove
	engine.removeDocumentByID("4")
	if _, ok := engine.Documents["4"]; ok {
		t.Error("removeDocumentByID did not delete doc4")
	}
	sr = engine.Search("egg", 0, nil, 5)
	// Accept either no SearchResult or an empty Docs slice
	if sr != nil && len(sr.Docs) != 0 {
		t.Errorf("expected no results after remove, got %+v", sr)
	}
}

func TestAddAndRemoveDocument_ExistingString(t *testing.T) {
	engine := buildTestEngine(t)
	// add single new doc
	doc4 := map[string]interface{}{"id": "4", "name": "banana", "tags": []interface{}{"fruit"}, "year": 2022}
	engine.addDocument(doc4)
	if _, ok := engine.Documents["4"]; !ok {
		t.Fatal("addDocument did not insert doc4")
	}
	// ensure searchable
	sr := engine.Search("banana", 0, nil, 5)
	if sr == nil || sr.Docs[0].ID != "4" || sr.Docs[0].ScoreWeight != 50000 {
		t.Errorf("Search banana after add failed: %+v", sr)
	}

	// remove
	engine.removeDocumentByID("4")
	if _, ok := engine.Documents["4"]; ok {
		t.Error("removeDocumentByID did not delete doc4")
	}
	sr = engine.Search("banana", 0, nil, 5)
	if !((sr.Docs[0].ID == "1" || sr.Docs[0].ID == "2") && sr.Docs[0].ScoreWeight == 33333) {
		t.Errorf("expected DocID 1 or 2 after removing banana, got %+v", sr)
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

func TestUpdateDocumentDataOnly(t *testing.T) {
	engine := buildTestEngine(t)
	// "banana" appears in both doc1 and doc2, same weight => order not guaranteed
	res := engine.SearchOneTermWithoutFilter("cherry", 0)
	if len(res) != 2 {
		t.Fatalf("expected 2 results for banana, got %d", len(res))
	}
	seen := map[string]bool{res[0].ID: true, res[1].ID: true}
	if !seen["2"] || !seen["3"] {
		t.Errorf("banana search missing docs, got %v", res)
	}

	for _, doc := range res {
		if doc.ID == "3" {
			if doc.Data["tags"].([]interface{})[0] != "dry" {
				t.Errorf("Wrong tag, it should be 'dry', got %v", doc.Data["tags"])
			}
		}
	}

	engine.Documents["3"]["tags"] = []interface{}{"dry-upd"}

	for _, doc := range res {
		if doc.ID == "3" {
			if doc.Data["tags"].([]interface{})[0] != "dry-upd" {
				t.Errorf("Wrong tag, it should be 'dry-upd', got %v", doc.Data["tags"])
			}
		}
	}
}

// helper to build a SearchEngine with prepopulated FilterDocs
func newTestEngine(data map[string]map[string]bool) *SearchEngine {
	return &SearchEngine{
		mu:         sync.RWMutex{},
		FilterDocs: data,
	}
}

func TestApplyFilter_SingleValue(t *testing.T) {
	se := newTestEngine(map[string]map[string]bool{
		"author:Alice": {"doc1": true, "doc2": true},
	})

	filters := map[string][]interface{}{"author": {"Alice"}}
	got := se.ApplyFilter(filters)
	exp := map[string]bool{"doc1": true, "doc2": true}
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("Expected %v, got %v", exp, got)
	}
}

func TestApplyFilter_MultipleValuesSingleFilter(t *testing.T) {
	se := newTestEngine(map[string]map[string]bool{
		"tag:go":   {"doc1": true, "doc2": true},
		"tag:test": {"doc2": true, "doc3": true},
	})

	filters := map[string][]interface{}{"tag": {"go", "test"}}
	got := se.ApplyFilter(filters)
	exp := map[string]bool{"doc1": true, "doc2": true, "doc3": true}
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("Expected %v, got %v", exp, got)
	}
}

// func TestApplyFilter_MultipleFilters(t *testing.T) {
// 	se := newTestEngine(map[string]map[string]bool{
// 		"author:Alice": {"doc1": true},
// 		"tag:go":       {"doc2": true},
// 	})

// 	filters := map[string][]interface{}{
// 		"author": {"Alice"},
// 		"tag":    {"go"},
// 	}
// 	got := se.ApplyFilter(filters)
// 	exp := map[string]bool{"doc1": true, "doc2": true}
// 	if !reflect.DeepEqual(got, exp) {
// 		t.Errorf("Expected %v, got %v", exp, got)
// 	}
// }

func TestApplyFilter_NoFilters(t *testing.T) {
	se := newTestEngine(nil)
	filters := map[string][]interface{}{}
	got := se.ApplyFilter(filters)
	// Expect empty map (not nil)
	if got == nil || len(got) != 0 {
		t.Errorf("Expected empty map, got %v", got)
	}
}

func TestApplyFilter_MissingSingleValue(t *testing.T) {
	se := newTestEngine(map[string]map[string]bool{})
	filters := map[string][]interface{}{"author": {"Unknown"}}
	got := se.ApplyFilter(filters)
	// Single-value missing should return nil
	if got != nil {
		t.Errorf("Expected nil for missing filter, got %v", got)
	}
}

func TestApplyFilter_MissingValuesInMulti(t *testing.T) {
	se := newTestEngine(map[string]map[string]bool{
		"tag:go": {"doc1": true},
	})
	filters := map[string][]interface{}{"tag": {"go", "nope"}}
	got := se.ApplyFilter(filters)
	exp := map[string]bool{"doc1": true}
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("Expected %v, got %v", exp, got)
	}
}

// newTestEngine constructs a bare SearchEngine suitable for unit tests.
func newTestEngineForMultipleTerm(pageSize int) *SearchEngine {
	return &SearchEngine{
		ScoreIndex: make(map[string][]Document),
		Data:       make(map[string]map[string]int),
		DocData:    make(map[string]map[string]bool),
		Documents:  make(map[string]map[string]interface{}),
		FilterDocs: make(map[string]map[string]bool),
		PageSize:   pageSize,
	}
}

func TestSearchMultipleTermsWithoutFilter(t *testing.T) {
	e := newTestEngineForMultipleTerm(10)

	// two docs for term "foo"
	e.ScoreIndex["foo"] = []Document{
		{ID: "d1", Data: map[string]interface{}{"title": "Doc1"}, ScoreWeight: 1},
		{ID: "d2", Data: map[string]interface{}{"title": "Doc2"}, ScoreWeight: 2},
	}
	e.Documents["d1"] = map[string]interface{}{"title": "Doc1"}
	e.Documents["d2"] = map[string]interface{}{"title": "Doc2"}

	// prepare for prefix branch: raw[1] length == 2
	e.DocData["d1"] = map[string]bool{"foobar": true}
	e.Data["foobar"] = map[string]int{"d1": 3}
	e.DocData["d2"] = map[string]bool{"foobaz": true}
	e.Data["foobaz"] = map[string]int{"d2": 4}

	t.Run("prefix branch", func(t *testing.T) {
		queries := map[string][]string{
			"raw":   {"foo", "fo"},
			"exact": {"foo"},
		}
		res := e.SearchMultipleTermsWithoutFilter(queries, 0)
		if len(res) != 2 {
			t.Fatalf("expected 2 docs, got %d", len(res))
		}
		wantScores := map[string]int{"d1": 1 + 3, "d2": 2 + 4}
		for _, doc := range res {
			if got, want := doc.ScoreWeight, wantScores[doc.ID]; got != want {
				t.Errorf("doc %s: score %d; want %d", doc.ID, got, want)
			}
		}
	})

	// prepare for non-prefix branch: raw[1] length > 2
	t.Run("no-prefix branch", func(t *testing.T) {
		// Only d1 has the exact "foobar" term
		queries := map[string][]string{
			"raw":    {"foo", "foobar"},
			"exact":  {"foo"},
			"prefix": {"foobar"},
			"fuzzy":  nil,
		}
		res := e.SearchMultipleTermsWithoutFilter(queries, 0)
		if len(res) != 1 || res[0].ID != "d1" {
			t.Fatalf("expected only [d1], got %v", res)
		}
		if got, want := res[0].ScoreWeight, 1+3; got != want {
			t.Errorf("d1 score %d; want %d", got, want)
		}
	})
}

func TestSearchDoubleTermsWithPrefixWithoutFilter_Paging(t *testing.T) {
	e := newTestEngineForMultipleTerm(1)
	e.ScoreIndex["foo"] = []Document{
		{ID: "d1", Data: nil, ScoreWeight: 1},
		{ID: "d2", Data: nil, ScoreWeight: 2},
	}
	e.Documents["d1"] = map[string]interface{}{"title": "D1"}
	e.Documents["d2"] = map[string]interface{}{"title": "D2"}
	// both docs have the term "bar" to match prefix "ba"
	e.DocData["d1"] = map[string]bool{"bar": true}
	e.DocData["d2"] = map[string]bool{"bar": true}
	e.Data["bar"] = map[string]int{"d1": 5, "d2": 6}

	queries := map[string][]string{
		"raw":   {"foo", "ba"},
		"exact": {"foo"},
	}
	// page 0 → first hit (d1)
	if res := e.SearchDoubleTermsWithPrefixWithoutFilter(queries, 0); len(res) != 1 || res[0].ID != "d1" {
		t.Errorf("page0 expected [d1], got %v", res)
	}
	// page 1 → second hit (d2)
	if res := e.SearchDoubleTermsWithPrefixWithoutFilter(queries, 1); len(res) != 1 || res[0].ID != "d2" {
		t.Errorf("page1 expected [d2], got %v", res)
	}
	// page 2 → out of range
	if res := e.SearchDoubleTermsWithPrefixWithoutFilter(queries, 2); len(res) != 0 {
		t.Errorf("page2 expected nil, got %v", res)
	}
}

func TestSearchDoubleTermsWithoutFilter(t *testing.T) {
	e := newTestEngineForMultipleTerm(10)
	e.ScoreIndex["foo"] = []Document{
		{ID: "d1", Data: nil, ScoreWeight: 2},
		{ID: "d2", Data: nil, ScoreWeight: 3},
	}
	// only d2 has term "baz"
	e.Data["baz"] = map[string]int{"d2": 7}
	e.DocData["d2"] = map[string]bool{"foo": true, "baz": true}

	queries := map[string][]string{
		"raw":    {"foo", "baz"},
		"exact":  {"foo"},
		"prefix": nil,
		"fuzzy":  {"baz"},
	}
	res := e.SearchDoubleTermsWithoutFilter(queries, 0)
	if len(res) != 1 || res[0].ID != "d2" {
		t.Fatalf("expected [d2], got %v", res)
	}
	if got, want := res[0].ScoreWeight, 3+7; got != want {
		t.Errorf("score = %d; want %d", got, want)
	}
}

func TestSearchMoreThanDoubleTermsWithPrefixWithoutFilter(t *testing.T) {
	e := newTestEngineForMultipleTerm(10)
	e.ScoreIndex["a"] = []Document{
		{ID: "d1", Data: nil, ScoreWeight: 1},
		{ID: "d2", Data: nil, ScoreWeight: 2},
	}
	e.Documents["d1"] = map[string]interface{}{"title": "D1"}
	e.Documents["d2"] = map[string]interface{}{"title": "D2"}
	// both docs have exact "b"
	e.Data["b"] = map[string]int{"d1": 3, "d2": 4}
	// doc-specific terms for prefix "c"
	e.DocData["d1"] = map[string]bool{"cx": true}
	e.Data["cx"] = map[string]int{"d1": 5}
	e.DocData["d2"] = map[string]bool{"cy": true}
	e.Data["cy"] = map[string]int{"d2": 6}

	queries := map[string][]string{
		"raw":   {"a", "b", "c"},
		"exact": {"a", "b"},
	}
	res := e.SearchMoreThanDoubleTermsWithPrefixWithoutFilter(queries, 0)
	if len(res) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(res))
	}
	wantScores := map[string]int{"d1": 1 + 3 + 5, "d2": 2 + 4 + 6}
	for _, d := range res {
		if got, want := d.ScoreWeight, wantScores[d.ID]; got != want {
			t.Errorf("%s score = %d; want %d", d.ID, got, want)
		}
	}
}

func TestSearchMoreThanDoubleTermsWithoutFilter(t *testing.T) {
	e := newTestEngineForMultipleTerm(10)
	e.ScoreIndex["a"] = []Document{
		{ID: "d1", Data: nil, ScoreWeight: 1},
		{ID: "d2", Data: nil, ScoreWeight: 2},
	}
	e.Documents["d1"] = map[string]interface{}{"title": "D1"}
	e.Documents["d2"] = map[string]interface{}{"title": "D2"}
	// exact "b" weights
	e.Data["b"] = map[string]int{"d1": 3, "d2": 4}
	// prefix/fuzzy "d" present only in d2
	e.Data["d"] = map[string]int{"d2": 5}
	e.DocData["d2"] = map[string]bool{"d": true}

	queries := map[string][]string{
		"raw":    {"a", "b", "d"},
		"exact":  {"a", "b"},
		"prefix": {"d"},
		"fuzzy":  nil,
	}
	res := e.SearchMoreThanDoubleTermsWithoutFilter(queries, 0)
	if len(res) != 1 || res[0].ID != "d2" {
		t.Fatalf("expected [d2], got %v", res)
	}
	// Score: base(2) + exact-b(4) + other-d(5) = 11
	if got, want := res[0].ScoreWeight, 11; got != want {
		t.Errorf("score = %d; want %d", got, want)
	}
}

// sanity-check that slices are stable and slicing logic works
func TestPagingEdgeCases(t *testing.T) {
	e := newTestEngineForMultipleTerm(2)
	// make 5 docs all matching
	var docs []Document
	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("d%d", i)
		docs = append(docs, Document{ID: id, Data: nil, ScoreWeight: i})
		e.Documents[id] = map[string]interface{}{"title": id}
		e.DocData[id] = map[string]bool{"term": true}
		e.Data["term"] = map[string]int{id: i}
	}
	e.ScoreIndex["term"] = docs

	queries := map[string][]string{
		"raw":   {"term", "te"},
		"exact": {"term"},
	}

	// page 0 -> docs[0:2]
	res0 := e.SearchDoubleTermsWithPrefixWithoutFilter(queries, 0)
	if !reflect.DeepEqual(getIDs(res0), []string{"d1", "d2"}) {
		t.Errorf("page0 IDs = %v; want [d1 d2]", getIDs(res0))
	}
	// page 1 -> docs[2:4]
	res1 := e.SearchDoubleTermsWithPrefixWithoutFilter(queries, 1)
	if !reflect.DeepEqual(getIDs(res1), []string{"d3", "d4"}) {
		t.Errorf("page1 IDs = %v; want [d3 d4]", getIDs(res1))
	}
	// page 2 -> docs[4:5]
	res2 := e.SearchDoubleTermsWithPrefixWithoutFilter(queries, 2)
	if !reflect.DeepEqual(getIDs(res2), []string{"d5"}) {
		t.Errorf("page2 IDs = %v; want [d5]", getIDs(res2))
	}
	// page 3 -> out of range
	if res3 := e.SearchDoubleTermsWithPrefixWithoutFilter(queries, 3); res3 != nil {
		t.Errorf("page3 expected nil, got %v", res3)
	}
}

func TestSearchMultipleTermsWithFilterDetail(t *testing.T) {
	e := newTestEngineForMultipleTerm(10)
	// only include d1 via filter
	e.FilterDocs["id:d1"] = map[string]bool{"d1": true}

	// setup documents
	e.ScoreIndex["foo"] = []Document{
		{ID: "d1", Data: nil, ScoreWeight: 1},
		{ID: "d2", Data: nil, ScoreWeight: 2},
	}
	e.Documents["d1"] = map[string]interface{}{"title": "Doc1"}
	e.Documents["d2"] = map[string]interface{}{"title": "Doc2"}

	// prefix data
	e.DocData["d1"] = map[string]bool{"foobar": true}
	e.Data["foobar"] = map[string]int{"d1": 3}
	e.DocData["d2"] = map[string]bool{"foobaz": true}
	e.Data["foobaz"] = map[string]int{"d2": 4}

	filters := map[string][]interface{}{"id": {"d1"}}

	t.Run("prefix branch", func(t *testing.T) {
		queries := map[string][]string{
			"raw":   {"foo", "fo"},
			"exact": {"foo"},
		}
		res := e.SearchMultipleTermsWithFilter(queries, filters, 0)
		if len(res) != 1 || res[0].ID != "d1" {
			t.Fatalf("expected only d1, got %v", res)
		}
		if res[0].ScoreWeight != 1+3 {
			t.Errorf("score = %d; want %d", res[0].ScoreWeight, 4)
		}
	})

	t.Run("no-prefix branch", func(t *testing.T) {
		queries := map[string][]string{
			"raw":    {"foo", "foobar"},
			"exact":  {"foo"},
			"prefix": {"foobar"},
			"fuzzy":  nil,
		}
		res := e.SearchMultipleTermsWithFilter(queries, filters, 0)
		if len(res) != 1 || res[0].ID != "d1" {
			t.Fatalf("expected only d1, got %v", res)
		}
	})
}

func TestSearchDoubleTermsWithPrefixWithFilter(t *testing.T) {
	e := newTestEngineForMultipleTerm(5)
	e.FilterDocs["id:d2"] = map[string]bool{"d2": true}

	e.ScoreIndex["foo"] = []Document{
		{ID: "d1", Data: nil, ScoreWeight: 1},
		{ID: "d2", Data: nil, ScoreWeight: 2},
	}
	e.Documents["d1"] = map[string]interface{}{"": nil}
	e.Documents["d2"] = map[string]interface{}{"": nil}
	e.DocData["d1"] = map[string]bool{"bar": true}
	e.Data["bar"] = map[string]int{"d1": 5}
	e.DocData["d2"] = map[string]bool{"bar": true}
	e.Data["bar"]["d2"] = 6

	filters := map[string][]interface{}{"id": {"d2"}}
	queries := map[string][]string{"raw": {"foo", "ba"}, "exact": {"foo"}}

	res := e.SearchDoubleTermsWithPrefixWithFilter(queries, filters, 0)
	if len(res) != 1 || res[0].ID != "d2" {
		t.Fatalf("expected only d2, got %v", res)
	}
	if res[0].ScoreWeight != 2+6 {
		t.Errorf("score = %d; want %d", res[0].ScoreWeight, 8)
	}
}

func TestSearchDoubleTermsWithFilter(t *testing.T) {
	e := newTestEngineForMultipleTerm(5)
	e.FilterDocs["id:d1"] = map[string]bool{"d1": true}

	e.ScoreIndex["foo"] = []Document{{ID: "d1", Data: nil, ScoreWeight: 3}}
	e.Documents["d1"] = map[string]interface{}{"": nil}
	e.Data["baz"] = map[string]int{"d1": 7}

	filters := map[string][]interface{}{"id": {"d1"}}
	queries := map[string][]string{
		"raw":    {"foo", "baz"},
		"exact":  {"foo"},
		"prefix": nil,
		"fuzzy":  {"baz"},
	}
	res := e.SearchDoubleTermsWithFilter(queries, filters, 0)
	if len(res) != 1 || res[0].ID != "d1" {
		t.Fatalf("expected d1, got %v", res)
	}
	if res[0].ScoreWeight != 3+7 {
		t.Errorf("score = %d; want %d", res[0].ScoreWeight, 10)
	}
}

func TestSearchMoreThanDoubleTermsWithPrefixWithFilter(t *testing.T) {
	e := newTestEngineForMultipleTerm(5)
	e.FilterDocs["id:d2"] = map[string]bool{"d2": true}

	e.ScoreIndex["a"] = []Document{{ID: "d1", Data: nil, ScoreWeight: 1}, {ID: "d2", Data: nil, ScoreWeight: 2}}
	e.Documents["d1"] = map[string]interface{}{"": nil}
	e.Documents["d2"] = map[string]interface{}{"": nil}
	e.Data["b"] = map[string]int{"d1": 3, "d2": 4}
	e.DocData["d1"] = map[string]bool{"cx": true}
	e.Data["cx"] = map[string]int{"d1": 5}
	e.DocData["d2"] = map[string]bool{"cy": true}
	e.Data["cy"] = map[string]int{"d2": 6}

	filters := map[string][]interface{}{"id": {"d2"}}
	queries := map[string][]string{
		"raw":   {"a", "b", "c"},
		"exact": {"a", "b"},
	}
	res := e.SearchMoreThanDoubleTermsWithPrefixWithFilter(queries, filters, 0)
	if len(res) != 1 || res[0].ID != "d2" {
		t.Fatalf("expected only d2, got %v", res)
	}
}

func TestSearchMoreThanDoubleTermsWithFilter(t *testing.T) {
	e := newTestEngineForMultipleTerm(5)
	e.FilterDocs["id:d1"] = map[string]bool{"d1": true}
	e.FilterDocs["id:d2"] = map[string]bool{"d2": true}

	e.ScoreIndex["a"] = []Document{{ID: "d1", Data: nil, ScoreWeight: 1}, {ID: "d2", Data: nil, ScoreWeight: 2}}
	e.Documents["d1"] = map[string]interface{}{"": nil}
	e.Documents["d2"] = map[string]interface{}{"": nil}
	e.Data["b"] = map[string]int{"d1": 3, "d2": 4}
	e.Data["d"] = map[string]int{"d2": 5}

	filters := map[string][]interface{}{"id": {"d1", "d2"}}
	queries := map[string][]string{
		"raw":    {"a", "b", "d"},
		"exact":  {"a", "b"},
		"prefix": {"d"},
		"fuzzy":  nil,
	}
	res := e.SearchMoreThanDoubleTermsWithFilter(queries, filters, 0)
	if len(res) != 1 || res[0].ID != "d2" {
		t.Fatalf("expected only d2, got %v", res)
	}
	if res[0].ScoreWeight != 2+4+5 {
		t.Errorf("score = %d; want %d", res[0].ScoreWeight, 11)
	}
}

// helper to extract IDs
func getIDs(docs []Document) []string {
	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = d.ID
	}
	return ids
}

func TestSearchEngineFlow_MultiTerm_LastTermLessThan2(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProducts2JSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	engine := NewSearchEngine(
		[]string{"name", "tags"},
		map[string]bool{"year": true},
		5, // pageSize
		0, // shardID unused
	)

	engine.Index(0, docs)

	emptyFilters := make(map[string][]interface{})

	// map[exact:[smart] prefix:[crop clay cream compact] raw:[smart c]]
	result := engine.Search("smart c", 0, emptyFilters, 5)
	resultDocs := result.Docs

	sort.Slice(resultDocs, func(i, j int) bool {
		return resultDocs[i].ScoreWeight > resultDocs[j].ScoreWeight
	})

	if len(resultDocs) != 5 {
		t.Fatalf("expected 5 results, got %d", len(resultDocs))
	}
	if resultDocs[0].ID != "13" || resultDocs[0].ScoreWeight != 100000 {
		t.Errorf("for 'smart c', expected {ID:\"13\",ScoreWeight:100000}, got %s - %d", resultDocs[0].ID, resultDocs[0].ScoreWeight)
	}
	if resultDocs[1].ID != "12" || resultDocs[1].ScoreWeight != 99999 {
		t.Errorf("for 'smart c', expected {ID:\"12\",ScoreWeight:99999}, got %s - %d", resultDocs[1].ID, resultDocs[1].ScoreWeight)
	}
	if !((resultDocs[2].ID == "28" ||
		resultDocs[2].ID == "2" ||
		resultDocs[2].ID == "22") && resultDocs[2].ScoreWeight == 66666) {
		t.Errorf("for 'smart c', expected {ID:\"28, 2, 22\",ScoreWeight:66666}, got %s - %d", resultDocs[2].ID, resultDocs[2].ScoreWeight)
	}
	if !((resultDocs[3].ID == "28" ||
		resultDocs[3].ID == "2" ||
		resultDocs[3].ID == "22") && resultDocs[3].ScoreWeight == 66666) {
		t.Errorf("for 'smart c', expected {ID:\"28, 2, 22\",ScoreWeight:66666}, got %s - %d", resultDocs[3].ID, resultDocs[3].ScoreWeight)
	}
	if !((resultDocs[4].ID == "28" ||
		resultDocs[4].ID == "2" ||
		resultDocs[4].ID == "22") && resultDocs[4].ScoreWeight == 66666) {
		t.Errorf("for 'smart c', expected {ID:\"28, 2, 22\",ScoreWeight:66666}, got %s - %d", resultDocs[4].ID, resultDocs[4].ScoreWeight)
	}

	result = engine.Search("smart c", 1, emptyFilters, 5)
	resultDocs = result.Docs

	sort.Slice(resultDocs, func(i, j int) bool {
		return resultDocs[i].ScoreWeight > resultDocs[j].ScoreWeight
	})

	if len(resultDocs) != 5 {
		t.Fatalf("expected 5 results, got %d", len(resultDocs))
	}
	if !((resultDocs[0].ID == "17" ||
		resultDocs[0].ID == "10" ||
		resultDocs[0].ID == "9" ||
		resultDocs[0].ID == "19") && resultDocs[0].ScoreWeight == 50000) {
		t.Errorf("for 'smart c', expected {ID:\"17, 10, 9, 19\",ScoreWeight:50000}, got %s - %d", resultDocs[0].ID, resultDocs[0].ScoreWeight)
	}
	if !((resultDocs[1].ID == "17" ||
		resultDocs[1].ID == "10" ||
		resultDocs[1].ID == "9" ||
		resultDocs[1].ID == "19") && resultDocs[1].ScoreWeight == 50000) {
		t.Errorf("for 'smart c', expected {ID:\"17, 10, 9, 19\",ScoreWeight:50000}, got %s - %d", resultDocs[1].ID, resultDocs[1].ScoreWeight)
	}
	if !((resultDocs[2].ID == "17" ||
		resultDocs[2].ID == "10" ||
		resultDocs[2].ID == "9" ||
		resultDocs[2].ID == "19") && resultDocs[2].ScoreWeight == 50000) {
		t.Errorf("for 'smart c', expected {ID:\"17, 10, 9, 19\",ScoreWeight:50000}, got %s - %d", resultDocs[2].ID, resultDocs[2].ScoreWeight)
	}
	if !((resultDocs[3].ID == "17" ||
		resultDocs[3].ID == "10" ||
		resultDocs[3].ID == "9" ||
		resultDocs[3].ID == "19") && resultDocs[3].ScoreWeight == 50000) {
		t.Errorf("for 'smart c', expected {ID:\"17, 10, 9, 19\",ScoreWeight:50000}, got %s - %d", resultDocs[3].ID, resultDocs[3].ScoreWeight)
	}
	if resultDocs[4].ID != "26" || resultDocs[4].ScoreWeight != 40000 {
		t.Errorf("for 'smart c', expected {ID:\"26\",ScoreWeight:40000}, got %s - %d", resultDocs[4].ID, resultDocs[4].ScoreWeight)
	}

	result = engine.Search("smart c", 2, emptyFilters, 5)
	resultDocs = result.Docs

	if len(resultDocs) != 2 {
		t.Fatalf("expected 2 results, got %d", len(resultDocs))
	}
	if !((resultDocs[0].ID == "27" ||
		resultDocs[0].ID == "15") && resultDocs[0].ScoreWeight == 33332) {
		t.Errorf("for 'smart c', expected {ID:\"27, 15\",ScoreWeight:33332}, got %s - %d", resultDocs[0].ID, resultDocs[0].ScoreWeight)
	}
	if !((resultDocs[1].ID == "27" ||
		resultDocs[1].ID == "15") && resultDocs[1].ScoreWeight == 33332) {
		t.Errorf("for 'smart c', expected {ID:\"27, 15\",ScoreWeight:33332}, got %s - %d", resultDocs[1].ID, resultDocs[1].ScoreWeight)
	}
}
