package engine

import (
	_ "embed"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	searchkeys "github.com/mg52/search/internal/pkg/keys"
	"github.com/mg52/search/internal/pkg/trie"
)

//go:embed testdata/products.json
var testProductsJSON []byte

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
				"exact":  {"ap", "foo"},
				"prefix": {"kite"},
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

// newTestEngineForMultipleTerm builds a SearchEngine prepopulated for SearchMultipleTermsWithFilter tests
func newTestEngineForMultipleTerm(
	pageSize int,
	scoreIndex map[string][]Document,
	weights map[string]map[string]int,
	docs map[string]map[string]interface{},
	filterDocs map[string]map[string]bool,
) *SearchEngine {
	return &SearchEngine{
		mu:         sync.RWMutex{},
		PageSize:   pageSize,
		ScoreIndex: scoreIndex,
		Data:       weights,
		Documents:  docs,
		FilterDocs: filterDocs,
	}
}

func TestSearchMultipleTermsWithFilter_NoExactTerms(t *testing.T) {
	se := newTestEngineForMultipleTerm(10, nil, nil, nil, nil)
	queries := map[string][]string{"exact": {}, "prefix": {}, "fuzzy": {}}
	res := se.SearchMultipleTermsWithFilter(queries, nil, 0)
	if res != nil {
		t.Errorf("expected nil when no exact terms, got %v", res)
	}
}

func TestSearchMultipleTermsWithFilter_SingleExact_NoOthers(t *testing.T) {
	// Setup single exact term, two docs, page 0
	scoreIdx := map[string][]Document{
		"foo": {{ID: "d1", ScoreWeight: 5}, {ID: "d2", ScoreWeight: 3}},
	}
	weights := map[string]map[string]int{}
	docs := map[string]map[string]interface{}{
		"d1": {"value": "A"},
		"d2": {"value": "B"},
	}
	filterDocs := map[string]map[string]bool{"type:all": {"d1": true, "d2": true}}
	se := newTestEngineForMultipleTerm(
		2,
		scoreIdx,
		weights,
		docs,
		filterDocs,
	)
	queries := map[string][]string{"exact": {"foo"}, "prefix": {}, "fuzzy": {}}
	res := se.SearchMultipleTermsWithFilter(queries, map[string][]interface{}{"type": {"all"}}, 0)
	// Expect two documents in original order with data and weight
	exp := []Document{
		{ID: "d1", Data: docs["d1"], ScoreWeight: 5},
		{ID: "d2", Data: docs["d2"], ScoreWeight: 3},
	}
	if !reflect.DeepEqual(res, exp) {
		t.Errorf("expected %v, got %v", exp, res)
	}
}

func TestSearchMultipleTermsWithFilter_MultipleExact_TermFiltering(t *testing.T) {
	// exact terms: foo, bar, baz. Only d1 has all
	scoreIdx := map[string][]Document{
		"foo": {{ID: "d1", ScoreWeight: 5}, {ID: "d2", ScoreWeight: 5}},
	}
	weights := map[string]map[string]int{
		"bar": {"d1": 2, "d2": 2},
		"baz": {"d1": 3}, // d2 missing baz
	}
	docs := map[string]map[string]interface{}{
		"d1": {"value": "A"},
		"d2": {"value": "B"},
	}
	filterDocs := map[string]map[string]bool{"all:yes": {"d1": true, "d2": true}}
	se := newTestEngineForMultipleTerm(10, scoreIdx, weights, docs, filterDocs)
	queries := map[string][]string{"exact": {"foo", "bar", "baz"}, "prefix": {}, "fuzzy": {}}
	res := se.SearchMultipleTermsWithFilter(queries, map[string][]interface{}{"all": {"yes"}}, 0)
	// Only d1, score =5+2+3=10
	exp := []Document{{ID: "d1", Data: docs["d1"], ScoreWeight: 10}}
	if !reflect.DeepEqual(res, exp) {
		t.Errorf("expected %v, got %v", exp, res)
	}
}

func TestSearchMultipleTermsWithFilter_WithOtherTerms(t *testing.T) {
	// exact=foo; prefix=[p1,p2]; fuzzy=[f1]
	scoreIdx := map[string][]Document{
		"foo": {{ID: "d1", ScoreWeight: 4}, {ID: "d2", ScoreWeight: 7}},
	}
	weights := map[string]map[string]int{
		"p1": {"d2": 1},
		"p2": {"d1": 2},
		"f1": {"d1": 3, "d2": 3},
	}
	docs := map[string]map[string]interface{}{
		"d1": {"value": "A"},
		"d2": {"value": "B"},
	}
	filterDocs := map[string]map[string]bool{"all:yes": {"d1": true, "d2": true}}
	se := newTestEngineForMultipleTerm(10, scoreIdx, weights, docs, filterDocs)
	queries := map[string][]string{"exact": {"foo"}, "prefix": {"p1", "p2"}, "fuzzy": {"f1"}}
	res := se.SearchMultipleTermsWithFilter(queries, map[string][]interface{}{"all": {"yes"}}, 0)
	// d1: found at p2(index1) => score=4 + p2(2)+f1(3)=9
	// d2: found at p1(index0) => score=7 + p1(1)+p2 missing(0)+f1(3)=11
	exp := []Document{
		{ID: "d1", Data: docs["d1"], ScoreWeight: 9},
		{ID: "d2", Data: docs["d2"], ScoreWeight: 11},
	}
	if !reflect.DeepEqual(res, exp) {
		t.Errorf("expected %v, got %v", exp, res)
	}
}

func TestSearchMultipleTermsWithFilter_Paging(t *testing.T) {
	// page size 1, two docs => page1 returns second only
	scoreIdx := map[string][]Document{"foo": {{ID: "d1", ScoreWeight: 1}, {ID: "d2", ScoreWeight: 2}}}
	weights := map[string]map[string]int{}
	docs := map[string]map[string]interface{}{
		"d1": {"value": "A"},
		"d2": {"value": "B"},
	}
	filterDocs := map[string]map[string]bool{"all:yes": {"d1": true, "d2": true}}
	se := newTestEngineForMultipleTerm(1, scoreIdx, weights, docs, filterDocs)
	queries := map[string][]string{"exact": {"foo"}, "prefix": {}, "fuzzy": {}}
	res := se.SearchMultipleTermsWithFilter(queries, map[string][]interface{}{"all": {"yes"}}, 1)
	exp := []Document{{ID: "d2", Data: docs["d2"], ScoreWeight: 2}}
	if !reflect.DeepEqual(res, exp) {
		t.Errorf("expected page 1 result %v, got %v", exp, res)
	}
}

func TestSearchMultipleTermsWithFilter_FilterExcludesAll(t *testing.T) {
	// filter only d3, but scoreIdx has d1,d2 => no docs
	scoreIdx := map[string][]Document{"foo": {{ID: "d1", ScoreWeight: 1}, {ID: "d2", ScoreWeight: 2}}}
	weights := map[string]map[string]int{}
	docs := map[string]map[string]interface{}{
		"d1": {"value": "A"},
		"d2": {"value": "B"},
	}
	filterDocs := map[string]map[string]bool{"all:yes": {"d3": true}}
	se := newTestEngineForMultipleTerm(5, scoreIdx, weights, docs, filterDocs)
	queries := map[string][]string{"exact": {"foo"}, "prefix": {}, "fuzzy": {}}
	res := se.SearchMultipleTermsWithFilter(queries, map[string][]interface{}{"all": {"yes"}}, 0)
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}
	if len(res) != 0 {
		t.Errorf("expected 0 docs, got %d", len(res))
	}
}
