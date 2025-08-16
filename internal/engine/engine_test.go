package engine

import (
	_ "embed"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	searchkeys "github.com/mg52/search/internal/pkg/keys"
	"github.com/mg52/search/internal/pkg/symspell"
	"github.com/mg52/search/internal/pkg/trie"
)

//go:embed testdata/products.json
var testProductsJSON []byte

//go:embed testdata/products2.json
var testProducts2JSON []byte

func setupEngineWithKeys(keys []string) *SearchEngine {
	// Initialize engine
	se := &SearchEngine{
		Keys:     searchkeys.NewKeys(),
		Trie:     trie.NewTrie(),
		Symspell: symspell.NewSymSpell(),
	}
	// Insert each key into Keys and Trie
	for _, k := range keys {
		se.Keys.Insert(k)
		se.Trie.Insert(k)
		se.Symspell.AddWord(k)
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
		// {
		// 	name:     "empty query",
		// 	keys:     []string{"a", "b"},
		// 	query:    "",
		// 	expected: nil,
		// },
		// {
		// 	name:  "exact match",
		// 	keys:  []string{"foo", "bar"},
		// 	query: "foo",
		// 	expected: map[string][]string{
		// 		"prefix": {"foo"},
		// 		"raw":    {"foo"},
		// 	},
		// },
		// {
		// 	name:  "prefix match",
		// 	keys:  []string{"apple", "apricot", "banana"},
		// 	query: "ap",
		// 	expected: map[string][]string{
		// 		"prefix": {"apple", "apricot"},
		// 		"raw":    {"ap"},
		// 	},
		// },
		{
			name:  "fuzzy match within tolerance",
			keys:  []string{"kitten"},
			query: "kiten", // one deletion away
			expected: map[string][]string{
				"fuzzy": {"kitten"},
				"raw":   {"kiten"},
			},
		},
		// {
		// 	name:  "no match falls back to fuzzy with original",
		// 	keys:  []string{"apple"},
		// 	query: "xyz",
		// 	expected: map[string][]string{
		// 		"fuzzy": {"xyz"},
		// 		"raw":   {"xyz"},
		// 	},
		// },
		// {
		// 	name:  "multiple tokens mixed types",
		// 	keys:  []string{"apple", "apricot", "foo", "kite"},
		// 	query: "ap foo ki",
		// 	expected: map[string][]string{
		// 		"exact":  {"apple", "foo"},
		// 		"prefix": {"kite"},
		// 		"raw":    {"ap", "foo", "ki"},
		// 	},
		// },
		// {
		// 	name:  "multiple tokens mixed types 2",
		// 	keys:  []string{"apple", "apricot", "foo", "kite", "pepper"},
		// 	query: "peppar f",
		// 	expected: map[string][]string{
		// 		"exact":  {"pepper"},
		// 		"prefix": {"foo"},
		// 		"raw":    {"peppar", "f"},
		// 	},
		// },
		// {
		// 	name:  "multiple tokens mixed types 3",
		// 	keys:  []string{"apple", "apricot", "foo", "kite", "pepper", "fine"},
		// 	query: "peppar f",
		// 	expected: map[string][]string{
		// 		"exact":  {"pepper"},
		// 		"prefix": {"foo", "fine"},
		// 		"raw":    {"peppar", "f"},
		// 	},
		// },
		// {
		// 	name:  "multiple tokens mixed types 4",
		// 	keys:  []string{"apple", "apricot", "foo", "kite", "pepper", "fine"},
		// 	query: "peppar fop",
		// 	expected: map[string][]string{
		// 		"exact": {"pepper"},
		// 		"fuzzy": {"foo"},
		// 		"raw":   {"peppar", "fop"},
		// 	},
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se := setupEngineWithKeys(tt.keys)
			got, _ := se.ProcessQuery(tt.query)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("ProcessQuery(%q):\n got: %#v\nwant: %#v", tt.query, got, tt.expected)
			}
		})
	}
}

func TestCombineResults(t *testing.T) {
	// helper to build SearchResult
	mk := func(isMulti, isPrefixOrExact, isFuzzy bool, docs ...Document) *SearchResult {
		return &SearchResult{
			IsMultiTerm:     isMulti,
			IsPrefixOrExact: isPrefixOrExact,
			IsFuzzy:         isFuzzy,
			Docs:            docs,
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
				mk(true, false, false, a, b),
				mk(false, false, false, c),
			},
			[]Document{b, a}, // sorted by weight desc
		},
		{
			"prefix next",
			[]*SearchResult{
				mk(false, true, false, b, c),
			},
			[]Document{b, c},
		},
		{
			"fuzzy fallback",
			[]*SearchResult{
				mk(false, false, true, c),
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
		2, 0,
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
		2, 0,
	)
	docs := []map[string]interface{}{
		{"id": "1", "name": "apple banana", "tags": []interface{}{"fruit"}, "year": 2020},
		{"id": "2", "name": "banana cherry date", "tags": []interface{}{"fruit"}, "year": 2021},
		{"id": "3", "name": "cherry date melon pear", "tags": []interface{}{"dry"}, "year": 2020},
	}
	engine.Index(0, docs)
	return engine
}

func buildTestEngineForSaveLoadTest() *SearchEngine {
	// two fields indexed, one filter on "year"
	engine := NewSearchEngine(
		[]string{"name", "tags"},
		map[string]bool{"year": true},
		11, 2,
	)
	docs := []map[string]interface{}{
		{"id": "1", "name": "apple banana", "tags": []interface{}{"fruit"}, "year": 2020},
		{"id": "2", "name": "banana cherry date", "tags": []interface{}{"fruit"}, "year": 2021},
		{"id": "3", "name": "cherry date melon pear", "tags": []interface{}{"dry"}, "year": 2020},
	}
	engine.Index(2, docs)
	return engine
}

func TestSaveLoadAll_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	prefix := filepath.Join(dir, "engine")
	engine := buildTestEngineForSaveLoadTest()

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
	if !reflect.DeepEqual(loaded.IndexFields, engine.IndexFields) {
		t.Errorf("IndexFields mismatch\ngot:  %+v\nwant: %+v", loaded.IndexFields, engine.IndexFields)
	}
	if !reflect.DeepEqual(loaded.Filters, engine.Filters) {
		t.Errorf("Filters mismatch\ngot:  %+v\nwant: %+v", loaded.Filters, engine.Filters)
	}
	if !reflect.DeepEqual(loaded.PageSize, engine.PageSize) {
		t.Errorf("PageSize mismatch\ngot:  %+v\nwant: %+v", loaded.PageSize, engine.PageSize)
	}
	if !reflect.DeepEqual(loaded.Symspell, engine.Symspell) {
		t.Errorf("Symspell mismatch\ngot:  %+v\nwant: %+v", loaded.Symspell, engine.Symspell)
	}
	if !reflect.DeepEqual(loaded.Trie, engine.Trie) {
		t.Errorf("Trie mismatch\ngot:  %+v\nwant: %+v", loaded.Trie, engine.Trie)
	}
	if !reflect.DeepEqual(loaded.Keys, engine.Keys) {
		t.Errorf("Keys mismatch\ngot:  %+v\nwant: %+v", loaded.Keys, engine.Keys)
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

func TestEndToEnd_Search(t *testing.T) {
	engine := buildTestEngine(t)

	// exact one term
	sr := engine.Search("apple", 0, nil, 0)
	if sr == nil || !sr.IsPrefixOrExact || len(sr.Docs) != 1 || sr.Docs[0].ID != "1" {
		t.Errorf("Search apple exact failed: %+v", sr)
	}

	// prefix
	sr = engine.Search("cher", 0, nil, 0)
	if sr == nil || !sr.IsPrefixOrExact || len(sr.Docs) != 2 {
		t.Errorf("Search cher prefix failed: %+v", sr)
	}

	// fuzzy ("aple" -> apple)
	sr = engine.Search("aple", 0, nil, 0)
	if sr == nil || !sr.IsFuzzy || len(sr.Docs) != 1 || sr.Docs[0].ID != "1" {
		t.Errorf("Search aple fuzzy failed: %+v", sr)
	}

	// multi-term
	sr = engine.Search("banana cherry", 0, nil, 0)
	if sr == nil || !sr.IsMultiTerm || len(sr.Docs) != 1 || sr.Docs[0].ID != "2" {
		t.Errorf("Search multi failed: %+v", sr)
	}

	// with filter
	sr = engine.Search("banana", 0, map[string][]interface{}{"year": {2020}}, 0)
	if sr == nil || !sr.IsPrefixOrExact || len(sr.Docs) != 1 || sr.Docs[0].ID != "1" {
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
	sr := engine.Search("egg", 0, nil, 0)
	if sr == nil || sr.Docs[0].ID != "4" {
		t.Errorf("Search egg after add failed: %+v", sr)
	}

	// remove
	engine.removeDocumentByID("4")
	if _, ok := engine.Documents["4"]; ok {
		t.Error("removeDocumentByID did not delete doc4")
	}
	sr = engine.Search("egg", 0, nil, 0)
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
	sr := engine.Search("banana", 0, nil, 0)
	if sr == nil || sr.Docs[0].ID != "4" || sr.Docs[0].ScoreWeight != 50000 {
		t.Errorf("Search banana after add failed: %+v", sr)
	}

	// remove
	engine.removeDocumentByID("4")
	if _, ok := engine.Documents["4"]; ok {
		t.Error("removeDocumentByID did not delete doc4")
	}
	sr = engine.Search("banana", 0, nil, 0)
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

func TestApplyFilter_NoFilters(t *testing.T) {
	se := newTestEngine(nil)
	filters := map[string][]interface{}{}
	got := se.ApplyFilter(filters)
	// With no filters provided, ApplyFilter returns nil.
	if got != nil {
		t.Errorf("Expected nil, got %v", got)
	}
}

func TestApplyFilter_MissingSingleValue(t *testing.T) {
	se := newTestEngine(map[string]map[string]bool{})
	filters := map[string][]interface{}{"author": {"Unknown"}}
	got := se.ApplyFilter(filters)
	// Missing single value now returns an empty map (no matches), not nil.
	if got == nil || len(got) != 0 {
		t.Errorf("Expected empty map, got %v", got)
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

// OR-within-field, AND-across-fields.
func TestApplyFilter_ANDAcrossFields(t *testing.T) {
	se := newTestEngine(map[string]map[string]bool{
		"year:2020": {"doc1": true, "doc2": true},
		"year:2021": {"doc2": true, "doc3": true},
		"type:song": {"doc2": true, "doc3": true},
	})
	// (year=2020 OR 2021) AND (type=song) => doc2, doc3
	filters := map[string][]interface{}{
		"year": {2020, 2021},
		"type": {"song"},
	}
	got := se.ApplyFilter(filters)
	exp := map[string]bool{"doc2": true, "doc3": true}
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("Expected %v, got %v", exp, got)
	}
}

// AND across fields should yield empty intersection when one side is empty.
func TestApplyFilter_ANDAcrossFields_EmptyIntersection(t *testing.T) {
	se := newTestEngine(map[string]map[string]bool{
		"year:2024": {"doc1": true},
		"type:book": {"doc2": true},
	})
	// (year=2024) AND (type=song) => ∅
	filters := map[string][]interface{}{
		"year": {2024},
		"type": {"song"},
	}
	got := se.ApplyFilter(filters)
	if got == nil || len(got) != 0 {
		t.Errorf("Expected empty map, got %v", got)
	}
}

// numeric values should match via fmt.Sprintf("%v").
func TestApplyFilter_NumericValues(t *testing.T) {
	se := newTestEngine(map[string]map[string]bool{
		"year:2024": {"doc1": true, "doc2": true},
	})
	filters := map[string][]interface{}{
		"year": {2024},
	}
	got := se.ApplyFilter(filters)
	exp := map[string]bool{"doc1": true, "doc2": true}
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("Expected %v, got %v", exp, got)
	}
}

// empty value slice for a field yields empty result.
func TestApplyFilter_EmptyValuesSlice(t *testing.T) {
	se := newTestEngine(map[string]map[string]bool{
		"tag:go": {"doc1": true},
	})
	filters := map[string][]interface{}{
		"tag": {},
	}
	got := se.ApplyFilter(filters)
	if got == nil || len(got) != 0 {
		t.Errorf("Expected empty map, got %v", got)
	}
}

// union within a single field should de-duplicate shared docs.
func TestApplyFilter_DedupWithinUnion(t *testing.T) {
	se := newTestEngine(map[string]map[string]bool{
		"tag:go":   {"doc1": true, "doc2": true},
		"tag:test": {"doc2": true, "doc3": true},
	})
	filters := map[string][]interface{}{
		"tag": {"go", "test"},
	}
	got := se.ApplyFilter(filters)
	exp := map[string]bool{"doc1": true, "doc2": true, "doc3": true}
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("Expected %v, got %v", exp, got)
	}
}

func TestSearchEngineFlow_MultiTerm_LastTermLessThan2(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProducts2JSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	engine := NewSearchEngine(
		[]string{"name", "tags"},
		map[string]bool{"year": true},
		5, 0,
	)

	engine.Index(0, docs)

	emptyFilters := make(map[string][]interface{})

	// map[exact:[smart] prefix:[crop clay cream compact] raw:[smart c]]
	result := engine.Search("smart c", 0, emptyFilters, 0)
	resultDocs := result.Docs

	sort.Slice(resultDocs, func(i, j int) bool {
		return resultDocs[i].ScoreWeight > resultDocs[j].ScoreWeight
	})

	if len(resultDocs) != 5 {
		t.Fatalf("expected 5 results, got %d", len(resultDocs))
	}
	if resultDocs[0].ID != "13" || resultDocs[0].ScoreWeight != 75000 {
		t.Errorf("for 'smart c', expected {ID:\"13\",ScoreWeight:75000}, got %s - %d", resultDocs[0].ID, resultDocs[0].ScoreWeight)
	}
	if !((resultDocs[2].ID == "28" ||
		resultDocs[2].ID == "2" ||
		resultDocs[2].ID == "22" ||
		resultDocs[2].ID == "12") && resultDocs[2].ScoreWeight == 66666) {
		t.Errorf("for 'smart c', expected {ID:\"28, 2, 22\",ScoreWeight:66666}, got %s - %d", resultDocs[2].ID, resultDocs[2].ScoreWeight)
	}
	if !((resultDocs[3].ID == "28" ||
		resultDocs[3].ID == "2" ||
		resultDocs[3].ID == "22" ||
		resultDocs[3].ID == "12") && resultDocs[3].ScoreWeight == 66666) {
		t.Errorf("for 'smart c', expected {ID:\"28, 2, 22\",ScoreWeight:66666}, got %s - %d", resultDocs[3].ID, resultDocs[3].ScoreWeight)
	}
	if !((resultDocs[4].ID == "28" ||
		resultDocs[4].ID == "2" ||
		resultDocs[4].ID == "22" ||
		resultDocs[4].ID == "12") && resultDocs[4].ScoreWeight == 66666) {
		t.Errorf("for 'smart c', expected {ID:\"28, 2, 22\",ScoreWeight:66666}, got %s - %d", resultDocs[4].ID, resultDocs[4].ScoreWeight)
	}

	result = engine.Search("smart c", 1, emptyFilters, 0)
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

	result = engine.Search("smart c", 2, emptyFilters, 0)
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
