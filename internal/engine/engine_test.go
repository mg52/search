package engine

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	_ "embed"
)

// helper to build a SearchEngine with prepopulated FilterDocs
func newTestEngineWithData(data map[string]map[uint32]bool) *SearchEngine {
	return &SearchEngine{
		mu:         sync.RWMutex{},
		FilterDocs: data,
		DocDeleted: make(map[uint32]bool),
	}
}

func TestApplyFilter_SingleValue(t *testing.T) {
	se := newTestEngineWithData(map[string]map[uint32]bool{
		"author:Alice": {1: true, 2: true},
	})

	filters := map[string][]interface{}{"author": {"Alice"}}
	got := se.ApplyFilter(filters)
	exp := map[uint32]bool{1: true, 2: true}
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("Expected %v, got %v", exp, got)
	}
}

func newTestEngine() *SearchEngine {
	se := &SearchEngine{
		DataMap: map[string]map[uint32]int{
			"apple": {
				1: 10,
				2: 20,
				3: 5,
			},
			"iphone": {
				1: 30,
				2: 15,
			},
			"phone": {
				2: 25,
				3: 40,
			},
		},
		DocDeleted: map[uint32]bool{
			3: true,
		},
		Documents: map[uint32]map[string]interface{}{
			1: {"id": "doc1"},
			2: {"id": "doc2"},
			3: {"id": "doc3"},
		},
		InternalToExternal: map[uint32]string{
			1: "doc1",
			2: "doc2",
			3: "doc3",
		},
		PageSize: 2,
	}
	return se
}

func newTestEngineForMultiTerm() *SearchEngine {
	se := &SearchEngine{
		DataMap: map[string]map[uint32]int{
			"apple": {
				1: 10,
				2: 20,
				3: 5,
			},
			"mapple": {
				4: 7,
				2: 20,
				7: 5,
			},
			"iphone": {
				10: 30,
				2:  15,
			},
			"phone": {
				7:  25,
				12: 40,
			},
			"phona": {
				70: 25,
			},
		},
		DocDeleted: map[uint32]bool{
			3: true,
		},
		Documents: map[uint32]map[string]interface{}{
			1:  {"id": "doc1"},
			2:  {"id": "doc2"},
			3:  {"id": "doc3"},
			4:  {"id": "doc4"},
			7:  {"id": "doc7"},
			10: {"id": "doc10"},
			12: {"id": "doc12"},
			70: {"id": "doc70"},
		},
		InternalToExternal: map[uint32]string{
			1:  "doc1",
			2:  "doc2",
			3:  "doc3",
			4:  "doc4",
			7:  "doc7",
			10: "doc10",
			12: "doc12",
			70: "doc70",
		},
		PageSize: 2,
	}
	return se
}

func TestSearchOneTermBasic(t *testing.T) {
	se := newTestEngine()

	res := se.SearchOneTermWithoutFilter("apple", 0)
	if len(res) != 2 {
		t.Fatalf("expected 2 results, got %d", len(res))
	}

	if res[0].ID != "doc2" || res[0].Score != 20 {
		t.Fatalf("unexpected top result: %+v", res[0])
	}
}

func TestSearchOneTermPaging(t *testing.T) {
	se := newTestEngine()

	res := se.SearchOneTermWithoutFilter("apple", 1)
	if len(res) != 0 {
		t.Fatalf("expected empty page, got %+v", res)
	}
}

func TestSearchOneTermDeleted(t *testing.T) {
	se := newTestEngine()

	res := se.SearchOneTermWithoutFilter("phone", 0)
	if len(res) != 1 {
		t.Fatalf("expected 1 result, got %d", len(res))
	}

	if res[0].ID != "doc2" {
		t.Fatalf("deleted doc returned or wrong doc: %+v", res[0])
	}
}

func TestSearchMultiTermAND_OR(t *testing.T) {
	se := newTestEngineForMultiTerm()

	terms := [][]string{
		{"apple", "mapple"},
		{"iphone", "phone", "phona"},
	}

	res := se.SearchMultipleTermsWithoutFilter(terms, 0)
	if len(res) != 2 {
		t.Fatalf("expected 2 result, got %d, res=%+v", len(res), res)
	}

	// Only doc2 matches: (apple OR mapple) AND (iphone OR phone OR phona)
	if res[0].ID != "doc2" {
		t.Fatalf("unexpected result: %+v", res[0])
	}
	if res[1].ID != "doc7" {
		t.Fatalf("unexpected result: %+v", res[1])
	}
}

func TestSearchMultiTermScoreAggregation(t *testing.T) {
	se := newTestEngine()

	terms := [][]string{
		{"apple"},
		{"iphone"},
	}

	res := se.SearchMultipleTermsWithoutFilter(terms, 0)
	if len(res) != 2 {
		t.Fatalf("expected 2 result, got %d, res=%+v", len(res), res)
	}

	// doc1 score = apple(10) + iphone(30) = 40
	if res[0].ID != "doc1" || res[0].Score != 40 {
		t.Fatalf("unexpected score aggregation: %+v", res[0])
	}
	if res[1].ID != "doc2" || res[1].Score != 35 {
		t.Fatalf("unexpected score aggregation: %+v", res[1])
	}
}

func TestSearchMultiTermEmpty(t *testing.T) {
	se := newTestEngine()

	res := se.SearchMultipleTermsWithoutFilter([][]string{{"nonexistent"}}, 0)
	if res != nil {
		t.Fatalf("expected nil result")
	}
}

func newTestEngineForE2E() *SearchEngine {
	// indexFields: which doc fields are tokenized into inverted index
	// filters: which doc fields are indexed into FilterDocs ("field:value")
	return NewSearchEngine(
		[]string{"title"},
		map[string]bool{"genre": true},
		10, // page size
	)
}

func idsFromDocs(docs []ReturnedDocument) []string {
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.ID)
	}
	sort.Strings(out)
	return out
}

func assertIDs(t *testing.T, got []ReturnedDocument, expIDs ...string) {
	t.Helper()

	gotIDs := idsFromDocs(got)
	sort.Strings(expIDs)

	if len(gotIDs) != len(expIDs) {
		t.Fatalf("unexpected result count: got=%v exp=%v", gotIDs, expIDs)
	}

	for i := range gotIDs {
		if gotIDs[i] != expIDs[i] {
			t.Fatalf("unexpected ids: got=%v exp=%v", gotIDs, expIDs)
		}
	}
}

func TestAddOrUpdateAndDelete_E2E(t *testing.T) {
	se := newTestEngineForE2E()

	// 1) Add some documents (single-doc API)
	err := se.AddOrUpdateDocument(map[string]interface{}{
		"id":    "1",
		"title": "Sunny Rio",
		"genre": "rock",
	})
	if err != nil {
		t.Fatalf("AddOrUpdateDocument doc1: %v", err)
	}

	err = se.AddOrUpdateDocument(map[string]interface{}{
		"id":    "2",
		"title": "Rio Nights",
		"genre": "pop",
	})
	if err != nil {
		t.Fatalf("AddOrUpdateDocument doc2: %v", err)
	}

	err = se.AddOrUpdateDocument(map[string]interface{}{
		"id":    "3",
		"title": "Cloudy Day",
		"genre": "jazz",
	})
	if err != nil {
		t.Fatalf("AddOrUpdateDocument doc3: %v", err)
	}

	// 2) Exact-term searches should find what we indexed
	assertIDs(t, se.SearchOneTermWithoutFilter("sunny", 0), "1")
	assertIDs(t, se.SearchOneTermWithoutFilter("rio", 0), "1", "2")
	assertIDs(t, se.SearchOneTermWithoutFilter("cloudy", 0), "3")

	// 3) Update doc2: remove "rio", add "sunny"
	// Old internal version should become tombstoned, new version indexed.
	err = se.AddOrUpdateDocument(map[string]interface{}{
		"id":    "2",
		"title": "Sunny Days",
		"genre": "pop",
	})
	if err != nil {
		t.Fatalf("AddOrUpdateDocument doc2 update: %v", err)
	}

	// Now "rio" should no longer include doc2 (old internal is deleted)
	assertIDs(t, se.SearchOneTermWithoutFilter("rio", 0), "1")

	// "sunny" should include doc1 and updated doc2
	// (order not guaranteed, we compare as a set)
	assertIDs(t, se.SearchOneTermWithoutFilter("sunny", 0), "1", "2")

	// 4) Delete doc1 and verify it disappears from results
	if ok := se.DeleteDocument("1"); !ok {
		t.Fatalf("DeleteDocument(1) expected true")
	}

	assertIDs(t, se.SearchOneTermWithoutFilter("sunny", 0), "2")
	assertIDs(t, se.SearchOneTermWithoutFilter("rio", 0)) // empty

	// Deleting an unknown external ID should return false
	if ok := se.DeleteDocument("does-not-exist"); ok {
		t.Fatalf("DeleteDocument(does-not-exist) expected false")
	}

	// 5) Add a new doc4 and verify it shows up
	err = se.AddOrUpdateDocument(map[string]interface{}{
		"id":    "4",
		"title": "Rio Sunny",
		"genre": "rock",
	})
	if err != nil {
		t.Fatalf("AddOrUpdateDocument doc4: %v", err)
	}

	assertIDs(t, se.SearchOneTermWithoutFilter("rio", 0), "4")
	assertIDs(t, se.SearchOneTermWithoutFilter("sunny", 0), "2", "4")

	// 6) E2E via Search() as well (single term path)
	// (Uses prefix/fuzzy expansion internally, but for these exact terms it should include the same docs.)
	res := se.Search("sunny", 0, nil)
	if res == nil {
		t.Fatalf("Search returned nil")
	}
	assertIDs(t, res.Docs, "2", "4")

	// 7) Filtered search (if you wired filters end-to-end as discussed):
	// genre:pop should return only doc2 for "sunny"
	filters := map[string][]interface{}{
		"genre": {"pop"},
	}
	res = se.Search("sunny", 0, filters)
	if res == nil {
		t.Fatalf("Search (filtered) returned nil")
	}
	// If your Search() wiring applies filters, this should pass.
	// If you haven't wired filters into Search() yet, use SearchOneTermWithFilter below.
	assertIDs(t, res.Docs, "2")

	// Extra safety: leaf filtered function should always work if present.
	assertIDs(t, se.SearchOneTermWithFilter("sunny", 0, filters), "2")
}

func docIDs(docs []ReturnedDocument) []string {
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.ID)
	}
	sort.Strings(out)
	return out
}

func containsString(arr []string, s string) bool {
	for _, x := range arr {
		if x == s {
			return true
		}
	}
	return false
}

func TestSaveLoad_RebuildsIndexesFromDocuments(t *testing.T) {
	// 1) Build engine + mutate state (add/update/delete)
	se := NewSearchEngine(
		[]string{"name"},
		map[string]bool{"year": true},
		10,
	)

	// Add docs
	if err := se.AddOrUpdateDocument(map[string]interface{}{
		"id":   "1",
		"name": "Sunny Rio",
		"year": "2020",
	}); err != nil {
		t.Fatalf("AddOrUpdateDocument doc1: %v", err)
	}
	if err := se.AddOrUpdateDocument(map[string]interface{}{
		"id":   "2",
		"name": "Rio Nights",
		"year": "2021",
	}); err != nil {
		t.Fatalf("AddOrUpdateDocument doc2: %v", err)
	}

	// Update doc2: remove "rio", add "sunny"
	if err := se.AddOrUpdateDocument(map[string]interface{}{
		"id":   "2",
		"name": "Sunny Days",
		"year": "2021",
	}); err != nil {
		t.Fatalf("AddOrUpdateDocument doc2 update: %v", err)
	}

	// Delete doc1
	if ok := se.DeleteDocument("1"); !ok {
		t.Fatalf("DeleteDocument(1) expected true")
	}

	// Sanity before save
	assertIDs(t, se.SearchOneTermWithoutFilter("sunny", 0), "2")
	assertIDs(t, se.SearchOneTermWithoutFilter("rio", 0)) // should be empty now

	// 2) Save to temp dir
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir temp dir: %v", err)
	}

	if err := se.SaveAll(dir); err != nil {
		t.Fatalf("SaveAll: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "engine.gob")); err != nil {
		t.Fatalf("expected engine.gob to exist: %v", err)
	}

	// 3) Load and ensure derived structures are rebuilt from Documents
	loaded, err := LoadAll(dir)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	// Metadata restored
	if loaded.PageSize != 10 {
		t.Fatalf("PageSize mismatch: got=%d exp=%d", loaded.PageSize, 10)
	}
	if len(loaded.IndexFields) != 1 || loaded.IndexFields[0] != "name" {
		t.Fatalf("IndexFields mismatch: got=%v", loaded.IndexFields)
	}
	if !loaded.Filters["year"] {
		t.Fatalf("Filters mismatch: expected Filters[year]=true")
	}

	// Searches work (meaning DataMap rebuilt and tombstones respected)
	assertIDs(t, loaded.SearchOneTermWithoutFilter("sunny", 0), "2")
	assertIDs(t, loaded.SearchOneTermWithoutFilter("rio", 0)) // empty

	// Filter logic rebuilt
	filtered := loaded.SearchOneTermWithFilter("sunny", 0, map[string][]interface{}{"year": {"2021"}})
	assertIDs(t, filtered, "2")
	filtered = loaded.SearchOneTermWithFilter("sunny", 0, map[string][]interface{}{"year": {"2020"}})
	assertIDs(t, filtered) // empty

	// Derived structures sanity checks (not exhaustive, but ensures rebuild happened)
	if loaded.DataMap["sunny"] == nil {
		t.Fatalf("expected DataMap to contain 'sunny' after rebuild")
	}
	if loaded.FilterDocs["year:2021"] == nil || len(loaded.FilterDocs["year:2021"]) == 0 {
		t.Fatalf("expected FilterDocs to contain key 'year:2021' after rebuild")
	}

	// Prefix should have 'sunny' under 'su' (exact placement depends on your prefix builder)
	if len(loaded.Prefix["su"]) == 0 {
		t.Fatalf("expected Prefix['su'] to be non-empty after rebuild")
	}

	// Symspell + Keys should have the indexed term
	if loaded.Keys == nil || loaded.Keys.GetData() == nil {
		t.Fatalf("expected Keys to be non-nil after rebuild")
	}
	if _, ok := loaded.Keys.GetData()["sunny"]; !ok {
		t.Fatalf("expected Keys to contain 'sunny' after rebuild")
	}
	if loaded.Symspell == nil {
		t.Fatalf("expected Symspell to be non-nil after rebuild")
	}
	// SymSpell often returns empty for an exact word; test a near-miss instead.
	fz := loaded.Symspell.FuzzySearch("suny", 10) // missing one 'n'
	if len(fz) == 0 {
		t.Fatalf("expected Symspell.FuzzySearch('suny') to return suggestions after rebuild, got=%v", fz)
	}
	// Optional: if your implementation usually suggests the correct word:
	if !containsString(fz, "sunny") {
		t.Logf("note: Symspell suggestions for 'suny' did not include 'sunny': %v", fz)
	}

	// 4) Verify nextInternalID is usable after load by inserting a new doc
	if err := loaded.AddOrUpdateDocument(map[string]interface{}{
		"id":   "3",
		"name": "Rio Sunny",
		"year": "2022",
	}); err != nil {
		t.Fatalf("AddOrUpdateDocument doc3 after load: %v", err)
	}

	assertIDs(t, loaded.SearchOneTermWithoutFilter("rio", 0), "3")
	assertIDs(t, loaded.SearchOneTermWithoutFilter("sunny", 0), "2", "3")
}
