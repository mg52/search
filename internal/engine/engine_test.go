package engine

import (
	"container/heap"
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
		ResultSize: 100,
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
		ResultSize: 100,
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
		ResultSize: 2,
	}
	return se
}

func TestSearchOneTermBasic(t *testing.T) {
	se := newTestEngine()

	res := se.SearchOneTermWithoutFilter("apple")
	if len(res) != 2 {
		t.Fatalf("expected 2 results, got %d", len(res))
	}

	if res[0].ID != "doc2" || res[0].Score != 20 {
		t.Fatalf("unexpected top result: %+v", res[0])
	}
}

func TestSearchOneTermDeleted(t *testing.T) {
	se := newTestEngine()

	res := se.SearchOneTermWithoutFilter("phone")
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

	res := se.SearchMultipleTermsWithoutFilter(terms)
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

	res := se.SearchMultipleTermsWithoutFilter(terms)
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

	res := se.SearchMultipleTermsWithoutFilter([][]string{{"nonexistent"}})
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
	assertIDs(t, se.SearchOneTermWithoutFilter("sunny"), "1")
	assertIDs(t, se.SearchOneTermWithoutFilter("rio"), "1", "2")
	assertIDs(t, se.SearchOneTermWithoutFilter("cloudy"), "3")

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
	assertIDs(t, se.SearchOneTermWithoutFilter("rio"), "1")

	// "sunny" should include doc1 and updated doc2
	// (order not guaranteed, we compare as a set)
	assertIDs(t, se.SearchOneTermWithoutFilter("sunny"), "1", "2")

	// 4) Delete doc1 and verify it disappears from results
	if ok := se.DeleteDocument("1"); !ok {
		t.Fatalf("DeleteDocument(1) expected true")
	}

	assertIDs(t, se.SearchOneTermWithoutFilter("sunny"), "2")
	assertIDs(t, se.SearchOneTermWithoutFilter("rio")) // empty

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

	assertIDs(t, se.SearchOneTermWithoutFilter("rio"), "4")
	assertIDs(t, se.SearchOneTermWithoutFilter("sunny"), "2", "4")

	// 6) E2E via Search() as well (single term path)
	// (Uses prefix/fuzzy expansion internally, but for these exact terms it should include the same docs.)
	res := se.Search("sunny", nil)
	if res == nil {
		t.Fatalf("Search returned nil")
	}
	assertIDs(t, res.Docs, "2", "4")

	// 7) Filtered search (if you wired filters end-to-end as discussed):
	// genre:pop should return only doc2 for "sunny"
	filters := map[string][]interface{}{
		"genre": {"pop"},
	}
	res = se.Search("sunny", filters)
	if res == nil {
		t.Fatalf("Search (filtered) returned nil")
	}
	// If your Search() wiring applies filters, this should pass.
	// If you haven't wired filters into Search() yet, use SearchOneTermWithFilter below.
	assertIDs(t, res.Docs, "2")

	// Extra safety: leaf filtered function should always work if present.
	assertIDs(t, se.SearchOneTermWithFilter("sunny", filters), "2")
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
	assertIDs(t, se.SearchOneTermWithoutFilter("sunny"), "2")
	assertIDs(t, se.SearchOneTermWithoutFilter("rio")) // should be empty now

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
	if loaded.ResultSize != 10 {
		t.Fatalf("PageSize mismatch: got=%d exp=%d", loaded.ResultSize, 10)
	}
	if len(loaded.IndexFields) != 1 || loaded.IndexFields[0] != "name" {
		t.Fatalf("IndexFields mismatch: got=%v", loaded.IndexFields)
	}
	if !loaded.Filters["year"] {
		t.Fatalf("Filters mismatch: expected Filters[year]=true")
	}

	// Searches work (meaning DataMap rebuilt and tombstones respected)
	assertIDs(t, loaded.SearchOneTermWithoutFilter("sunny"), "2")
	assertIDs(t, loaded.SearchOneTermWithoutFilter("rio")) // empty

	// Filter logic rebuilt
	filtered := loaded.SearchOneTermWithFilter("sunny", map[string][]interface{}{"year": {"2021"}})
	assertIDs(t, filtered, "2")
	filtered = loaded.SearchOneTermWithFilter("sunny", map[string][]interface{}{"year": {"2020"}})
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

	assertIDs(t, loaded.SearchOneTermWithoutFilter("rio"), "3")
	assertIDs(t, loaded.SearchOneTermWithoutFilter("sunny"), "2", "3")
}

func TestMultiTermSearch_E2E(t *testing.T) {
	se := NewSearchEngine(
		[]string{"title", "tags"},      // indexed fields
		map[string]bool{"genre": true}, // filters
		10,
	)

	// 1) Insert documents
	docs := []map[string]interface{}{
		{
			"id":    "1",
			"title": "Apple iPhone Pro",
			"genre": "tech",
			"tags":  "phone mobile",
		},
		{
			"id":    "2",
			"title": "Apple Phone Basic",
			"genre": "tech",
			"tags":  "phone mobile",
		},
		{
			"id":    "3",
			"title": "Samsung Phone",
			"genre": "tech",
			"tags":  "phone mobile",
		},
		{
			"id":    "4",
			"title": "Apple Banana",
			"genre": "food",
			"tags":  "fruit",
		},
	}

	for _, d := range docs {
		if err := se.AddOrUpdateDocument(d); err != nil {
			t.Fatalf("AddOrUpdateDocument failed: %v", err)
		}
	}

	// 2) Multi-term search
	// Query: "apple phone"
	// Expected logic:
	// AND across terms:
	//   must match BOTH "apple" AND "phone"
	//
	// Matching docs:
	// - doc1: "Apple iPhone Pro" (iphone fuzzy/prefix match)
	// - doc2: "Apple Phone Basic"
	// - doc4: "Apple Banana" -> excluded (no phone)
	// - doc3: "Samsung Phone" -> excluded (no apple)

	res := se.Search("apple phone", nil)
	if res == nil {
		t.Fatalf("Search returned nil")
	}

	assertIDs(t, res.Docs, "1", "2")

	// 3) Filtered multi-term search
	// Only genre=tech → still doc1 + doc2
	filters := map[string][]interface{}{
		"genre": {"tech"},
	}

	res = se.Search("apple phone", filters)
	if res == nil {
		t.Fatalf("Filtered search returned nil")
	}

	assertIDs(t, res.Docs, "1", "2")

	// 4) Filter that excludes one result
	// genre=food → should remove both (since neither doc1/doc2 are food)
	filters = map[string][]interface{}{
		"genre": {"food"},
	}

	res = se.Search("apple phone", filters)
	if res == nil {
		t.Fatalf("Filtered search returned nil")
	}

	assertIDs(t, res.Docs) // expect empty
}

func TestMultiTermSearch_E2E_WithScoringOrder(t *testing.T) {
	se := NewSearchEngine(
		[]string{"title", "tags"},
		map[string]bool{"genre": true},
		10,
	)

	// 1) Insert documents with intentional scoring differences
	// We control score via:
	// - repeated tokens
	// - multiple fields
	// - token count normalization

	docs := []map[string]interface{}{
		{
			"id":    "1",
			"title": "apple phone phone", // phone twice
			"tags":  "phone mobile",      // +1 phone
			"genre": "tech",
		},
		{
			"id":    "2",
			"title": "apple phone", // phone once
			"tags":  "phone",       // +1 phone
			"genre": "tech",
		},
		{
			"id":    "3",
			"title": "apple device",
			"tags":  "phone", // phone only in tags
			"genre": "tech",
		},
		{
			"id":    "4",
			"title": "apple something else",
			"tags":  "other",
			"genre": "tech",
		},
	}

	for _, d := range docs {
		if err := se.AddOrUpdateDocument(d); err != nil {
			t.Fatalf("AddOrUpdateDocument failed: %v", err)
		}
	}

	// 2) Multi-term search
	// appre -> system will fuzzy fix it as apple
	// pho -> system will find the word phone for prefix pho
	res := se.Search("appre pho", nil)
	if res == nil {
		t.Fatalf("Search returned nil")
	}

	if len(res.Docs) != 3 {
		t.Fatalf("expected 3 results, got %d: %+v", len(res.Docs), res.Docs)
	}

	gotOrder := []string{
		res.Docs[0].ID,
		res.Docs[1].ID,
		res.Docs[2].ID,
	}

	expectedOrder := []string{"2", "1", "3"}

	for i := range expectedOrder {
		if gotOrder[i] != expectedOrder[i] {
			t.Fatalf("unexpected ranking order: got=%v expected=%v", gotOrder, expectedOrder)
		}
	}

	// 3) Extra: assert strictly descending scores (stronger check)
	if !(res.Docs[0].Score >= res.Docs[1].Score &&
		res.Docs[1].Score >= res.Docs[2].Score) {
		t.Fatalf("scores not sorted descending: %+v", res.Docs)
	}
}

func TestIndex_MultipleBatches(t *testing.T) {
	se := NewSearchEngine(
		[]string{"title", "tags"},
		map[string]bool{"genre": true},
		10,
	)

	// First batch
	batch1 := []map[string]interface{}{
		{
			"id":    "1",
			"title": "Apple Phone",
			"tags":  "mobile tech",
			"genre": "tech",
		},
		{
			"id":    "2",
			"title": "Samsung Tablet",
			"tags":  "device tech",
			"genre": "tech",
		},
	}

	// Second batch
	batch2 := []map[string]interface{}{
		{
			"id":    "3",
			"title": "Banana Fruit",
			"tags":  "food yellow",
			"genre": "food",
		},
		{
			"id":    "4",
			"title": "Gaming Laptop",
			"tags":  "computer performance",
			"genre": "gaming",
		},
	}

	// Index first batch
	se.Index(batch1)

	// Index second batch
	se.Index(batch2)

	// Verify all docs searchable
	tests := []struct {
		query      string
		expectedID string
	}{
		{"apple", "1"},
		{"samsung", "2"},
		{"banana", "3"},
		{"gaming", "4"},
	}

	for _, tt := range tests {
		res := se.Search(tt.query, nil)

		if res == nil {
			t.Fatalf("search returned nil for query %q", tt.query)
		}

		if len(res.Docs) == 0 {
			t.Fatalf("expected results for query %q", tt.query)
		}

		found := false
		for _, d := range res.Docs {
			if d.ID == tt.expectedID {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("expected doc %s for query %q, got %+v",
				tt.expectedID, tt.query, res.Docs)
		}
	}
}

func TestMinHeap_PushPopOrder(t *testing.T) {
	h := &minHeap{}

	heap.Init(h)

	input := []internalHit{
		{id: 1, score: 50},
		{id: 2, score: 10},
		{id: 3, score: 30},
		{id: 4, score: 5},
		{id: 5, score: 20},
	}

	for _, v := range input {
		heap.Push(h, v)
	}

	expectedOrder := []int{5, 10, 20, 30, 50}

	for _, want := range expectedOrder {
		got := heap.Pop(h).(internalHit)
		if got.score != want {
			t.Errorf("expected %v, got %v", want, got.score)
		}
	}
}

func TestMinHeap_Len(t *testing.T) {
	h := &minHeap{}
	heap.Init(h)

	if h.Len() != 0 {
		t.Fatalf("expected empty heap")
	}

	heap.Push(h, internalHit{id: 1, score: 10})
	heap.Push(h, internalHit{id: 2, score: 20})

	if h.Len() != 2 {
		t.Fatalf("expected len 2, got %d", h.Len())
	}
}

func TestMinHeap_SingleElement(t *testing.T) {
	h := &minHeap{}
	heap.Init(h)

	heap.Push(h, internalHit{id: 1, score: 42})

	x := heap.Pop(h).(internalHit)

	if x.score != 42 {
		t.Errorf("expected 42, got %v", x.score)
	}

	if h.Len() != 0 {
		t.Errorf("expected empty heap after pop")
	}
}

func TestMinHeap_StabilityRandomInsertions(t *testing.T) {
	h := &minHeap{}
	heap.Init(h)

	values := []int{100, 1, 50, 2, 99, 3, 75, 4, 60}

	for i, v := range values {
		heap.Push(h, internalHit{id: uint32(i), score: v})
	}

	last := -1

	for h.Len() > 0 {
		x := heap.Pop(h).(internalHit)
		if last > x.score {
			t.Errorf("heap order violated: %v came after %v", x.score, last)
		}
		last = x.score
	}
}

func TestMinHeap_StabilityRandomInsertions2(t *testing.T) {
	h := &minHeap{}
	heap.Init(h)

	values := []int{100, 105, 1, 50, 2, 99, 101, 3, 75, 4, 60, 104, 110, 95, 90, 90, 106, 8, 111, 101, 106, 79}

	k := 5
	for i, score := range values {
		if h.Len() < k {
			heap.Push(h, internalHit{id: uint32(i), score: score})
		} else if (*h)[0].score < score {
			heap.Pop(h)
			heap.Push(h, internalHit{id: uint32(i), score: score})
		}
	}

	last := -1

	var scores []int
	for h.Len() > 0 {
		x := heap.Pop(h).(internalHit)
		if last > x.score {
			t.Errorf("heap order violated: %v came after %v", x.score, last)
		}
		last = x.score
		scores = append(scores, x.score)
	}
	if scores[0] != 105 || scores[1] != 106 || scores[2] != 106 || scores[3] != 110 || scores[4] != 111 {
		t.Errorf("Score violated")
	}
}
