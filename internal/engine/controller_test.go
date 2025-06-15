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
)

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

func TestAddDocumentToFreshIndex(t *testing.T) {
	indexFields := []string{"name", "tags"}
	filters := map[string]bool{"year": true}
	sec := NewSearchEngineController(indexFields, filters, 15, 4)
	doc1 := map[string]interface{}{
		"id":          "1",
		"name":        "abece Affordable kalemite asdasdfsf",
		"description": "get value segment try week great real end high image ergonomic broad pass beat.",
		"tags":        []interface{}{"section", "rose", "sadvde435r"},
		"year":        2015,
	}
	sec.AddOrUpdateDocument(doc1)

	doc2 := map[string]interface{}{
		"id":          "2",
		"name":        "affordable test123",
		"description": "get value segment broad pass beat.",
		"tags":        []interface{}{"section", "rose", "big"},
		"year":        2018,
	}
	sec.AddOrUpdateDocument(doc2)

	result := sec.Search("afford", 0, nil)

	if len(result) != 2 {
		t.Errorf("Expected search result is 2, got %d", len(result))
	}
	if result[0].ID != "2" || result[0].ScoreWeight != 20000 {
		t.Errorf("Expected 1. document ID is 2, score is 20000, got ID: %v, Score: %d",
			result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "1" || result[1].ScoreWeight != 14285 {
		t.Errorf("Expected 2. document ID is 1, score is 14285, got ID: %v, Score: %d",
			result[1].ID, result[1].ScoreWeight)
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

func TestSearchEngineControllerFlow_Pagination(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	// Create controller
	filtersConfig := map[string]bool{"year": true}
	sec := NewSearchEngineController(
		[]string{"name", "tags"},
		filtersConfig,
		2, // pageCount
		1, // workers
	)
	sec.Index(docs)

	emptyFilters := make(map[string][]interface{})

	result := sec.Search("optimal afford", 0, emptyFilters)
	if len(result) != 2 {
		t.Errorf("expected 2 results for 'optimal afford', got %d: %#v", len(result), result)
	}
	if result[0].ID != "15" || result[0].ScoreWeight != 100000 {
		t.Errorf("Expected 1. document ID is 15, score is 100000, got ID: %v, Score: %d",
			result[0].ID, result[0].ScoreWeight)
	}
	if !((result[1].ID == "13" || result[1].ID == "19") && result[1].ScoreWeight == 66666) {
		t.Errorf("Expected 2. document ID is 13 or 19, score is 66666, got ID: %v, Score: %d",
			result[1].ID, result[1].ScoreWeight)
	}

	result = sec.Search("optimal afford", 1, emptyFilters)
	if len(result) != 2 {
		t.Errorf("expected 2 results for 'optimal afford', got %d: %#v", len(result), result)
	}
	if !((result[0].ID == "13" || result[0].ID == "19") && result[0].ScoreWeight == 66666) {
		t.Errorf("Expected 1. document ID is 13 or 19, score is 66666, got ID: %v, Score: %d",
			result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "17" || result[1].ScoreWeight != 50000 {
		t.Errorf("Expected 2. document ID is 17, score is 50000, got ID: %v, Score: %d",
			result[1].ID, result[1].ScoreWeight)
	}

	result = sec.Search("optimal afford", 2, emptyFilters)
	if len(result) != 1 {
		t.Errorf("expected 1 results for 'optimal afford', got %d: %#v", len(result), result)
	}
	if result[0].ID != "14" || result[0].ScoreWeight != 60000 {
		t.Errorf("Expected 1. document ID is 14, score is 60000, got ID: %v, Score: %d",
			result[0].ID, result[0].ScoreWeight)
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
