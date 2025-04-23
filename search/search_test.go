package search

import (
	_ "embed"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

//go:embed products.json
var testProductsJSON []byte

func TestInsertAndSearch(t *testing.T) {
	tr := NewTrie()
	// insert some keys
	keys := []string{"app", "apple", "banana", "apple"}
	for _, k := range keys {
		tr.Insert(k)
	}

	t.Run("SearchPrefix 'app'", func(t *testing.T) {
		got := tr.SearchPrefix("app")
		exp := []string{"app", "apple"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("SearchPrefix(\"app\") = %v; want %v", got, exp)
		}
	})

	t.Run("SearchPrefix 'ban'", func(t *testing.T) {
		got := tr.SearchPrefix("ban")
		exp := []string{"banana"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("SearchPrefix(\"ban\") = %v; want %v", got, exp)
		}
	})

	t.Run("Search non-existent prefix", func(t *testing.T) {
		if got := tr.SearchPrefix("xyz"); got != nil {
			t.Errorf("SearchPrefix(\"xyz\") = %v; want nil", got)
		}
	})
}

func TestRemove(t *testing.T) {
	tr := NewTrie()
	// insert keys
	keys := []string{"app", "apple", "appol"}
	for _, k := range keys {
		tr.Insert(k)
	}

	t.Run("Remove existing leaf 'apple'", func(t *testing.T) {
		if err := tr.Remove("apple"); err != nil {
			t.Fatalf("Remove(\"apple\") error: %v", err)
		}
		got := tr.SearchPrefix("app")
		exp := []string{"app", "appol"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("After Remove apple, SearchPrefix(\"app\") = %v; want %v", got, exp)
		}
	})

	t.Run("Remove existing leaf 'appol'", func(t *testing.T) {
		if err := tr.Remove("appol"); err != nil {
			t.Fatalf("Remove(\"apple\") error: %v", err)
		}
		got := tr.SearchPrefix("app")
		exp := []string{"app"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("After Remove apple, SearchPrefix(\"app\") = %v; want %v", got, exp)
		}
	})

	t.Run("Remove existing prefix 'app'", func(t *testing.T) {
		// first ensure 'app' is still there
		if err := tr.Remove("app"); err != nil {
			t.Fatalf("Remove(\"app\") error: %v", err)
		}
		if got := tr.SearchPrefix("app"); len(got) != 0 {
			t.Errorf("After Remove app, SearchPrefix(\"app\") = %v; want empty", got)
		}
	})

	t.Run("Remove non-existent key", func(t *testing.T) {
		if err := tr.Remove("nonexistent"); err == nil {
			t.Errorf("Remove(\"nonexistent\") = nil; want error")
		}
	})
}

func TestOneTermSearchWithFilter(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	weights := map[string]int{"name": 2, "description": 1, "tags": 1}
	filters := map[string]bool{"year": true}
	sec := NewSearchEngineController(weights, filters, 10, 1)
	sec.Index(docs)

	filter := make(map[string][]interface{})
	filter["year"] = append(filter["year"], 2012)
	result := sec.Search("sleek", 0, filter)

	if len(result) != 3 {
		t.Errorf("Expected search result is 3, got %d", len(result))
	}
	if result[0].ID != "3" || result[0].ScoreWeight != 1025000 {
		t.Errorf("Expected 1. document ID is 3, score is 1025000, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "2" || result[1].ScoreWeight != 1000014 {
		t.Errorf("Expected 2. document ID is 2, score is 1000014, got ID: %v, Score: %d", result[1].ID, result[1].ScoreWeight)
	}
	if result[2].ID != "7" || result[2].ScoreWeight != 1000007 {
		t.Errorf("Expected 3. document ID is 7, score is 1000007, got ID: %v, Score: %d", result[2].ID, result[2].ScoreWeight)
	}
}

func TestOneTermSearchWithoutFilter(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	weights := map[string]int{"name": 2, "description": 1, "tags": 1}
	filters := map[string]bool{}
	sec := NewSearchEngineController(weights, filters, 10, 1)
	sec.Index(docs)

	filter := make(map[string][]interface{})
	result := sec.Search("sleek", 0, filter)

	if len(result) != 5 {
		t.Errorf("Expected search result is 5, got %d", len(result))
	}
	if result[0].ID != "1" || result[0].ScoreWeight != 1066014 {
		t.Errorf("Expected 1. document ID is 1, score is 1066014, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "6" || result[1].ScoreWeight != 1050014 {
		t.Errorf("Expected 2. document ID is 6, score is 1050014, got ID: %v, Score: %d", result[1].ID, result[1].ScoreWeight)
	}
	if result[2].ID != "3" || result[2].ScoreWeight != 1025000 {
		t.Errorf("Expected 3. document ID is 3, score is 1025000, got ID: %v, Score: %d", result[2].ID, result[2].ScoreWeight)
	}
	if result[3].ID != "2" || result[3].ScoreWeight != 1000014 {
		t.Errorf("Expected 4. document ID is 2, score is 1000014, got ID: %v, Score: %d", result[3].ID, result[3].ScoreWeight)
	}
	if result[4].ID != "7" || result[4].ScoreWeight != 1000007 {
		t.Errorf("Expected 5. document ID is 7, score is 1000007, got ID: %v, Score: %d", result[4].ID, result[4].ScoreWeight)
	}
}

func TestOneTermSearchWithoutFilterPagination(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	weights := map[string]int{"name": 2, "description": 1, "tags": 1}
	filters := map[string]bool{}
	sec := NewSearchEngineController(weights, filters, 3, 1)
	sec.Index(docs)

	filter := make(map[string][]interface{})
	// First page
	result := sec.Search("sleek", 0, filter)

	if len(result) != 3 {
		t.Errorf("Expected search result is 3, got %d", len(result))
	}
	if result[0].ID != "1" || result[0].ScoreWeight != 1066014 {
		t.Errorf("Expected 1. document ID is 1, score is 1066014, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "6" || result[1].ScoreWeight != 1050014 {
		t.Errorf("Expected 2. document ID is 6, score is 1050014, got ID: %v, Score: %d", result[1].ID, result[1].ScoreWeight)
	}
	if result[2].ID != "3" || result[2].ScoreWeight != 1025000 {
		t.Errorf("Expected 3. document ID is 3, score is 1025000, got ID: %v, Score: %d", result[2].ID, result[2].ScoreWeight)
	}

	// Second page
	result = sec.Search("sleek", 1, filter)

	if len(result) != 2 {
		t.Errorf("Expected search result is 2, got %d", len(result))
	}
	if result[0].ID != "2" || result[0].ScoreWeight != 1000014 {
		t.Errorf("Expected 1. document ID is 2, score is 1000014, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "7" || result[1].ScoreWeight != 1000007 {
		t.Errorf("Expected 2. document ID is 7, score is 1000007, got ID: %v, Score: %d", result[1].ID, result[1].ScoreWeight)
	}
}

func TestOneTermSearchWithFilterPagination(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	weights := map[string]int{"name": 2, "description": 1, "tags": 1}
	filters := map[string]bool{"year": true}
	sec := NewSearchEngineController(weights, filters, 3, 1)
	sec.Index(docs)

	filter := make(map[string][]interface{})
	filter["year"] = append(filter["year"], 2015)
	// First page
	result := sec.Search("affordable", 0, filter)

	if len(result) != 3 {
		t.Errorf("Expected search result is 3, got %d", len(result))
	}
	if result[0].ID != "15" || result[0].ScoreWeight != 1100007 {
		t.Errorf("Expected 1. document ID is 5, score is 1100007, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "14" || result[1].ScoreWeight != 1066000 {
		t.Errorf("Expected 2. document ID is 14, score is 1066000, got ID: %v, Score: %d", result[1].ID, result[1].ScoreWeight)
	}
	if result[2].ID != "13" || result[2].ScoreWeight != 1050000 {
		t.Errorf("Expected 3. document ID is 13, score is 1050000, got ID: %v, Score: %d", result[2].ID, result[2].ScoreWeight)
	}

	// Second page
	result = sec.Search("affordable", 1, filter)

	if len(result) != 3 {
		t.Errorf("Expected search result is 3, got %d", len(result))
	}
	if result[0].ID != "17" || result[0].ScoreWeight != 1033014 {
		t.Errorf("Expected 1. document ID is 17, score is 1033014, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "16" || result[1].ScoreWeight != 1000028 {
		t.Errorf("Expected 2. document ID is 16, score is 1000028, got ID: %v, Score: %d", result[1].ID, result[1].ScoreWeight)
	}
	if !((result[2].ID == "18" || result[2].ID == "20") && result[2].ScoreWeight == 1000014) {
		t.Errorf("Expected 3. document ID is 18 or 20, score is 1000014, got ID: %v, Score: %d", result[2].ID, result[2].ScoreWeight)
	}

	// Third page
	result = sec.Search("affordable", 2, filter)

	if len(result) != 1 {
		t.Errorf("Expected search result is 1, got %d", len(result))
	}
	if !((result[0].ID == "18" || result[0].ID == "20") && result[0].ScoreWeight == 1000014) {
		t.Errorf("Expected 1. document ID is 18 or 20, score is 1000014, got ID: %v, Score: %d", result[2].ID, result[2].ScoreWeight)
	}

	// Forth page
	result = sec.Search("affordable", 3, filter)

	if len(result) != 0 {
		t.Errorf("Expected search result is 0, got %d", len(result))
	}
}

func TestRemoveDocumentByID(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	weights := map[string]int{"name": 2, "description": 1, "tags": 1}
	filters := map[string]bool{"year": true}
	sec := NewSearchEngineController(weights, filters, 10, 1)
	sec.Index(docs)

	filter := make(map[string][]interface{})
	filter["year"] = append(filter["year"], 2012)

	// Search before removing
	result := sec.Search("sleek", 0, filter)

	if len(result) != 3 {
		t.Errorf("Expected search result is 3, got %d", len(result))
	}
	if result[0].ID != "3" || result[0].ScoreWeight != 1025000 {
		t.Errorf("Expected 1. document ID is 3, score is 1025000, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "2" || result[1].ScoreWeight != 1000014 {
		t.Errorf("Expected 2. document ID is 2, score is 1000014, got ID: %v, Score: %d", result[1].ID, result[1].ScoreWeight)
	}
	if result[2].ID != "7" || result[2].ScoreWeight != 1000007 {
		t.Errorf("Expected 3. document ID is 7, score is 1000007, got ID: %v, Score: %d", result[2].ID, result[2].ScoreWeight)
	}

	sec.RemoveDocumentByID("3")

	// Search after removing
	result = sec.Search("sleek", 0, filter)

	if len(result) != 2 {
		t.Errorf("Expected search result is 2, got %d", len(result))
	}
	if result[0].ID != "2" || result[0].ScoreWeight != 1000014 {
		t.Errorf("Expected 1. document ID is 2, score is 1000014, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "7" || result[1].ScoreWeight != 1000007 {
		t.Errorf("Expected 2. document ID is 7, score is 1000007, got ID: %v, Score: %d", result[1].ID, result[1].ScoreWeight)
	}

	sec.RemoveDocumentByID("1")

	// Search after removing
	result = sec.Search("sleek", 0, filter)

	// removing docID 1 should not affect the result since it is not in the year:2012 filter
	if len(result) != 2 {
		t.Errorf("Expected search result is 2, got %d", len(result))
	}
	if result[0].ID != "2" || result[0].ScoreWeight != 1000014 {
		t.Errorf("Expected 1. document ID is 2, score is 1000014, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "7" || result[1].ScoreWeight != 1000007 {
		t.Errorf("Expected 2. document ID is 7, score is 1000007, got ID: %v, Score: %d", result[1].ID, result[1].ScoreWeight)
	}

	sec.RemoveDocumentByID("7")

	// Search after removing
	result = sec.Search("sleek", 0, filter)

	if len(result) != 1 {
		t.Errorf("Expected search result is 2, got %d", len(result))
	}
	if result[0].ID != "2" || result[0].ScoreWeight != 1000014 {
		t.Errorf("Expected 1. document ID is 2, score is 1000014, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
}

func TestUpdateDocument(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	weights := map[string]int{"name": 2, "description": 1, "tags": 1}
	filters := map[string]bool{"year": true}
	sec := NewSearchEngineController(weights, filters, 10, 1)
	sec.Index(docs)

	filter := make(map[string][]interface{})
	filter["year"] = append(filter["year"], 2012)

	// Search before updating
	sleekResult := sec.Search("sleek", 0, filter)

	if len(sleekResult) != 3 {
		t.Errorf("Expected search result is 3, got %d", len(sleekResult))
	}
	if sleekResult[0].ID != "3" || sleekResult[0].ScoreWeight != 1025000 {
		t.Errorf("Expected 1. document ID is 3, score is 1025000, got ID: %v, Score: %d", sleekResult[0].ID, sleekResult[0].ScoreWeight)
	}
	if sleekResult[1].ID != "2" || sleekResult[1].ScoreWeight != 1000014 {
		t.Errorf("Expected 2. document ID is 2, score is 1000014, got ID: %v, Score: %d", sleekResult[1].ID, sleekResult[1].ScoreWeight)
	}
	if sleekResult[2].ID != "7" || sleekResult[2].ScoreWeight != 1000007 {
		t.Errorf("Expected 3. document ID is 7, score is 1000007, got ID: %v, Score: %d", sleekResult[2].ID, sleekResult[2].ScoreWeight)
	}

	// Search before updating
	filter = make(map[string][]interface{})
	compactResult := sec.Search("compact", 0, filter)

	if len(compactResult) != 2 {
		t.Errorf("Expected search result is 2, got %d", len(compactResult))
	}
	if compactResult[0].ID != "2" || compactResult[0].ScoreWeight != 1100000 {
		t.Errorf("Expected 1. document ID is 2, score is 1100000, got ID: %v, Score: %d", compactResult[0].ID, compactResult[0].ScoreWeight)
	}
	if compactResult[1].ID != "1" || compactResult[1].ScoreWeight != 1033000 {
		t.Errorf("Expected 2. document ID is 1, score is 1033000, got ID: %v, Score: %d", compactResult[1].ID, compactResult[1].ScoreWeight)
	}

	// Search before updating
	filter = make(map[string][]interface{})
	istanbulResult := sec.Search("istanb", 0, filter)

	updatedData := map[string]interface{}{
		"id":          "2",
		"name":        "compact trend abcde abcdef",
		"description": "trend precision sleek sleek robust bright superior powerful precision ultimate",
		"tags":        []string{"best-seller", "sleek"},
		"year":        2012,
	}
	sec.UpdateDocument(updatedData)

	// Search after removing
	filter["year"] = append(filter["year"], 2012)
	sleekResult = sec.Search("sleek", 0, filter)

	if len(sleekResult) != 3 {
		t.Errorf("Expected search result is 3, got %d", len(sleekResult))
	}
	if sleekResult[0].ID != "3" || sleekResult[0].ScoreWeight != 1025000 {
		t.Errorf("Expected 1. document ID is 3, score is 1025000, got ID: %v, Score: %d", sleekResult[0].ID, sleekResult[0].ScoreWeight)
	}
	if sleekResult[1].ID != "2" || sleekResult[1].ScoreWeight != 1000070 {
		t.Errorf("Expected 2. document ID is 2, score is 1000070, got ID: %v, Score: %d", sleekResult[1].ID, sleekResult[1].ScoreWeight)
	}
	if sleekResult[2].ID != "7" || sleekResult[2].ScoreWeight != 1000007 {
		t.Errorf("Expected 3. document ID is 7, score is 1000007, got ID: %v, Score: %d", sleekResult[2].ID, sleekResult[2].ScoreWeight)
	}

	// Search after updating
	filter = make(map[string][]interface{})
	brightResult := sec.Search("bright", 0, filter)

	if len(brightResult) != 1 {
		t.Errorf("Expected search result is 1, got %d", len(brightResult))
	}
	if brightResult[0].ID != "2" || brightResult[0].ScoreWeight != 1000010 {
		t.Errorf("Expected 1. document ID is 2, score is 1000010, got ID: %v, Score: %d", brightResult[0].ID, brightResult[0].ScoreWeight)
	}

	// Search after updating
	filter = make(map[string][]interface{})
	compactResult = sec.Search("compact", 0, filter)

	if len(compactResult) != 2 {
		t.Errorf("Expected search result is 2, got %d", len(compactResult))
	}
	if compactResult[0].ID != "1" || compactResult[0].ScoreWeight != 1033000 {
		t.Errorf("Expected 1. document ID is 1, score is 1033000, got ID: %v, Score: %d", compactResult[0].ID, compactResult[0].ScoreWeight)
	}
	if compactResult[1].ID != "2" || compactResult[1].ScoreWeight != 1025000 {
		t.Errorf("Expected 2. document ID is 2, score is 1025000, got ID: %v, Score: %d", compactResult[1].ID, compactResult[1].ScoreWeight)
	}

	// Search after updating. term 'istanb' will yield 'istanbul'
	filter = make(map[string][]interface{})
	istanbulResultNew := sec.Search("istanb", 0, filter)

	// because no document changed related to istanbul term.
	// before and after update, results should be same
	if !reflect.DeepEqual(istanbulResultNew,
		istanbulResult) {
		t.Errorf("ScoreIndex mismatch:\n got %#v\n want %#v", istanbulResultNew, istanbulResult)
	}
}

func TestPartiallyIndex(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	weights := map[string]int{"name": 2, "description": 1, "tags": 1}
	filters := map[string]bool{"year": true}
	sec := NewSearchEngineController(weights, filters, 10, 1)
	sec.Index(docs)

	sec2 := NewSearchEngineController(weights, filters, 10, 1)
	sec2.Index(docs[:10])
	sec2.Index(docs[10:20])
	sec2.Index(docs[20:])

	if !reflect.DeepEqual(sec.Engines[0].Data, sec2.Engines[0].Data) {
		t.Errorf("Data mismatch:\n got %#v\n want %#v", sec2.Engines[0].Data, sec.Engines[0].Data)
	}
	if !reflect.DeepEqual(normalizeScoreIndex(sec.Engines[0].ScoreIndex),
		normalizeScoreIndex(sec2.Engines[0].ScoreIndex)) {
		t.Errorf("ScoreIndex mismatch:\n got %#v\n want %#v", sec2.Engines[0].ScoreIndex, sec.Engines[0].ScoreIndex)
	}
	if !reflect.DeepEqual(sec.Engines[0].FilterDocs, sec2.Engines[0].FilterDocs) {
		t.Errorf("FilterDocs mismatch:\n got %#v\n want %#v", sec2.Engines[0].FilterDocs, sec.Engines[0].FilterDocs)
	}
	if !reflect.DeepEqual(sec.Engines[0].Documents, sec2.Engines[0].Documents) {
		t.Errorf("Documents mismatch:\n got %#v\n want %#v", sec2.Engines[0].Documents, sec.Engines[0].Documents)
	}
}

func TestOneTermSearchWithoutFilterMultiShard(t *testing.T) {
	var docs []map[string]interface{}
	if err := json.Unmarshal(testProductsJSON, &docs); err != nil {
		t.Fatalf("Failed to unmarshal test JSON: %v", err)
	}

	weights := map[string]int{"name": 2, "description": 1, "tags": 1}
	filters := map[string]bool{}
	sec := NewSearchEngineController(weights, filters, 10, 4)
	sec.Index(docs)

	filter := make(map[string][]interface{})
	result := sec.Search("sleek", 0, filter)

	if len(result) != 5 {
		t.Errorf("Expected search result is 5, got %d", len(result))
	}
	if result[0].ID != "1" || result[0].ScoreWeight != 1066014 {
		t.Errorf("Expected 1. document ID is 1, score is 1066014, got ID: %v, Score: %d", result[0].ID, result[0].ScoreWeight)
	}
	if result[1].ID != "6" || result[1].ScoreWeight != 1050014 {
		t.Errorf("Expected 2. document ID is 6, score is 1050014, got ID: %v, Score: %d", result[1].ID, result[1].ScoreWeight)
	}
	if result[2].ID != "3" || result[2].ScoreWeight != 1025000 {
		t.Errorf("Expected 3. document ID is 3, score is 1025000, got ID: %v, Score: %d", result[2].ID, result[2].ScoreWeight)
	}
	if result[3].ID != "2" || result[3].ScoreWeight != 1000014 {
		t.Errorf("Expected 4. document ID is 2, score is 1000014, got ID: %v, Score: %d", result[3].ID, result[3].ScoreWeight)
	}
	if result[4].ID != "7" || result[4].ScoreWeight != 1000007 {
		t.Errorf("Expected 5. document ID is 7, score is 1000007, got ID: %v, Score: %d", result[4].ID, result[4].ScoreWeight)
	}
}

func normalizeScoreIndex(si map[string][]Document) map[string][]Document {
	out := make(map[string][]Document, len(si))
	for term, docs := range si {
		docsCopy := append([]Document(nil), docs...)

		sort.Slice(docsCopy, func(i, j int) bool {
			return docsCopy[i].ID < docsCopy[j].ID
		})

		out[term] = docsCopy
	}
	return out
}
