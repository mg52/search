package trie

import (
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestInsertAndSearch(t *testing.T) {
	tr := NewTrie()
	// "apple" inserted twice → DocFreq=2; "app" once → DocFreq=1
	keys := []string{"app", "apple", "banana", "apple"}
	for _, k := range keys {
		tr.Insert(k)
	}

	t.Run("SearchPrefix 'app'", func(t *testing.T) {
		got := tr.SearchPrefix("app", 5)
		// apple(DocFreq=2) sorts before app(DocFreq=1)
		exp := []string{"apple", "app"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("SearchPrefix(\"app\") = %v; want %v", got, exp)
		}
	})

	t.Run("SearchPrefix 'ban'", func(t *testing.T) {
		got := tr.SearchPrefix("ban", 5)
		exp := []string{"banana"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("SearchPrefix(\"ban\") = %v; want %v", got, exp)
		}
	})

	t.Run("Search non-existent prefix", func(t *testing.T) {
		if got := tr.SearchPrefix("xyz", 5); got != nil {
			t.Errorf("SearchPrefix(\"xyz\") = %v; want nil", got)
		}
	})
}

func TestInsertAndSearch_LargeKeys(t *testing.T) {
	tr := NewTrie()
	keys := []string{"app", "apple", "appl", "appf", "appc", "appfe", "appce", "appde", "appced"}
	for _, k := range keys {
		tr.Insert(k)
	}

	t.Run("SearchPrefix 'app'", func(t *testing.T) {
		got := tr.SearchPrefix("app", 5)
		// All DocFreq=1 → alphabetical tiebreaker gives the 5 lexicographically smallest
		exp := []string{"app", "appc", "appce", "appced", "appde"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("SearchPrefix(\"app\") = %v; want %v", got, exp)
		}
	})
}

func TestDocFreq_SortsByFrequency(t *testing.T) {
	tr := NewTrie()
	// "cream" in 3 docs, "creed" in 2, "creme" in 1
	for i := 0; i < 3; i++ {
		tr.Insert("cream")
	}
	for i := 0; i < 2; i++ {
		tr.Insert("creed")
	}
	tr.Insert("creme")

	got := tr.SearchPrefix("cre", 5)
	want := []string{"cream", "creed", "creme"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SearchPrefix docFreq ordering: got %v, want %v", got, want)
	}
}

func TestRemove(t *testing.T) {
	tr := NewTrie()
	keys := []string{"app", "apple", "appol"}
	for _, k := range keys {
		tr.Insert(k)
	}

	t.Run("Remove existing leaf 'apple'", func(t *testing.T) {
		if err := tr.Remove("apple"); err != nil {
			t.Fatalf("Remove(\"apple\") error: %v", err)
		}
		got := tr.SearchPrefix("app", 5)
		exp := []string{"app", "appol"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("After Remove apple, SearchPrefix(\"app\") = %v; want %v", got, exp)
		}
	})

	t.Run("Remove existing leaf 'appol'", func(t *testing.T) {
		if err := tr.Remove("appol"); err != nil {
			t.Fatalf("Remove(\"appol\") error: %v", err)
		}
		got := tr.SearchPrefix("app", 5)
		exp := []string{"app"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("After Remove appol, SearchPrefix(\"app\") = %v; want %v", got, exp)
		}
	})

	t.Run("Remove existing prefix 'app'", func(t *testing.T) {
		if err := tr.Remove("app"); err != nil {
			t.Fatalf("Remove(\"app\") error: %v", err)
		}
		if got := tr.SearchPrefix("app", 5); len(got) != 0 {
			t.Errorf("After Remove app, SearchPrefix(\"app\") = %v; want empty", got)
		}
	})

	t.Run("Remove non-existent key", func(t *testing.T) {
		if err := tr.Remove("nonexistent"); err == nil {
			t.Errorf("Remove(\"nonexistent\") = nil; want error")
		}
	})
}

func TestSaveLoadTrie_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "trie.gob")

	original := NewTrie()
	keys := []string{"apple", "apricot", "banana"}
	for _, k := range keys {
		original.Insert(k)
	}

	if err := original.Save(filePath); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := LoadTrie(filePath)
	if err != nil {
		t.Fatalf("LoadTrie failed: %v", err)
	}

	// All DocFreq=1 → alphabetical order: apple < apricot
	got := loaded.SearchPrefix("ap", 5)
	want := []string{"apple", "apricot"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SearchPrefix(\"ap\") = %v; want %v", got, want)
	}
}

func TestLoadTrie_Nonexistent(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "no-file.gob")

	tr, err := LoadTrie(filePath)
	if err != nil {
		t.Fatalf("expected no error for missing file, got: %v", err)
	}
	if tr == nil {
		t.Fatal("expected non-nil Trie on missing file")
	}

	if res := tr.SearchPrefix("anything", 5); res != nil {
		t.Errorf("expected nil on SearchPrefix on empty trie, got: %v", res)
	}
}

func TestSaveTrie_Error(t *testing.T) {
	tmpDir := t.TempDir()
	tr := NewTrie()
	if err := tr.Save(tmpDir); err == nil {
		t.Fatal("expected error when saving Trie to a directory path, got nil")
	}
}

func TestTrie_Update_NewKeyInserted(t *testing.T) {
	tr := NewTrie()
	tr.Insert("apple")
	tr.Insert("banana")

	if err := tr.Update("apple", "apricot"); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	rst := tr.SearchPrefix("apr", 5)
	if !reflect.DeepEqual(rst, []string{"apricot"}) {
		t.Errorf("SearchPrefix(apr) = %v; want [apricot]", rst)
	}

	if rst2 := tr.SearchPrefix("ban", 5); !reflect.DeepEqual(rst2, []string{"banana"}) {
		t.Errorf("SearchPrefix(ban) = %v; want [banana]", rst2)
	}
}

// ---- SearchPrefix edge cases ----

func TestSearchPrefix_EmptyTrie(t *testing.T) {
	tr := NewTrie()
	if got := tr.SearchPrefix("a", 5); got != nil {
		t.Errorf("empty trie SearchPrefix = %v; want nil", got)
	}
}

func TestSearchPrefix_EmptyPrefix(t *testing.T) {
	tr := NewTrie()
	tr.Insert("cat")
	tr.Insert("car")
	tr.Insert("dog")
	got := tr.SearchPrefix("", 10)
	if len(got) != 3 {
		t.Errorf("empty prefix should return all terms, got %v", got)
	}
}

func TestSearchPrefix_LimitOne(t *testing.T) {
	tr := NewTrie()
	for i := 0; i < 3; i++ {
		tr.Insert("zoo")
	}
	tr.Insert("zap")
	tr.Insert("zip")
	got := tr.SearchPrefix("z", 1)
	if len(got) != 1 {
		t.Fatalf("limit=1 should return exactly 1 result, got %v", got)
	}
	// "zoo" has DocFreq=3, highest — must be first
	if got[0] != "zoo" {
		t.Errorf("expected zoo (highest DocFreq), got %v", got[0])
	}
}

func TestSearchPrefix_LimitLargerThanResults(t *testing.T) {
	tr := NewTrie()
	tr.Insert("hi")
	tr.Insert("hey")
	got := tr.SearchPrefix("h", 100)
	if len(got) != 2 {
		t.Errorf("expected 2 results, got %v", got)
	}
}

func TestSearchPrefix_ExactWordIsPrefix(t *testing.T) {
	tr := NewTrie()
	tr.Insert("test")
	tr.Insert("testing")
	got := tr.SearchPrefix("test", 5)
	// both "test" and "testing" share the prefix "test"
	if len(got) != 2 {
		t.Errorf("expected 2 results for prefix=test, got %v", got)
	}
}

func TestSearchPrefix_SingleChar(t *testing.T) {
	tr := NewTrie()
	tr.Insert("a")
	got := tr.SearchPrefix("a", 5)
	if !reflect.DeepEqual(got, []string{"a"}) {
		t.Errorf("single char insert/search: got %v, want [a]", got)
	}
}

// ---- Remove edge cases ----

func TestRemove_SharedPrefix(t *testing.T) {
	tr := NewTrie()
	tr.Insert("apple")
	tr.Insert("application")
	tr.Insert("apply")

	if err := tr.Remove("apple"); err != nil {
		t.Fatalf("Remove apple: %v", err)
	}
	got := tr.SearchPrefix("app", 10)
	for _, w := range got {
		if w == "apple" {
			t.Errorf("apple should be gone after Remove, got %v", got)
		}
	}
	if len(got) != 2 {
		t.Errorf("expected application and apply to remain, got %v", got)
	}
}

func TestRemove_LastKey_EmptyTrie(t *testing.T) {
	tr := NewTrie()
	tr.Insert("only")
	if err := tr.Remove("only"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if got := tr.SearchPrefix("", 10); len(got) != 0 {
		t.Errorf("trie should be empty after removing last key, got %v", got)
	}
}

func TestRemove_ChildrenArrConsistency(t *testing.T) {
	tr := NewTrie()
	tr.Insert("ab")
	tr.Insert("ac")
	tr.Insert("ad")

	if err := tr.Remove("ac"); err != nil {
		t.Fatalf("Remove ac: %v", err)
	}
	got := tr.SearchPrefix("a", 10)
	if len(got) != 2 {
		t.Errorf("expected [ab ad], got %v", got)
	}
	for _, w := range got {
		if w == "ac" {
			t.Errorf("ac still present after Remove: %v", got)
		}
	}
}

func TestRemove_EmptyString(t *testing.T) {
	tr := NewTrie()
	tr.Insert("")
	if err := tr.Remove(""); err != nil {
		t.Fatalf("Remove empty string: %v", err)
	}
	if got := tr.SearchPrefix("", 5); len(got) != 0 {
		t.Errorf("expected empty trie, got %v", got)
	}
}

func TestRemove_NotFound_AfterRemoval(t *testing.T) {
	tr := NewTrie()
	tr.Insert("hello")
	_ = tr.Remove("hello")
	if err := tr.Remove("hello"); err == nil {
		t.Error("second Remove of same key should return error")
	}
}

// ---- Update edge cases ----

func TestUpdate_NonExistentKey(t *testing.T) {
	tr := NewTrie()
	tr.Insert("apple")
	if err := tr.Update("missing", "other"); err == nil {
		t.Error("Update of non-existent key should return error")
	}
	// original key must still be intact
	got := tr.SearchPrefix("app", 5)
	if !reflect.DeepEqual(got, []string{"apple"}) {
		t.Errorf("apple should survive failed Update, got %v", got)
	}
}

func TestUpdate_SameKey(t *testing.T) {
	tr := NewTrie()
	for i := 0; i < 3; i++ {
		tr.Insert("same")
	}
	// Update same→same: removes (DocFreq→0, IsEnd=false) then re-inserts (DocFreq=1)
	if err := tr.Update("same", "same"); err != nil {
		t.Fatalf("Update same→same: %v", err)
	}
	got := tr.SearchPrefix("sam", 5)
	if !reflect.DeepEqual(got, []string{"same"}) {
		t.Errorf("expected [same] after self-update, got %v", got)
	}
}

func TestUpdate_PreservesOtherKeys(t *testing.T) {
	tr := NewTrie()
	tr.Insert("cat")
	tr.Insert("car")
	tr.Insert("dog")

	if err := tr.Update("cat", "cow"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got := tr.SearchPrefix("c", 10)
	has := func(s string) bool {
		for _, w := range got {
			if w == s {
				return true
			}
		}
		return false
	}
	if has("cat") {
		t.Error("cat should be gone after Update")
	}
	if !has("car") {
		t.Error("car should still be present")
	}
	if !has("cow") {
		t.Error("cow should be present after Update")
	}
	dogGot := tr.SearchPrefix("dog", 5)
	if !reflect.DeepEqual(dogGot, []string{"dog"}) {
		t.Errorf("dog unaffected by Update, got %v", dogGot)
	}
}

// ---- FuzzySearch edge cases ----

func TestFuzzySearch_ExactMatchOnly(t *testing.T) {
	tr := NewTrie()
	tr.Insert("hello")
	tr.Insert("helo")
	got := tr.FuzzySearch("hello", 0, 10)
	if !reflect.DeepEqual(got, []string{"hello"}) {
		t.Errorf("maxDist=0 should return only exact match, got %v", got)
	}
}

func TestFuzzySearch_EmptyTrie(t *testing.T) {
	tr := NewTrie()
	got := tr.FuzzySearch("anything", 1, 10)
	if len(got) != 0 {
		t.Errorf("empty trie FuzzySearch should return empty, got %v", got)
	}
}

func TestFuzzySearch_MaxResultsCap(t *testing.T) {
	tr := NewTrie()
	words := []string{"bat", "bad", "ban", "bar", "bay"}
	for _, w := range words {
		tr.Insert(w)
	}
	got := tr.FuzzySearch("bax", 1, 2)
	if len(got) > 2 {
		t.Errorf("maxResults=2 should cap results, got %v", got)
	}
}

func TestFuzzySearch_NoMatchBeyondMaxDist(t *testing.T) {
	tr := NewTrie()
	tr.Insert("abcdef")
	got := tr.FuzzySearch("xyz", 1, 10)
	if len(got) != 0 {
		t.Errorf("completely different word should not match at dist=1, got %v", got)
	}
}

func TestFuzzySearch_PrefersDeletion(t *testing.T) {
	// query is longer than candidates → "deletion" from query perspective (insertion into candidate)
	tr := NewTrie()
	tr.Insert("cat")  // query "cats" → delete 's' → "cat"  (len < query)
	tr.Insert("coat") // query "cats" → substitution         (len == query)
	got := tr.FuzzySearch("cats", 1, 10)
	if len(got) == 0 {
		t.Fatal("expected at least one match")
	}
	// "cat" is shorter than query → classified as deletion
	found := false
	for _, w := range got {
		if w == "cat" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected cat in results, got %v", got)
	}
}

// ---- Unicode ----

func TestInsert_Unicode(t *testing.T) {
	tr := NewTrie()
	tr.Insert("café")
	tr.Insert("cafeteria")
	got := tr.SearchPrefix("caf", 5)
	if len(got) != 2 {
		t.Errorf("unicode insert: expected 2 results, got %v", got)
	}
}

func TestFuzzySearch_Unicode(t *testing.T) {
	tr := NewTrie()
	tr.Insert("naïve")
	got := tr.FuzzySearch("naive", 1, 5)
	// one substitution (ï→i) should match
	if len(got) == 0 {
		t.Errorf("expected fuzzy match for unicode word, got %v", got)
	}
}

// ---- Concurrency ----

func TestConcurrentInsertAndSearch(t *testing.T) {
	tr := NewTrie()
	words := []string{"apple", "apricot", "banana", "blueberry", "cherry"}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr.Insert(words[i%len(words)])
		}(i)
	}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr.SearchPrefix("ap", 10)
		}()
	}
	wg.Wait()

	got := tr.SearchPrefix("ap", 10)
	if len(got) == 0 {
		t.Error("expected results after concurrent inserts")
	}
}

func TestConcurrentInsertAndRemove(t *testing.T) {
	tr := NewTrie()
	// pre-populate so removes have something to work on
	words := []string{"dog", "dot", "doe"}
	for _, w := range words {
		for i := 0; i < 10; i++ {
			tr.Insert(w)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr.Insert(words[i%len(words)])
		}(i)
	}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = tr.Remove(words[i%len(words)])
		}(i)
	}
	wg.Wait()
	// just verifying no panic / data race — correctness after mixed ops is non-deterministic
}

func TestConcurrentSearchPrefix(t *testing.T) {
	tr := NewTrie()
	for _, w := range []string{"run", "runner", "running", "ran", "race"} {
		tr.Insert(w)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := tr.SearchPrefix("r", 10)
			if len(got) == 0 {
				t.Errorf("concurrent SearchPrefix returned empty")
			}
		}()
	}
	wg.Wait()
}

// ---- DocFreq after Remove ----

func TestDocFreq_AfterRemoveAndReInsert(t *testing.T) {
	tr := NewTrie()
	for i := 0; i < 5; i++ {
		tr.Insert("word")
	}
	tr.Insert("world")

	// Remove sets DocFreq=0 / IsEnd=false entirely
	if err := tr.Remove("word"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	got := tr.SearchPrefix("wor", 5)
	if !reflect.DeepEqual(got, []string{"world"}) {
		t.Errorf("after Remove word, expected [world], got %v", got)
	}

	// Re-insert word: DocFreq restarts at 1, still sorts after world (DocFreq=1 tie → alpha)
	tr.Insert("word")
	got2 := tr.SearchPrefix("wor", 5)
	if len(got2) != 2 {
		t.Errorf("after re-insert, expected 2 results, got %v", got2)
	}
}

// ---- Save/Load round-trip with DocFreq ----

func TestSaveLoad_DocFreqPreserved(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "trie.gob")

	tr := NewTrie()
	for i := 0; i < 5; i++ {
		tr.Insert("popular")
	}
	tr.Insert("rare")

	if err := tr.Save(filePath); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := LoadTrie(filePath)
	if err != nil {
		t.Fatalf("LoadTrie: %v", err)
	}
	got := loaded.SearchPrefix("", 5)
	// "popular" (DocFreq=5) must come before "rare" (DocFreq=1)
	if len(got) < 2 || got[0] != "popular" {
		t.Errorf("DocFreq not preserved across Save/Load: got %v", got)
	}
}

func TestFuzzySearch(t *testing.T) {
	trie := NewTrie()
	words := []string{"black", "block", "back", "blacks", "slack", "flack"}
	for _, w := range words {
		trie.Insert(w)
	}

	tests := []struct {
		name       string
		query      string
		maxDist    int
		maxResults int
		want       []string
	}{
		{
			name:       "ExactMatch",
			query:      "black",
			maxDist:    1,
			maxResults: 10,
			want:       []string{"black"},
		},
		{
			name:       "Substitution",
			query:      "blacj",
			maxDist:    1,
			maxResults: 10,
			want:       []string{"black"},
		},
		{
			name:       "Insertion",
			query:      "blackk",
			maxDist:    1,
			maxResults: 10,
			want:       []string{"black"},
		},
		{
			name:       "Deletion",
			query:      "blac",
			maxDist:    1,
			maxResults: 10,
			want:       []string{"black"},
		},
		{
			name:       "NoMatch",
			query:      "xyz",
			maxDist:    1,
			maxResults: 10,
			want:       []string{},
		},
		{
			name:       "LimitResults",
			query:      "bck",
			maxDist:    1,
			maxResults: 10,
			want:       []string{"back"},
		},
		{
			name:       "MultiMatch",
			query:      "bleck",
			maxDist:    1,
			maxResults: 10,
			want:       []string{"black", "block"},
		},
		{
			name:       "FirstCharFail",
			query:      "vlock",
			maxDist:    1,
			maxResults: 10,
			want:       []string{"block"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := trie.FuzzySearch(tc.query, tc.maxDist, tc.maxResults)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("FuzzySearch(%q, %d, %d) = %v; want %v",
					tc.query, tc.maxDist, tc.maxResults, got, tc.want)
			}
		})
	}
}
