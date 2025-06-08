package trie

import (
	_ "embed"
	"path/filepath"
	"reflect"
	"testing"
)

func TestInsertAndSearch(t *testing.T) {
	tr := NewTrie()
	// insert some keys
	keys := []string{"app", "apple", "banana", "apple"}
	for _, k := range keys {
		tr.Insert(k)
	}

	t.Run("SearchPrefix 'app'", func(t *testing.T) {
		got := tr.SearchPrefix("app", 5)
		exp := []string{"app", "apple"}
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
	// insert some keys
	keys := []string{"app", "apple", "appl", "appf", "appc", "appfe", "appce", "appde", "appced"}
	for _, k := range keys {
		tr.Insert(k)
	}

	t.Run("SearchPrefix 'app'", func(t *testing.T) {
		got := tr.SearchPrefix("app", 5)
		exp := []string{"app", "appl", "appf", "appc", "apple"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("SearchPrefix(\"app\") = %v; want %v", got, exp)
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
		got := tr.SearchPrefix("app", 5)
		exp := []string{"app", "appol"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("After Remove apple, SearchPrefix(\"app\") = %v; want %v", got, exp)
		}
	})

	t.Run("Remove existing leaf 'appol'", func(t *testing.T) {
		if err := tr.Remove("appol"); err != nil {
			t.Fatalf("Remove(\"apple\") error: %v", err)
		}
		got := tr.SearchPrefix("app", 5)
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

// TestSaveLoadTrie_RoundTrip ensures that a Trie can be saved and loaded, preserving its contents.
func TestSaveLoadTrie_RoundTrip(t *testing.T) {
	// Prepare temporary file
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "trie.gob")

	// Build a trie and insert some keys
	original := NewTrie()
	keys := []string{"apple", "apricot", "banana"}
	for _, k := range keys {
		original.Insert(k)
	}

	// Save to disk
	if err := original.Save(filePath); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Load from disk
	loaded, err := LoadTrie(filePath)
	if err != nil {
		t.Fatalf("LoadTrie failed: %v", err)
	}

	// Verify that SearchPrefix("ap") returns the inserted "apple" and "apricot"
	got := loaded.SearchPrefix("ap", 5)
	want := []string{"apple", "apricot"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SearchPrefix(\"ap\") = %v; want %v", got, want)
	}
}

// TestLoadTrie_Nonexistent checks that loading a missing file returns an empty trie without error.
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

	// An empty trie should return nil for any prefix
	if res := tr.SearchPrefix("anything", 5); res != nil {
		t.Errorf("expected nil on SearchPrefix on empty trie, got: %v", res)
	}
}

// TestSaveTrie_Error ensures that attempting to save to a directory path returns an error.
func TestSaveTrie_Error(t *testing.T) {
	tmpDir := t.TempDir()
	tr := NewTrie()
	// Saving to the directory itself should fail
	if err := tr.Save(tmpDir); err == nil {
		t.Fatal("expected error when saving Trie to a directory path, got nil")
	}
}

func TestTrie_Update_NewKeyInserted(t *testing.T) {
	tr := NewTrie()
	// Insert two keys
	tr.Insert("apple")
	tr.Insert("banana")

	// Update 'apple' to 'apricot'
	if err := tr.Update("apple", "apricot"); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Collect all keys starting with 'apr'
	rst := tr.SearchPrefix("apr", 5)
	if !reflect.DeepEqual(rst, []string{"apricot"}) {
		t.Errorf("SearchPrefix(apr) = %v; want [apricot]", rst)
	}

	// Ensure 'banana' still present
	if rst2 := tr.SearchPrefix("ban", 5); !reflect.DeepEqual(rst2, []string{"banana"}) {
		t.Errorf("SearchPrefix(ban) = %v; want [banana]", rst2)
	}
}
