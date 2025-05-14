package search

import (
	_ "embed"
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
