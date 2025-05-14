package search

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestKeys_InsertRemoveGetData(t *testing.T) {
	k := NewKeys()

	// Initially empty
	if data := k.GetData(); len(data) != 0 {
		t.Fatalf("expected empty data, got %v", data)
	}

	// Insert keys
	k.Insert("foo")
	k.Insert("bar")
	k.Insert("baz")

	data := k.GetData()
	if len(data) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(data))
	}
	for _, key := range []string{"foo", "bar", "baz"} {
		if _, ok := data[key]; !ok {
			t.Errorf("expected key %q present", key)
		}
	}

	// Remove a key
	k.Remove("bar")
	data = k.GetData()
	if len(data) != 2 {
		t.Fatalf("expected 2 keys after removal, got %d", len(data))
	}
	if _, ok := data["bar"]; ok {
		t.Errorf("expected key %q to be removed", "bar")
	}
}

func TestSaveLoadKeys_FileNotExist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "does-not-exist.gob")

	// LoadKeys on non-existent file should return empty Keys and no error
	k, err := LoadKeys(path)
	if err != nil {
		t.Fatalf("expected no error loading non-existent file, got %v", err)
	}
	if got := k.GetData(); len(got) != 0 {
		t.Errorf("expected empty data on load from non-existent file, got %v", got)
	}
}

func TestSaveLoadKeys_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.gob")

	// Prepare Keys and insert some entries
	orig := NewKeys()
	orig.Insert("alpha")
	orig.Insert("beta")
	orig.Insert("gamma")

	// Save to disk
	if err := orig.Save(path); err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	// Ensure file exists
	if info, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s, got error: %v", path, err)
	} else if info.Size() == 0 {
		t.Fatalf("expected non-empty file, got size 0")
	}

	// Load back
	loaded, err := LoadKeys(path)
	if err != nil {
		t.Fatalf("LoadKeys() error: %v", err)
	}

	// Compare contents
	want := orig.GetData()
	got := loaded.GetData()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("loaded data mismatch\ngot:  %v\nwant: %v", got, want)
	}
}
