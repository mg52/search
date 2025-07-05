package symspell

import (
	"reflect"
	"sort"
	"testing"
)

func TestSymSpell_FuzzySearch(t *testing.T) {
	// Prepare dictionary
	words := []string{"black", "block", "back", "blacks", "slack", "flack"}
	ss := NewSymSpell()
	ss.LoadDictionary(words)

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "Substitution",
			query: "blacj",
			want:  []string{"black"},
		},
		{
			name:  "Deletion",
			query: "blac",
			want:  []string{"black", "back"},
		},
		{
			name:  "Insertion",
			query: "blackso",
			want:  []string{"blacks"},
		},
		{
			name:  "MultipleMatches",
			query: "lack",
			want:  []string{"black", "flack", "slack", "back"},
		},
		{
			name:  "NoMatch",
			query: "xyz",
			want:  []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ss.FuzzySearch(tc.query, 5)
			sort.Strings(got)
			sort.Strings(tc.want)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("FuzzySearch(%q) = %v; want %v", tc.query, got, tc.want)
			}
		})
	}
}

func TestAddWord_Idempotent(t *testing.T) {
	ss := NewSymSpell()
	ss.AddWord("test")
	ss.AddWord("test")
	got := ss.FuzzySearch("tes", 5)
	if len(got) != 1 || got[0] != "test" {
		t.Errorf("Duplicate AddWord should not create multiple entries, got %v", got)
	}
}

func TestEmptyDictionary(t *testing.T) {
	ss := NewSymSpell()
	got := ss.FuzzySearch("anything", 5)
	if len(got) != 0 {
		t.Errorf("Empty dictionary: FuzzySearch should return empty slice, got %v", got)
	}
}

func TestDeleteWord_RemovesWordAndVariants(t *testing.T) {
	words := []string{"black", "block", "back", "blacks", "slack", "flack"}
	ss := NewSymSpell()
	ss.LoadDictionary(words)

	got := ss.FuzzySearch("lack", 5)
	want := []string{"black", "flack", "slack", "back"}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FuzzySearch(\"lack\") = %v; want %v", got, want)
	}

	ss.DeleteWord("slack")

	got = ss.FuzzySearch("lack", 5)
	want = []string{"black", "flack", "back"}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FuzzySearch(\"lack\") = %v; want %v", got, want)
	}
}
