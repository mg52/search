package symspell

import (
	"sync"
)

// SymSpell implements the Symmetric-Delete spelling correction algorithm
// for edit-distance-1 fuzzy search. It precomputes all single-character
// deletions of each dictionary word and stores them in a map for O(m) lookup.

type SymSpell struct {
	DeleteMap map[string]map[string]struct{}
	mu        sync.RWMutex
}

// NewSymSpell creates a SymSpell instance.
func NewSymSpell() *SymSpell {
	return &SymSpell{
		DeleteMap: make(map[string]map[string]struct{}),
	}
}

// AddWord indexes a new word by generating all its single-character deletes.
func (s *SymSpell) AddWord(word string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.DeleteMap[word]; !exists {
		s.DeleteMap[word] = make(map[string]struct{})
	}
	s.DeleteMap[word][word] = struct{}{}
	for i := 0; i < len(word); i++ {
		del := word[:i] + word[i+1:]
		if _, exists := s.DeleteMap[del]; !exists {
			s.DeleteMap[del] = make(map[string]struct{})
		}
		s.DeleteMap[del][word] = struct{}{}
	}
}

// LoadDictionary adds all words in the slice to the SymSpell index.
func (s *SymSpell) LoadDictionary(words []string) {
	for _, w := range words {
		s.AddWord(w)
	}
}

// DeleteWord removes a word from the SymSpell index, including its
// entry for exact match and all its single-character deletions.
func (s *SymSpell) DeleteWord(word string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Remove the word itself
	if originals, exists := s.DeleteMap[word]; exists {
		delete(originals, word)
		if len(originals) == 0 {
			delete(s.DeleteMap, word)
		}
	}
	// Remove word from all deletion variants
	for i := 0; i < len(word); i++ {
		d := word[:i] + word[i+1:]
		if originals, exists := s.DeleteMap[d]; exists {
			delete(originals, word)
			if len(originals) == 0 {
				delete(s.DeleteMap, d)
			}
		}
	}
}

func (s *SymSpell) FuzzySearch(query string, maxReturnCount int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]string, 0, maxReturnCount)
	seen := make(map[string]struct{}, maxReturnCount)

	add := func(key string) bool {
		if originals, ok := s.DeleteMap[key]; ok {
			for w := range originals {
				if w == query {
					continue
				}
				if _, dup := seen[w]; dup {
					continue
				}
				seen[w] = struct{}{}
				results = append(results, w)
				if len(results) >= maxReturnCount {
					return true
				}
			}
		}
		return false
	}

	if add(query) {
		return results
	}

	for i := 0; i < len(query); i++ {
		del := query[:i] + query[i+1:]
		if add(del) {
			break
		}
	}

	return results
}

// FuzzySearch returns all dictionary words within Levenshtein distance ≤1 of query.
func (s *SymSpell) FuzzySearchOld(query string, maxReturnCount int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	candidates := make(map[string]struct{})

	if originals, existsInDeleteMap := s.DeleteMap[query]; existsInDeleteMap {
		for w := range originals {
			candidates[w] = struct{}{}
		}
	}

	for i := 0; i < len(query); i++ {
		if len(candidates) >= maxReturnCount {
			break
		}
		del := query[:i] + query[i+1:]
		if originals, existsInDeleteMap := s.DeleteMap[del]; existsInDeleteMap {
			for w := range originals {
				candidates[w] = struct{}{}
			}
		}
	}

	delete(candidates, query)

	var results []string

	counter := 0
	for w := range candidates {
		counter++
		results = append(results, w)
		if counter >= maxReturnCount {
			break
		}
	}
	return results
}
