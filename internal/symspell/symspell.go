package symspell

// SymSpell implements the Symmetric-Delete spelling correction algorithm
// for edit-distance-1 fuzzy search. It precomputes all single-character
// deletions of each dictionary word and stores them in a map for O(m) lookup.
//
// Thread safety: SymSpell has no internal lock. Callers must ensure that
// AddWord/DeleteWord are not called concurrently with FuzzySearch. In the
// engine this is guaranteed by se.mu (writes under Lock, reads under RLock).

type SymSpell struct {
	DeleteMap map[string][]string
}

func NewSymSpell() *SymSpell {
	return &SymSpell{
		DeleteMap: make(map[string][]string),
	}
}

// AddWord indexes a new word by generating all its single-character deletes.
func (s *SymSpell) AddWord(word string) {
	s.appendUnique(word, word)
	if len(word) < 2 {
		return
	}
	buf := make([]byte, len(word)-1)
	for i := 0; i < len(word); i++ {
		copy(buf, word[:i])
		copy(buf[i:], word[i+1:])
		s.appendUnique(string(buf), word)
	}
}

// appendUnique adds word to DeleteMap[key] only if not already present.
func (s *SymSpell) appendUnique(key, word string) {
	for _, w := range s.DeleteMap[key] {
		if w == word {
			return
		}
	}
	s.DeleteMap[key] = append(s.DeleteMap[key], word)
}

// LoadDictionary adds all words in the slice to the SymSpell index.
func (s *SymSpell) LoadDictionary(words []string) {
	for _, w := range words {
		s.AddWord(w)
	}
}

// DeleteWord removes a word from the SymSpell index.
func (s *SymSpell) DeleteWord(word string) {
	s.removeFrom(word, word)
	if len(word) < 2 {
		return
	}
	buf := make([]byte, len(word)-1)
	for i := 0; i < len(word); i++ {
		copy(buf, word[:i])
		copy(buf[i:], word[i+1:])
		s.removeFrom(string(buf), word)
	}
}

// removeFrom removes word from DeleteMap[key], deleting the key if empty.
func (s *SymSpell) removeFrom(key, word string) {
	words := s.DeleteMap[key]
	for i, w := range words {
		if w == word {
			last := len(words) - 1
			words[i] = words[last]
			s.DeleteMap[key] = words[:last]
			if last == 0 {
				delete(s.DeleteMap, key)
			}
			return
		}
	}
}

// FuzzySearch returns up to maxReturnCount dictionary words within
// Levenshtein distance ≤ 1 of query. Caller must hold se.mu.RLock().
func (s *SymSpell) FuzzySearch(query string, maxReturnCount int) []string {
	results := make([]string, 0, maxReturnCount)

	add := func(key string) bool {
		for _, w := range s.DeleteMap[key] {
			if w == query {
				continue
			}
			dup := false
			for _, r := range results {
				if r == w {
					dup = true
					break
				}
			}
			if !dup {
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

	if len(query) < 2 {
		return results
	}

	buf := make([]byte, len(query)-1)
	for i := 0; i < len(query); i++ {
		copy(buf, query[:i])
		copy(buf[i:], query[i+1:])
		if add(string(buf)) {
			break
		}
	}

	return results
}
