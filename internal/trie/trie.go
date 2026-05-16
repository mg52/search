package trie

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
)

type TrieNode struct {
	Children    map[rune]*TrieNode
	ChildrenArr []rune
	IsEnd       bool
	DocFreq     int
}

type Trie struct {
	Root *TrieNode
	mu   sync.RWMutex
}

func init() {
	gob.Register(&Trie{})
	gob.Register(&TrieNode{})
}

func NewTrie() *Trie {
	return &Trie{Root: &TrieNode{Children: make(map[rune]*TrieNode)}}
}

func (t *Trie) Save(path string) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	return enc.Encode(t)
}

func LoadTrie(path string) (*Trie, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return NewTrie(), nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	var t Trie
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&t); err != nil {
		return nil, err
	}
	return &t, nil
}

// insertLocked inserts key and increments DocFreq. Caller must hold the write lock.
func (t *Trie) insertLocked(key string) {
	node := t.Root
	for _, ch := range key {
		if _, exists := node.Children[ch]; !exists {
			node.Children[ch] = &TrieNode{Children: make(map[rune]*TrieNode)}
			node.ChildrenArr = append(node.ChildrenArr, ch)
		}
		node = node.Children[ch]
	}
	node.IsEnd = true
	node.DocFreq++
}

// Insert adds key to the trie and increments its DocFreq.
// Call once per document that contains the term to get accurate document frequency.
func (t *Trie) Insert(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.insertLocked(key)
}

// SearchPrefix returns up to limit terms starting with prefix, sorted by
// DocFreq descending. Terms with equal DocFreq are sorted alphabetically
// for deterministic results. Returns nil if no matching terms exist.
func (t *Trie) SearchPrefix(prefix string, limit int) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	node := t.Root
	for _, ch := range prefix {
		child, exists := node.Children[ch]
		if !exists {
			return nil
		}
		node = child
	}

	type candidate struct {
		word    string
		docFreq int
	}
	var candidates []candidate
	buf := []rune(prefix)

	var dfs func(n *TrieNode)
	dfs = func(n *TrieNode) {
		if n.IsEnd {
			candidates = append(candidates, candidate{string(buf), n.DocFreq})
		}
		for _, ch := range n.ChildrenArr {
			buf = append(buf, ch)
			dfs(n.Children[ch])
			buf = buf[:len(buf)-1]
		}
	}
	dfs(node)

	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].docFreq != candidates[j].docFreq {
			return candidates[i].docFreq > candidates[j].docFreq
		}
		return candidates[i].word < candidates[j].word
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	results := make([]string, len(candidates))
	for i, c := range candidates {
		results[i] = c.word
	}
	return results
}

var errNotFound = errors.New("not found")

// removeNode removes key from the trie in a single recursive O(len(key)) pass.
// Returns shouldDelete=true when the caller should remove its reference to this node.
func removeNode(node *TrieNode, runes []rune, depth int) (bool, error) {
	if depth == len(runes) {
		if !node.IsEnd {
			return false, errNotFound
		}
		node.IsEnd = false
		node.DocFreq = 0
		return len(node.Children) == 0, nil
	}
	ch := runes[depth]
	child, ok := node.Children[ch]
	if !ok {
		return false, errNotFound
	}
	shouldDelete, err := removeNode(child, runes, depth+1)
	if err != nil {
		return false, err
	}
	if shouldDelete {
		delete(node.Children, ch)
		for i, c := range node.ChildrenArr {
			if c == ch {
				node.ChildrenArr = append(node.ChildrenArr[:i], node.ChildrenArr[i+1:]...)
				break
			}
		}
	}
	return !node.IsEnd && len(node.Children) == 0, nil
}

// Remove deletes key from the trie in a single O(len(key)) pass.
// Returns an error if the key is not found.
func (t *Trie) Remove(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, err := removeNode(t.Root, []rune(key), 0)
	if err != nil {
		return fmt.Errorf("key %q not found in trie", key)
	}
	return nil
}

// Update atomically renames oldKey to newKey under a single lock.
// Returns an error if oldKey is not found.
func (t *Trie) Update(oldKey, newKey string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, err := removeNode(t.Root, []rune(oldKey), 0)
	if err != nil {
		return fmt.Errorf("key %q not found in trie", oldKey)
	}
	t.insertLocked(newKey)
	return nil
}

// FuzzySearch returns up to maxResults words whose Levenshtein distance to query
// is ≤ maxDist, prioritizing exact matches, then deletions, substitutions, insertions.
func (t *Trie) FuzzySearch(query string, maxDist, maxResults int) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	queryRunes := []rune(query)
	cols := len(queryRunes) + 1
	prevRow := make([]int, cols)
	for i := range prevRow {
		prevRow[i] = i
	}

	type candidate struct {
		word string
		dist int
	}
	var candidates []candidate

	var dfs func(node *TrieNode, prefix string, prevRow []int)
	dfs = func(node *TrieNode, prefix string, prevRow []int) {
		if minInt(prevRow) > maxDist {
			return
		}
		if node.IsEnd {
			if d := prevRow[cols-1]; d <= maxDist {
				candidates = append(candidates, candidate{prefix, d})
			}
		}
		for _, ch := range node.ChildrenArr {
			child := node.Children[ch]
			currRow := make([]int, cols)
			currRow[0] = prevRow[0] + 1
			for j := 1; j < cols; j++ {
				ins := currRow[j-1] + 1
				del := prevRow[j] + 1
				rep := prevRow[j-1]
				if queryRunes[j-1] != ch {
					rep++
				}
				curr := ins
				if del < curr {
					curr = del
				}
				if rep < curr {
					curr = rep
				}
				currRow[j] = curr
			}
			dfs(child, prefix+string(ch), currRow)
		}
	}
	dfs(t.Root, "", prevRow)

	if len(candidates) == 0 {
		return []string{}
	}

	var exact []string
	for _, c := range candidates {
		if c.dist == 0 {
			exact = append(exact, c.word)
		}
	}
	if len(exact) > 0 {
		sort.Strings(exact)
		if len(exact) > maxResults {
			exact = exact[:maxResults]
		}
		return exact
	}

	qLen := len(queryRunes)
	var dels, subs, adds []string
	for _, c := range candidates {
		wLen := len([]rune(c.word))
		switch {
		case wLen < qLen:
			dels = append(dels, c.word)
		case wLen == qLen:
			subs = append(subs, c.word)
		default:
			adds = append(adds, c.word)
		}
	}

	var out []string
	switch {
	case len(dels) > 0:
		out = dels
	case len(subs) > 0:
		out = subs
	default:
		out = adds
	}

	sort.Strings(out)
	if len(out) > maxResults {
		out = out[:maxResults]
	}
	return out
}

func minInt(s []int) int {
	m := s[0]
	for _, v := range s[1:] {
		if v < m {
			m = v
		}
	}
	return m
}
