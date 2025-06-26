package trie

import (
	"encoding/gob"
	"fmt"
	"os"
	"sort"
	"sync"
)

type TrieNode struct {
	Children    map[rune]*TrieNode
	ChildrenArr []rune
	IsEnd       bool
}

type Trie struct {
	Root *TrieNode
	mu   sync.RWMutex
}

func init() {
	// so gob knows about our types:
	gob.Register(&Trie{})
	gob.Register(&TrieNode{})
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
	// only the exported fields (Root, Children, ChildrenArr, IsEnd) will be encoded
	return enc.Encode(t)
}

func LoadTrie(path string) (*Trie, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		// no file yet
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
	// make sure the mutex is zero-value ready
	return &t, nil
}

func NewTrie() *Trie {
	return &Trie{Root: &TrieNode{Children: make(map[rune]*TrieNode)}}
}

func (t *Trie) Insert(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	node := t.Root
	for _, ch := range key {
		if _, exists := node.Children[ch]; !exists {
			node.Children[ch] = &TrieNode{Children: make(map[rune]*TrieNode)}
			node.ChildrenArr = append(node.ChildrenArr, ch)
		}
		node = node.Children[ch]
	}
	node.IsEnd = true
}

func (t *Trie) SearchPrefix(prefix string, prefixCount int) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	node := t.Root
	for _, ch := range prefix {
		if _, exists := node.Children[ch]; !exists {
			return nil
		}
		node = node.Children[ch]
	}

	var results []string
	t.collectWords(node, prefix, &results, prefixCount)
	return results
}

func (t *Trie) collectWords(root *TrieNode, prefix string, results *[]string, prefixCount int) {
	type entry struct {
		node   *TrieNode
		prefix string
	}

	// start BFS from the given node/prefix
	queue := []entry{{root, prefix}}
	for len(queue) > 0 && len(*results) < prefixCount {
		curr := queue[0]
		queue = queue[1:]

		// if this node marks a complete word, collect it
		if curr.node.IsEnd {
			*results = append(*results, curr.prefix)
			if len(*results) >= prefixCount {
				break
			}
		}

		// enqueue children in order
		for _, ch := range curr.node.ChildrenArr {
			child := curr.node.Children[ch]
			queue = append(queue, entry{
				node:   child,
				prefix: curr.prefix + string(ch),
			})
		}
	}
}

// Remove deletes `key` from the trie.
// Returns an error if `key` was not found.
func (t *Trie) Remove(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	runes := []rune(key)
	node := t.Root
	for _, ch := range runes {
		child, ok := node.Children[ch]
		if !ok {
			return fmt.Errorf("key %q not found in trie", key)
		}
		node = child
	}
	if !node.IsEnd {
		return fmt.Errorf("key %q not found in trie", key)
	}

	t.removeNode(t.Root, runes, 0)
	return nil
}

// removeNode walks down to depth, unmarks or deletes, and
// returns true if the caller should delete its reference
func (t *Trie) removeNode(node *TrieNode, runes []rune, depth int) bool {
	if depth == len(runes) {
		node.IsEnd = false
	} else {
		ch := runes[depth]
		child := node.Children[ch]
		if shouldDelete := t.removeNode(child, runes, depth+1); shouldDelete {
			delete(node.Children, ch)
			for i, c := range node.ChildrenArr {
				if c == ch {
					node.ChildrenArr = append(node.ChildrenArr[:i], node.ChildrenArr[i+1:]...)
					break
				}
			}
		}
	}
	return !node.IsEnd && len(node.Children) == 0
}

// Update renames a key: it removes oldKey (erroring if missing) and inserts newKey.
func (t *Trie) Update(oldKey, newKey string) error {
	if err := t.Remove(oldKey); err != nil {
		return err
	}
	t.Insert(newKey)
	return nil
}

// FuzzySearch returns up to maxResults words whose Levenshtein
// distance to query is ≤ maxDist (here 1), prioritizing exact matches,
// then deletion errors, then substitutions, then insertions.
func (t *Trie) FuzzySearch(query string, maxDist, maxResults int) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// 1) Gather all candidates with their edit‐distance <= maxDist
	cols := len(query) + 1
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
		// prune whole branch if even the best possible dist > maxDist
		if min(prevRow) > maxDist {
			return
		}
		if node.IsEnd {
			d := prevRow[cols-1]
			if d <= maxDist {
				candidates = append(candidates, candidate{prefix, d})
			}
		}
		for _, ch := range node.ChildrenArr {
			child := node.Children[ch]
			currRow := make([]int, cols)
			currRow[0] = prevRow[0] + 1
			for j := 1; j < cols; j++ {
				insertCost := currRow[j-1] + 1
				deleteCost := prevRow[j] + 1
				replaceCost := prevRow[j-1]
				if rune(query[j-1]) != ch {
					replaceCost++
				}
				currRow[j] = insertCost
				if deleteCost < currRow[j] {
					currRow[j] = deleteCost
				}
				if replaceCost < currRow[j] {
					currRow[j] = replaceCost
				}
			}
			dfs(child, prefix+string(ch), currRow)
		}
	}
	dfs(t.Root, "", prevRow)

	// no candidates → empty non‐nil slice
	if len(candidates) == 0 {
		return []string{}
	}

	// 2) If any exact hits, return only those
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

	// 3) Otherwise bucket by length‐difference
	var dels, subs, adds []string
	for _, c := range candidates {
		switch {
		case len(c.word) < len(query):
			dels = append(dels, c.word)
		case len(c.word) == len(query):
			subs = append(subs, c.word)
		default:
			adds = append(adds, c.word)
		}
	}

	// 4) Pick the first non‐empty bucket in (dels → subs → adds)
	var out []string
	switch {
	case len(dels) > 0:
		out = dels
	case len(subs) > 0:
		out = subs
	default:
		out = adds
	}

	// 5) Sort + truncate
	sort.Strings(out)
	if len(out) > maxResults {
		out = out[:maxResults]
	}
	return out
}

// helper to get the min element of a slice
func min(s []int) int {
	m := s[0]
	for _, v := range s[1:] {
		if v < m {
			m = v
		}
	}
	return m
}
