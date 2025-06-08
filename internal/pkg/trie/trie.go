package trie

import (
	"encoding/gob"
	"fmt"
	"os"
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
