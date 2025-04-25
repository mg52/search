package search

import (
	"fmt"
	"sync"
)

type TrieNode struct {
	children    map[rune]*TrieNode
	childrenArr []rune
	isEnd       bool
}

type Trie struct {
	root *TrieNode
	mu   sync.RWMutex
}

func NewTrie() *Trie {
	return &Trie{root: &TrieNode{children: make(map[rune]*TrieNode)}}
}

func (t *Trie) Insert(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	node := t.root
	for _, ch := range key {
		if _, exists := node.children[ch]; !exists {
			node.children[ch] = &TrieNode{children: make(map[rune]*TrieNode)}
			node.childrenArr = append(node.childrenArr, ch)
		}
		node = node.children[ch]
	}
	node.isEnd = true
}

func (t *Trie) SearchPrefix(prefix string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	node := t.root
	for _, ch := range prefix {
		if _, exists := node.children[ch]; !exists {
			return nil
		}
		node = node.children[ch]
	}

	var results []string
	t.collectWords(node, prefix, &results)
	return results
}

func (t *Trie) collectWords(node *TrieNode, prefix string, results *[]string) {
	if node.isEnd {
		*results = append(*results, prefix)
	}
	for _, ch := range node.childrenArr {
		if len(*results) >= 5 {
			break
		}
		t.collectWords(node.children[ch], prefix+string(ch), results)
	}
}

// Remove deletes `key` from the trie.
// Returns an error if `key` was not found.
func (t *Trie) Remove(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	runes := []rune(key)
	node := t.root
	for _, ch := range runes {
		child, ok := node.children[ch]
		if !ok {
			return fmt.Errorf("key %q not found in trie", key)
		}
		node = child
	}
	if !node.isEnd {
		return fmt.Errorf("key %q not found in trie", key)
	}

	t.removeNode(t.root, runes, 0)
	return nil
}

// removeNode walks down to depth, unmarks or deletes, and
// returns true if the caller should delete its reference
func (t *Trie) removeNode(node *TrieNode, runes []rune, depth int) bool {
	if depth == len(runes) {
		node.isEnd = false
	} else {
		ch := runes[depth]
		child := node.children[ch]
		if shouldDelete := t.removeNode(child, runes, depth+1); shouldDelete {
			delete(node.children, ch)
			for i, c := range node.childrenArr {
				if c == ch {
					node.childrenArr = append(node.childrenArr[:i], node.childrenArr[i+1:]...)
					break
				}
			}
		}
	}
	return !node.isEnd && len(node.children) == 0
}

// Update renames a key: it removes oldKey (erroring if missing) and inserts newKey.
func (t *Trie) Update(oldKey, newKey string) error {
	if err := t.Remove(oldKey); err != nil {
		return err
	}
	t.Insert(newKey)
	return nil
}
