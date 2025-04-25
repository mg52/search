package search

import "sync"

type Keys struct {
	data map[string]struct{}
	mu   sync.RWMutex
}

func NewKeys() *Keys {
	return &Keys{data: make(map[string]struct{})}
}

func (k *Keys) Insert(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.data[key] = struct{}{}
}

func (k *Keys) Remove(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.data, key)
}

func (k *Keys) GetData() map[string]struct{} {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.data
}
