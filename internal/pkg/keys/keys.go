package keys

import (
	"encoding/gob"
	"os"
	"sync"
)

type Keys struct {
	Data map[string]struct{}
	mu   sync.RWMutex
}

func NewKeys() *Keys {
	return &Keys{Data: make(map[string]struct{})}
}

func (k *Keys) Insert(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.Data[key] = struct{}{}
}

func (k *Keys) Remove(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.Data, key)
}

func (k *Keys) GetData() map[string]struct{} {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.Data
}

// Save writes the current set of keys to a file at `path` using gob.
func (k *Keys) Save(path string) error {
	k.mu.RLock()
	defer k.mu.RUnlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	// We only encode the map itself.
	return enc.Encode(k.Data)
}

// LoadKeys reads a gob-encoded map from `path` and returns a *Keys.
// If the file doesn't exist, it returns an empty Keys.
func LoadKeys(path string) (*Keys, error) {
	k := NewKeys()

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		// No existing file → start empty
		return k, nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	dec := gob.NewDecoder(f)
	var data map[string]struct{}
	if err := dec.Decode(&data); err != nil {
		return nil, err
	}

	k.mu.Lock()
	k.Data = data
	k.mu.Unlock()
	return k, nil
}
