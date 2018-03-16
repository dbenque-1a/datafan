package store

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/dbenque/datafan/pkg/engine"
)

type MapStore struct {
	sync.Mutex
	internal map[engine.ID]map[engine.Key]engine.Item
}

var _ Store = &MapStore{}

func NewMapStore() *MapStore {
	return &MapStore{
		internal: map[engine.ID]map[engine.Key]engine.Item{},
	}
}

func (m *MapStore) GetMembers() []engine.ID {
	m.Lock()
	defer m.Unlock()
	members := []engine.ID{}
	for id := range m.internal {
		members = append(members, id)
	}
	return members
}
func (m *MapStore) GetIndex(id engine.ID) engine.Index {
	m.Lock()
	defer m.Unlock()

	index := engine.Index{}
	if s, ok := m.internal[id]; ok {
		index.StampedKeys = []engine.StampedKey{}
		for _, i := range s {
			index.StampedKeys = append(index.StampedKeys, i.StampedKey())
		}
	}
	return index
}
func (m *MapStore) Delete(kp engine.KeyIDPair) {
	m.Lock()
	defer m.Unlock()

	if m, ok := m.internal[kp.ID]; ok {
		delete(m, kp.Key)
	}
}
func (m *MapStore) Set(i engine.Item) {
	m.Lock()
	defer m.Unlock()
	id := i.OwnedBy()
	s, ok := m.internal[id]
	if !ok {
		s = map[engine.Key]engine.Item{}
		m.internal[id] = s
	}
	s[i.GetKey()] = i.DeepCopy()
}
func (m *MapStore) Get(kp engine.KeyIDPair) engine.Item {
	m.Lock()
	defer m.Unlock()
	if s, ok := m.internal[kp.ID]; ok {
		if v, ok := s[kp.Key]; ok {
			return v.DeepCopy()
		}
	}
	return nil
}

func (m *MapStore) Dump() string {
	json, err := json.MarshalIndent(m.internal, "", "\t")
	if err != nil {
		log.Fatal(err)
	}
	return string(json)
}
