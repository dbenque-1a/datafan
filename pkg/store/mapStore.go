package store

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dbenque/datafan/pkg/engine"
)

type MapStore struct {
	sync.RWMutex
	internal      map[engine.ID]map[engine.Key]engine.Item
	panicOnDelete bool // for test purposes
}

var _ Store = &MapStore{}

func NewMapStore() *MapStore {
	return &MapStore{
		internal: map[engine.ID]map[engine.Key]engine.Item{},
	}
}
func (m *MapStore) PanicOnDelete() {
	m.panicOnDelete = true
}

func (m *MapStore) GetMembers() []engine.ID {
	m.RLock()
	defer m.RUnlock()
	members := []engine.ID{}
	for id := range m.internal {
		members = append(members, id)
	}
	return members
}
func (m *MapStore) GetIndex(id engine.ID) engine.Index {
	m.RLock()
	defer m.RUnlock()

	index := engine.Index{}
	if s, ok := m.internal[id]; ok {
		index.StampedKeys = []engine.StampedKey{}
		for _, i := range s {
			index.StampedKeys = append(index.StampedKeys, i.StampedKey())
		}
	}
	return index
}
func (m *MapStore) MultiDelete(kps engine.KeyIDPairs) {
	m.Lock()
	defer m.Unlock()
	if m.panicOnDelete {
		panic(fmt.Sprintf("we said no delete even multi"))
	}

	for _, kp := range kps {
		if m, ok := m.internal[kp.ID]; ok {
			delete(m, kp.Key)
		}
	}
}
func (m *MapStore) Delete(kp engine.KeyIDPair) {
	m.Lock()
	defer m.Unlock()
	if m.panicOnDelete {
		panic(fmt.Sprintf("we said no delete: %v", kp))
	}
	if m, ok := m.internal[kp.ID]; ok {
		delete(m, kp.Key)
	}
}
func (m *MapStore) MultiSet(ilist engine.Items) {
	m.Lock()
	defer m.Unlock()
	for _, i := range ilist {

		id := i.OwnedBy()
		s, ok := m.internal[id]
		if !ok {
			s = map[engine.Key]engine.Item{}
			m.internal[id] = s
		}
		s[i.GetKey()] = i.DeepCopy()
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
	m.RLock()
	defer m.RUnlock()
	if s, ok := m.internal[kp.ID]; ok {
		if v, ok := s[kp.Key]; ok {
			return v.DeepCopy()
		}
	}
	return nil
}

func (m *MapStore) Dump() string {
	m.RLock()
	defer m.RUnlock()
	json, err := json.MarshalIndent(m.internal, "", "\t")
	if err != nil {
		log.Fatal(err)
	}
	return string(json)
}

func (m *MapStore) Count() (count int) {
	m.RLock()
	defer m.RUnlock()
	for _, v := range m.internal {
		count += len(v)
	}
	return count
}

//UntilCount for test purposes only
func (m *MapStore) UntilCount(wg *sync.WaitGroup, count int, checkPeriod time.Duration) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(checkPeriod)
		defer t.Stop()
		for range t.C {
			if m.Count() == count {
				return
			}
		}
	}()
}

//UntilCount for test purposes only
func (m *MapStore) UntilCheck(wg *sync.WaitGroup, kp engine.KeyIDPair, check func(i engine.Item) bool, checkPeriod time.Duration) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(checkPeriod)
		defer t.Stop()
		for range t.C {
			if check(m.Get(kp)) {
				return
			}
		}
	}()
}
