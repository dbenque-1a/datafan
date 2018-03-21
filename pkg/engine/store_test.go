package engine

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

type Store interface {
	GetMembers() []ID
	GetIndex(id ID) Index
	Delete(KeyIDPair)
	Set(Item)
	MultiSet(Items)
	MultiDelete(KeyIDPairs)
	Get(KeyIDPair) Item
}

type MapStore struct {
	sync.RWMutex
	internal      map[ID]map[Key]Item
	panicOnDelete bool // for test purposes
}

var _ Store = &MapStore{}

func NewMapStore() *MapStore {
	return &MapStore{
		internal: map[ID]map[Key]Item{},
	}
}
func (m *MapStore) PanicOnDelete() {
	m.panicOnDelete = true
}

func (m *MapStore) GetMembers() []ID {
	m.RLock()
	defer m.RUnlock()
	members := []ID{}
	for id := range m.internal {
		members = append(members, id)
	}
	return members
}
func (m *MapStore) GetIndex(id ID) Index {
	m.RLock()
	defer m.RUnlock()

	index := Index{}
	if s, ok := m.internal[id]; ok {
		index.StampedKeys = []StampedKey{}
		for _, i := range s {
			index.StampedKeys = append(index.StampedKeys, i.StampedKey())
		}
	}
	return index
}
func (m *MapStore) MultiDelete(kps KeyIDPairs) {
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
func (m *MapStore) Delete(kp KeyIDPair) {
	m.Lock()
	defer m.Unlock()
	if m.panicOnDelete {
		panic(fmt.Sprintf("we said no delete: %v", kp))
	}
	if m, ok := m.internal[kp.ID]; ok {
		delete(m, kp.Key)
	}
}
func (m *MapStore) MultiSet(ilist Items) {
	m.Lock()
	defer m.Unlock()
	for _, i := range ilist {

		id := i.OwnedBy()
		s, ok := m.internal[id]
		if !ok {
			s = map[Key]Item{}
			m.internal[id] = s
		}
		s[i.GetKey()] = i.DeepCopy()
	}
}

func (m *MapStore) Set(i Item) {
	m.Lock()
	defer m.Unlock()
	id := i.OwnedBy()
	s, ok := m.internal[id]
	if !ok {
		s = map[Key]Item{}
		m.internal[id] = s
	}
	s[i.GetKey()] = i.DeepCopy()
}
func (m *MapStore) Get(kp KeyIDPair) Item {
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
func (m *MapStore) UntilCheck(wg *sync.WaitGroup, kp KeyIDPair, check func(i Item) bool, checkPeriod time.Duration) {
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
