package store

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dbenque/datafan/pkg/api"
)

type Interface interface {
	GetMembers() []api.ID
	GetIndex(id api.ID) api.Index
	Delete(api.KeyIDPair)
	Set(api.Item)
	MultiSet(api.Items)
	MultiDelete(api.KeyIDPairs)
	Get(api.KeyIDPair) api.Item
}

type MapStore struct {
	sync.RWMutex
	internal      map[api.ID]map[api.Key]api.Item
	panicOnDelete bool // for test purposes
}

var _ Interface = &MapStore{}

func NewMapStore() *MapStore {
	return &MapStore{
		internal: map[api.ID]map[api.Key]api.Item{},
	}
}
func (m *MapStore) PanicOnDelete() {
	m.panicOnDelete = true
}

func (m *MapStore) GetMembers() []api.ID {
	m.RLock()
	defer m.RUnlock()
	members := []api.ID{}
	for id := range m.internal {
		members = append(members, id)
	}
	return members
}
func (m *MapStore) GetIndex(id api.ID) api.Index {
	m.RLock()
	defer m.RUnlock()

	index := api.Index{}
	if s, ok := m.internal[id]; ok {
		index.StampedKeys = []api.StampedKey{}
		for _, i := range s {
			index.StampedKeys = append(index.StampedKeys, i.StampedKey())
		}
	}
	return index
}
func (m *MapStore) MultiDelete(kps api.KeyIDPairs) {
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
func (m *MapStore) Delete(kp api.KeyIDPair) {
	m.Lock()
	defer m.Unlock()
	if m.panicOnDelete {
		panic(fmt.Sprintf("we said no delete: %v", kp))
	}
	if m, ok := m.internal[kp.ID]; ok {
		delete(m, kp.Key)
	}
}
func (m *MapStore) MultiSet(ilist api.Items) {
	m.Lock()
	defer m.Unlock()
	for _, i := range ilist {

		id := i.OwnedBy()
		s, ok := m.internal[id]
		if !ok {
			s = map[api.Key]api.Item{}
			m.internal[id] = s
		}
		s[i.GetKey()] = i.DeepCopy()
	}
}

func (m *MapStore) Set(i api.Item) {
	m.Lock()
	defer m.Unlock()
	id := i.OwnedBy()
	s, ok := m.internal[id]
	if !ok {
		s = map[api.Key]api.Item{}
		m.internal[id] = s
	}
	s[i.GetKey()] = i.DeepCopy()
}
func (m *MapStore) Get(kp api.KeyIDPair) api.Item {
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
func (m *MapStore) UntilCount(wg *sync.WaitGroup, count int, checkPeriod time.Duration, timeout time.Duration) {
	m.RLock()
	defer m.RUnlock()
	wg.Add(1)

	tc := time.After(timeout)

	go func() {
		defer wg.Done()
		t := time.NewTicker(checkPeriod)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if m.Count() == count {
					return
				}
			case <-tc:
				return
			}
		}
	}()
}

//UntilCount for test purposes only
func (m *MapStore) UntilCheck(wg *sync.WaitGroup, kp api.KeyIDPair, check func(i api.Item) bool, checkPeriod time.Duration, timeout time.Duration) {
	m.RLock()
	defer m.RUnlock()
	wg.Add(1)

	tc := time.After(timeout)

	go func() {
		defer wg.Done()
		t := time.NewTicker(checkPeriod)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				if check(m.Get(kp)) {
					return
				}
			case <-tc:
				return
			}
		}
	}()
}
