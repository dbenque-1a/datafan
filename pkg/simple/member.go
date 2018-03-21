package simple

import (
	"log"
	"time"

	"github.com/dbenque/datafan/pkg/engine"
	"github.com/dbenque/datafan/pkg/store"
)

var _ engine.LocalMember = &Member{}

type Member struct {
	id        engine.ID
	store     store.Store
	connector engine.Connector
}

func NewMember(id string, store store.Store) *Member {
	m := &Member{id: engine.ID(id), store: store}
	m.connector = engine.NewConnector(m, NewConnector)
	return m
}

func (m *Member) ID() engine.ID {
	return m.id
}

func (m *Member) GetStore() store.Store {
	return m.store
}

func (m *Member) GetConnector() engine.Connector {
	return m.connector
}
func (m *Member) GetIndexes() engine.IndexMap {

	im := engine.IndexMap{
		Source:  m.ID(),
		Indexes: map[engine.ID]engine.Index{},
	}
	for _, id := range m.store.GetMembers() {
		im.Indexes[id] = m.store.GetIndex(id)
	}
	return im
}
func (m *Member) GetData(kps engine.KeyIDPairs) engine.Items {
	items := engine.Items{}
	for _, kp := range kps {
		i := m.store.Get(kp)
		if i == nil {
			continue
		}
		items = append(items, i)
	}
	return items
}
func (m *Member) Delete(kps engine.KeyIDPairs) {
	m.store.MultiDelete(kps)
}
func (m *Member) Put(items engine.Items) {
	m.store.MultiSet(items)
}

func (m *Member) Write(item *Item) {
	item.Time = time.Now()

	if item.Owner == "" {
		item.Owner = m.id
	}

	if item.Owner != m.id {
		log.Fatalf("The write interface is reserve to write owned items only")
	}

	m.store.Set(item)
}

func (m *Member) Remove(key engine.Key) {
	m.store.Delete(engine.KeyIDPair{Key: key, ID: m.id})
}
