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
	connector *Connector
}

func NewMember(id string, store store.Store) *Member {
	m := &Member{id: engine.ID(id), store: store}
	m.connector = NewConnector(m)
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
	keys := ""
	for _, kp := range kps {
		m.store.Delete(kp)
		keys += string(kp.ID) + "/" + string(kp.Key) + "  "
	}
}
func (m *Member) Put(items engine.Items) {
	keys := ""
	for _, i := range items {
		m.store.Set(i)
		keys += string(i.OwnedBy()) + "/" + string(i.GetKey()) + "  "
	}
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
