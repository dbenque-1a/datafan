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
	go m.connector.run(make(chan struct{})) // TODO move the run somewhere else to be able to close correctly
	return m
}

func (m *Member) ID() engine.ID {
	return m.id
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
		items = append(items, m.store.Get(kp))
	}
	return items
}
func (m *Member) Add(items engine.Items) {
	keys := ""
	for _, i := range items {
		m.store.Set(i)
		keys += string(i.OwnedBy()) + "/" + string(i.GetKey()) + "  "
	}
	log.Printf("[%s] Add: %s", string(m.id), keys)
}
func (m *Member) Delete(kps engine.KeyIDPairs) {
	keys := ""
	for _, kp := range kps {
		m.store.Delete(kp)
		keys += string(kp.ID) + "/" + string(kp.Key) + "  "
	}
	log.Printf("[%s] Delete: %s", string(m.id), keys)
}
func (m *Member) Update(items engine.Items) {
	keys := ""
	for _, i := range items {
		m.store.Set(i)
		keys += string(i.OwnedBy()) + "/" + string(i.GetKey()) + "  "
	}
	log.Printf("[%s] Update: %s", string(m.id), keys)
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
