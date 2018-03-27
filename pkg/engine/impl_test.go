package engine

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dbenque/datafan/pkg/api"
	"github.com/dbenque/datafan/pkg/store"
)

//============================ Item implementation for test =========================

type testItem struct {
	Key   api.Key
	Time  time.Time
	Value string
	Owner api.ID
}

var _ api.Item = &testItem{}

func newTestItem(key api.Key, value string) *testItem {
	return &testItem{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}
}

func (i *testItem) GetKey() api.Key {
	return i.Key
}
func (i *testItem) StampedKey() api.StampedKey {
	return api.StampedKey{
		Key:       i.Key,
		Timestamp: i.Time,
	}
}
func (i *testItem) OwnedBy() api.ID {
	return i.Owner
}
func (i *testItem) DeepCopy() api.Item {
	j := *i
	return &j
}

//============================ Member  implementation for test =========================

type testMember struct {
	id        api.ID
	storage   store.Interface
	connector api.Connector
}

var _ api.LocalMember = &testMember{}

func newTestMember(id string, storage store.Interface) *testMember {
	m := &testMember{id: api.ID(id), storage: storage}
	m.connector = NewConnector(m, newTestConnector)
	return m
}

func (m *testMember) ID() api.ID {
	return m.id
}

func (m *testMember) GetStore() store.Interface {
	return m.storage
}

func (m *testMember) GetConnector() api.Connector {
	return m.connector
}
func (m *testMember) GetIndexes() api.IndexMap {
	im := api.IndexMap{
		Source:  m.ID(),
		Indexes: map[api.ID]api.Index{},
	}
	for _, id := range m.storage.GetMembers() {
		im.Indexes[id] = m.storage.GetIndex(id)
	}
	return im
}
func (m *testMember) GetData(kps api.KeyIDPairs) api.Items {
	items := api.Items{}
	for _, kp := range kps {
		i := m.storage.Get(kp)
		if i == nil {
			continue
		}
		items = append(items, i)
	}
	return items
}
func (m *testMember) Delete(kps api.KeyIDPairs) {
	m.storage.MultiDelete(kps)
}
func (m *testMember) Put(items api.Items) {
	m.storage.MultiSet(items)
}

func (m *testMember) Write(item *testItem) {
	item.Time = time.Now()

	if item.Owner == "" {
		item.Owner = m.id
	}

	if item.Owner != m.id {
		log.Fatalf("The write interface is reserve to write owned items only")
	}

	m.storage.Set(item)
}

func (m *testMember) Remove(key api.Key) {
	m.storage.Delete(api.KeyIDPair{Key: key, ID: m.id})
}

//============================ Connector implementation for test =========================

type testConnector struct {
	localMember    *testMember
	remoteHandling sync.RWMutex
	remoteMember   map[api.ID]*testMember
	connectorChan  api.ConnectorChan
}

var _ api.ConnectorCore = &testConnector{}

func newTestConnector(localMember api.LocalMember, connectorChan api.ConnectorChan) api.ConnectorCore {
	return &testConnector{
		localMember:   localMember.(*testMember),
		remoteMember:  map[api.ID]*testMember{},
		connectorChan: connectorChan,
	}
}

func (c *testConnector) GetLocalMember() api.LocalMember {
	return c.localMember
}

func (c *testConnector) Connect(m api.Member) error {
	return c.connect(m, true)
}

func (c *testConnector) connect(m api.Member, twoWays bool) error {
	c.remoteHandling.Lock()
	defer c.remoteHandling.Unlock()
	mm, ok := m.(*testMember)
	if !ok {
		log.Fatalf("Can't connect member of different types.")
		return fmt.Errorf("Can't connect member of different types.")
	}

	c.remoteMember[mm.ID()] = mm

	if twoWays {
		mm.connector.(*ConnectorImpl).ConnectorCore.(*testConnector).connect(c.localMember, false)
	}
	return nil
}

func (c *testConnector) FanOutIndexMap(index api.IndexMap) {
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	for _, m := range c.remoteMember {
		m.connector.(*ConnectorImpl).ReceiveIndexCh <- index
	}
}

func (c *testConnector) ProcessDataRequest(rq api.DataRequest) {
	items := c.localMember.GetData(rq.KeyIDPairs)
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	m := c.remoteMember[rq.RequestSource]
	if m != nil {
		m.connector.(*ConnectorImpl).ReceiveDataCh <- api.DataResponse{Items: items, AssociatedBuildTime: rq.AssociatedBuildTime}
	} else {
		log.Fatalf("Lost member ProcessDataRequest")
	}
}

func (c *testConnector) ForwardDataRequest(rq api.DataRequest) {
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	m := c.remoteMember[rq.RequestDestination]
	if m != nil {
		m.connector.(*ConnectorImpl).RequestKeysCh <- rq
	} else {
		log.Fatalf("Lost member ForwardDataRequest")
	}
}
