package engine

import (
	"log"
	"sync"
	"time"
)

//============================ Item implementation for test =========================

type testItem struct {
	Key   Key
	Time  time.Time
	Value string
	Owner ID
}

var _ Item = &testItem{}

func newTestItem(key Key, value string) *testItem {
	return &testItem{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}
}

func (i *testItem) GetKey() Key {
	return i.Key
}
func (i *testItem) StampedKey() StampedKey {
	return StampedKey{
		Key:       i.Key,
		Timestamp: i.Time,
	}
}
func (i *testItem) OwnedBy() ID {
	return i.Owner
}
func (i *testItem) DeepCopy() Item {
	j := *i
	return &j
}

//============================ Member  implementation for test =========================

type testMember struct {
	id        ID
	store     Store
	connector Connector
}

var _ LocalMember = &testMember{}

func newTestMember(id string, store Store) *testMember {
	m := &testMember{id: ID(id), store: store}
	m.connector = NewConnector(m, newTestConnector)
	return m
}

func (m *testMember) ID() ID {
	return m.id
}

func (m *testMember) GetStore() Store {
	return m.store
}

func (m *testMember) GetConnector() Connector {
	return m.connector
}
func (m *testMember) GetIndexes() IndexMap {

	im := IndexMap{
		Source:  m.ID(),
		Indexes: map[ID]Index{},
	}
	for _, id := range m.store.GetMembers() {
		im.Indexes[id] = m.store.GetIndex(id)
	}
	return im
}
func (m *testMember) GetData(kps KeyIDPairs) Items {
	items := Items{}
	for _, kp := range kps {
		i := m.store.Get(kp)
		if i == nil {
			continue
		}
		items = append(items, i)
	}
	return items
}
func (m *testMember) Delete(kps KeyIDPairs) {
	m.store.MultiDelete(kps)
}
func (m *testMember) Put(items Items) {
	m.store.MultiSet(items)
}

func (m *testMember) Write(item *testItem) {
	item.Time = time.Now()

	if item.Owner == "" {
		item.Owner = m.id
	}

	if item.Owner != m.id {
		log.Fatalf("The write interface is reserve to write owned items only")
	}

	m.store.Set(item)
}

func (m *testMember) Remove(key Key) {
	m.store.Delete(KeyIDPair{Key: key, ID: m.id})
}

//============================ Connector implementation for test =========================

type testConnector struct {
	localMember    *testMember
	remoteHandling sync.RWMutex
	remoteMember   map[ID]*testMember
	connectorChan  ConnectorChan
}

var _ ConnectorCore = &testConnector{}

func newTestConnector(localMember LocalMember, connectorChan ConnectorChan) ConnectorCore {
	return &testConnector{
		localMember:   localMember.(*testMember),
		remoteMember:  map[ID]*testMember{},
		connectorChan: connectorChan,
	}
}

func (c *testConnector) GetLocalMember() LocalMember {
	return c.localMember
}

func (c *testConnector) Connect(m Member) {
	c.connect(m, true)
}

func (c *testConnector) connect(m Member, twoWays bool) {
	c.remoteHandling.Lock()
	defer c.remoteHandling.Unlock()
	mm, ok := m.(*testMember)
	if !ok {
		log.Fatalf("Can't connect member of different types.")
	}

	c.remoteMember[mm.ID()] = mm

	if twoWays {
		mm.connector.(*ConnectorImpl).ConnectorCore.(*testConnector).connect(c.localMember, false)
	}
}

func (c *testConnector) ProcessIndexMap(index IndexMap) {
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	for _, m := range c.remoteMember {
		m.connector.(*ConnectorImpl).ReceiveIndexCh <- index
	}
}

func (c *testConnector) ProcessDataRequest(rq DataRequest) {
	items := c.localMember.GetData(rq.KeyIDPairs)
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	m := c.remoteMember[rq.RequestSource]
	if m != nil {
		m.connector.(*ConnectorImpl).ReceiveDataCh <- DataResponse{Items: items, AssociatedBuildTime: rq.AssociatedBuildTime}
	} else {
		log.Fatalf("Lost member ProcessDataRequest")
	}
}

func (c *testConnector) ForwardDataRequest(rq DataRequest) {
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	m := c.remoteMember[rq.RequestDestination]
	if m != nil {
		m.connector.(*ConnectorImpl).RequestKeysCh <- rq
	} else {
		log.Fatalf("Lost member ForwardDataRequest")
	}
}
