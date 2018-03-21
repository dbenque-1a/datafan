package simple

import (
	"log"
	"sync"

	"github.com/dbenque/datafan/pkg/engine"
)

type Connector struct {
	localMember    *Member
	remoteHandling sync.RWMutex
	remoteMember   map[engine.ID]*Member
	connectorChan  engine.ConnectorChan
}

var _ engine.ConnectorCore = &Connector{}

func NewConnector(localMember engine.LocalMember, connectorChan engine.ConnectorChan) engine.ConnectorCore {
	return &Connector{
		localMember:   localMember.(*Member),
		remoteMember:  map[engine.ID]*Member{},
		connectorChan: connectorChan,
	}
}

func (c *Connector) GetLocalMember() engine.LocalMember {
	return c.localMember
}

func (c *Connector) Connect(m engine.Member) {
	c.connect(m, true)
}

func (c *Connector) connect(m engine.Member, twoWays bool) {
	c.remoteHandling.Lock()
	defer c.remoteHandling.Unlock()
	mm, ok := m.(*Member)
	if !ok {
		log.Fatalf("Can't connect member of different types.")
	}

	c.remoteMember[mm.ID()] = mm

	if twoWays {
		mm.connector.(*engine.ConnectorImpl).ConnectorCore.(*Connector).connect(c.localMember, false)
	}
}

func (c *Connector) ProcessIndexMap(index engine.IndexMap) {
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	for _, m := range c.remoteMember {
		m.connector.(*engine.ConnectorImpl).ReceiveIndexCh <- index
	}
}

func (c *Connector) ProcessDataRequest(rq engine.DataRequest) {
	items := c.localMember.GetData(rq.KeyIDPairs)
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	m := c.remoteMember[rq.RequestSource]
	if m != nil {
		m.connector.(*engine.ConnectorImpl).ReceiveDataCh <- items
	}
}

func (c *Connector) ForwardDataRequest(rq engine.DataRequest) {
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	m := c.remoteMember[rq.RequestDestination]
	if m != nil {
		m.connector.(*engine.ConnectorImpl).RequestKeysCh <- rq
	}
}
