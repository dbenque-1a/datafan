package simple

import (
	"log"
	"sync"

	"github.com/dbenque/datafan/pkg/engine"
)

type Connector struct {
	remoteHandling sync.RWMutex
	localMember    *Member
	remoteMember   map[engine.ID]*Member

	receiveIndexChan chan engine.IndexMap
	sendIndexChan    chan engine.IndexMap
	requestKeysChan  chan engine.DataRequest
	receiveDataChan  chan engine.Items
}

var _ engine.Connector = &Connector{}

func NewConnector(localMember *Member) *Connector {
	return &Connector{
		localMember:  localMember,
		remoteMember: map[engine.ID]*Member{},

		receiveIndexChan: make(chan engine.IndexMap, 50),
		sendIndexChan:    make(chan engine.IndexMap, 50),

		receiveDataChan: make(chan engine.Items, 50),
		requestKeysChan: make(chan engine.DataRequest, 50),
	}
}

func (c *Connector) ReceiveIndexChan() <-chan engine.IndexMap {
	return c.receiveIndexChan
}
func (c *Connector) SendIndexChan() chan<- engine.IndexMap {
	return c.sendIndexChan
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

	// if _, ok := c.remoteMember[mm.ID()]; ok {
	// 	log.Printf("Member %s is already connected to local %s", mm.ID(), c.localMember.ID())
	// }

	c.remoteMember[mm.ID()] = mm

	if twoWays {
		mm.connector.connect(c.localMember, false)
	}
}

func (c *Connector) RequestKeysChan() chan<- engine.DataRequest {
	return c.requestKeysChan
}
func (c *Connector) ReceiveDataChan() <-chan engine.Items {
	return c.receiveDataChan
}

func (c *Connector) Run(stop <-chan struct{}) {

	for {
		select {
		case indexFromChan := <-c.sendIndexChan: // fan out to remote
			go func(index engine.IndexMap) {
				c.remoteHandling.RLock()
				defer c.remoteHandling.RUnlock()
				for _, m := range c.remoteMember {
					m.connector.receiveIndexChan <- index
				}
			}(indexFromChan)
		case rqFromChan := <-c.requestKeysChan:
			if rqFromChan.RequestDestination == c.localMember.ID() {
				// handle the request
				go func(rq engine.DataRequest) {
					items := c.localMember.GetData(rq.KeyIDPairs)
					c.remoteHandling.RLock()
					defer c.remoteHandling.RUnlock()
					m := c.remoteMember[rq.RequestSource]
					if m != nil {
						m.connector.receiveDataChan <- items
					}
				}(rqFromChan)
			} else {
				// forward the query to the good member
				go func(rq engine.DataRequest) {
					c.remoteHandling.RLock()
					defer c.remoteHandling.RUnlock()
					m := c.remoteMember[rq.RequestDestination]
					if m != nil {
						m.connector.requestKeysChan <- rqFromChan
					}
				}(rqFromChan)
			}

		case <-stop:
			return
		}
	}

}
