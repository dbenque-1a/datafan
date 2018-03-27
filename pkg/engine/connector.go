package engine

import (
	"github.com/dbenque/datafan/pkg/api"
)

type ConnectorImpl struct {
	api.ConnectorCore
	ReceiveIndexCh chan api.IndexMap
	sendIndexCh    chan api.IndexMap
	RequestKeysCh  chan api.DataRequest
	ReceiveDataCh  chan api.DataResponse
}

var _ api.Connector = &ConnectorImpl{}

type ConnectorCoreFactory func(localMember api.LocalMember, connectorChan api.ConnectorChan) api.ConnectorCore

func NewConnector(localMember api.LocalMember, coreFactory ConnectorCoreFactory) *ConnectorImpl {
	impl := &ConnectorImpl{
		ReceiveIndexCh: make(chan api.IndexMap, 50),
		sendIndexCh:    make(chan api.IndexMap, 50),

		ReceiveDataCh: make(chan api.DataResponse, 50),
		RequestKeysCh: make(chan api.DataRequest, 50),
	}
	impl.ConnectorCore = coreFactory(localMember, impl)
	return impl
}

func (c *ConnectorImpl) Core() api.ConnectorCore {
	return c.ConnectorCore
}

func (c *ConnectorImpl) ReceiveIndexMap(in *api.IndexMap) {
	c.ReceiveIndexCh <- *in
}

func (c *ConnectorImpl) ReceiveIndexChan() <-chan api.IndexMap {
	return c.ReceiveIndexCh
}
func (c *ConnectorImpl) SendIndexChan() chan<- api.IndexMap {
	return c.sendIndexCh
}

func (c *ConnectorImpl) RequestKeysChan() chan<- api.DataRequest {
	return c.RequestKeysCh
}
func (c *ConnectorImpl) ReceiveData(in *api.DataResponse) {
	c.ReceiveDataCh <- *in
}
func (c *ConnectorImpl) ReceiveDataChan() <-chan api.DataResponse {
	return c.ReceiveDataCh
}

func (c *ConnectorImpl) Run(stop <-chan struct{}) {
	for {
		select {
		case indexFromChan := <-c.sendIndexCh: // fan out to remote
			go c.FanOutIndexMap(indexFromChan)
		case rqFromChan := <-c.RequestKeysCh:
			if rqFromChan.RequestDestination == c.GetLocalMember().ID() {
				// handle the request (only for non RPC context. In RPC context the remote server take care of that)
				go c.ProcessDataRequest(rqFromChan)
			} else {
				// forward the query to the good member
				go c.ForwardDataRequest(rqFromChan)
			}
		case <-stop:
			return
		}
	}
}
