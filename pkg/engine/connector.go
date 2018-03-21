package engine

type ConnectorImpl struct {
	ConnectorCore
	ReceiveIndexCh chan IndexMap
	sendIndexCh    chan IndexMap
	RequestKeysCh  chan DataRequest
	ReceiveDataCh  chan Items
}

var _ Connector = &ConnectorImpl{}

type ConnectorCoreFactory func(localMember LocalMember, connectorChan ConnectorChan) ConnectorCore

func NewConnector(localMember LocalMember, coreFactory ConnectorCoreFactory) *ConnectorImpl {
	impl := &ConnectorImpl{
		ReceiveIndexCh: make(chan IndexMap, 50),
		sendIndexCh:    make(chan IndexMap, 50),

		ReceiveDataCh: make(chan Items, 50),
		RequestKeysCh: make(chan DataRequest, 50),
	}
	impl.ConnectorCore = coreFactory(localMember, impl)
	return impl
}

func (c *ConnectorImpl) ReceiveIndexChan() <-chan IndexMap {
	return c.ReceiveIndexCh
}
func (c *ConnectorImpl) SendIndexChan() chan<- IndexMap {
	return c.sendIndexCh
}

func (c *ConnectorImpl) RequestKeysChan() chan<- DataRequest {
	return c.RequestKeysCh
}
func (c *ConnectorImpl) ReceiveDataChan() <-chan Items {
	return c.ReceiveDataCh
}

func (c *ConnectorImpl) Run(stop <-chan struct{}) {
	for {
		select {
		case indexFromChan := <-c.sendIndexCh: // fan out to remote
			go c.ProcessIndexMap(indexFromChan)
		case rqFromChan := <-c.RequestKeysCh:
			if rqFromChan.RequestDestination == c.GetLocalMember().ID() {
				// handle the request
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
