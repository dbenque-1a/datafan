package engine

import (
	"time"
)

//Key identifier for the object
type Key string
type Keys []Key

//ID identifier for the member
type ID string

type KeyIDPair struct {
	ID
	Key
}
type KeyIDPairs []KeyIDPair

type DataRequest struct {
	RequestSource      ID
	RequestDestination ID
	KeyIDPairs
}

type StampedKey struct {
	Key
	Timestamp time.Time
}

type Index struct {
	BuildTime   time.Time
	StampedKeys []StampedKey
}

type IndexMap struct {
	Source  ID
	Indexes map[ID]Index
}

type Member interface {
	ID() ID
	GetIndexes() IndexMap
	GetData(KeyIDPairs) Items
}

type ConnectorCore interface {
	GetLocalMember() LocalMember
	Connect(Member)
	ForwardDataRequest(rq DataRequest)
	ProcessDataRequest(rq DataRequest)
	ProcessIndexMap(index IndexMap)
}
type ConnectorChan interface {
	ReceiveIndexChan() <-chan IndexMap
	SendIndexChan() chan<- IndexMap
	RequestKeysChan() chan<- DataRequest
	ReceiveDataChan() <-chan Items
}
type Connector interface {
	ConnectorChan
	ConnectorCore
	Run(stop <-chan struct{})
}

type LocalMember interface {
	Member
	Delete(KeyIDPairs)
	Put(Items)
	GetConnector() Connector
}

type Items []Item

type Item interface {
	GetKey() Key
	StampedKey() StampedKey
	OwnedBy() ID
	DeepCopy() Item
}
