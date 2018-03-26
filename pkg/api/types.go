package api

import (
	"strings"
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
	RequestSource       ID
	RequestDestination  ID
	AssociatedBuildTime map[ID]time.Time
	KeyIDPairs
}

type DataResponse struct {
	AssociatedBuildTime map[ID]time.Time
	Items               Items
}

type StampedKey struct {
	Key
	Timestamp time.Time
}

type StampedKeys []StampedKey

func (a StampedKeys) Len() int      { return len(a) }
func (a StampedKeys) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a StampedKeys) Less(i, j int) bool {
	return strings.Compare(string(a[i].Key), string(a[j].Key)) > 0
}

func (a StampedKeys) Equal(b StampedKeys) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key {
			return false
		}
		if a[i].Timestamp != b[i].Timestamp {
			return false
		}
	}
	return true
}

type Index struct {
	BuildTime   time.Time
	StampedKeys StampedKeys
}

type IndexMap struct {
	Source  ID
	Indexes map[ID]Index
}

type Member interface {
	ID() ID
}

type ConnectorCore interface {
	GetLocalMember() LocalMember
	Connect(Member) error
	ForwardDataRequest(rq DataRequest)
	ProcessDataRequest(rq DataRequest)
	ProcessIndexMap(index IndexMap)
}
type ConnectorChan interface {
	ReceiveIndexChan() <-chan IndexMap
	SendIndexChan() chan<- IndexMap
	RequestKeysChan() chan<- DataRequest
	ReceiveDataChan() <-chan DataResponse
}
type Connector interface {
	ConnectorChan
	ConnectorCore
	Run(stop <-chan struct{})
	Core() ConnectorCore
}

type LocalMember interface {
	Member
	Delete(KeyIDPairs)
	Put(Items)
	GetConnector() Connector
	GetIndexes() IndexMap
	GetData(KeyIDPairs) Items
}

type Items []Item

type Item interface {
	GetKey() Key
	StampedKey() StampedKey
	OwnedBy() ID
	DeepCopy() Item
}
