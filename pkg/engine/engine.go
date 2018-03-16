package engine

import (
	"reflect"
	"sync"
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

type Connector interface {
	ReceiveIndexChan() <-chan IndexMap
	SendIndexChan() chan<- IndexMap
	Connect(Member)
	RequestKeysChan() chan<- DataRequest
	ReceiveDataChan() <-chan Items
}

type LocalMember interface {
	Member
	Add(Items)
	Delete(KeyIDPairs)
	Update(Items)
	GetConnector() Connector
}

type Items []Item

type Item interface {
	GetKey() Key
	StampedKey() StampedKey
	OwnedBy() ID
	DeepCopy() Item
}

type Engine struct {
	local                LocalMember
	connector            Connector
	indexTimeCacheMutext sync.Mutex
	indexTimeCache       map[ID]time.Time
	lastLocalKeys        []StampedKey
	syncPeriod           time.Duration
}

func (e *Engine) updateIndexTime(id ID, time time.Time) {
	e.indexTimeCacheMutext.Lock()
	defer e.indexTimeCacheMutext.Unlock()
	e.indexTimeCache[id] = time
}
func (e *Engine) getIndexTime(id ID) (time.Time, bool) {
	e.indexTimeCacheMutext.Lock()
	defer e.indexTimeCacheMutext.Unlock()
	t, ok := e.indexTimeCache[id]
	return t, ok
}

func NewEngine(local LocalMember, syncPeriod time.Duration) *Engine {
	return &Engine{
		local:          local,
		indexTimeCache: map[ID]time.Time{},
		connector:      local.GetConnector(),
		syncPeriod:     syncPeriod,
	}
}

func (e *Engine) AddMember(p Member) {
	e.connector.Connect(p)
}

func (e *Engine) Run(stop <-chan struct{}) {
	var wg sync.WaitGroup
	wg.Add(1)
	//Synch out Indexes
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(e.syncPeriod)

		for {
			select {
			case <-ticker.C:
				updatedIndexes := e.local.GetIndexes()
				for id, index := range updatedIndexes.Indexes {
					if id != e.local.ID() {
						index.BuildTime, _ = e.getIndexTime(id)
					} else {
						if reflect.DeepEqual(e.lastLocalKeys, updatedIndexes.Indexes[id]) { // update local generation time only in case of changes
							index.BuildTime, _ = e.getIndexTime(id)
						} else {
							index.BuildTime = time.Now()
							e.updateIndexTime(id, index.BuildTime)
						}
					}
					updatedIndexes.Indexes[id] = index
				}
				e.connector.SendIndexChan() <- updatedIndexes
			case <-stop:
				return
			}
		}
	}()

	wg.Add(1)
	//Synch in Indexes
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(20 * time.Second)
		for {
			select {
			case indexes := <-e.connector.ReceiveIndexChan():
				e.CheckAndGetUpdates(indexes)
			case <-ticker.C:
				//to refresh receive chan in case it is needed
			case <-stop:
				return
			}
		}
	}()

	wg.Add(1)
	//Synch in Data
	go func() {
		defer wg.Done()
		for {
			select {
			case data := <-e.connector.ReceiveDataChan():
				e.local.Update(data)
			case <-stop:
				return
			}
		}
	}()
	wg.Wait()
}

type keyPair struct {
	current *StampedKey
	update  *StampedKey
}

func (e *Engine) CheckAndGetUpdates(indexMap IndexMap) {
	membersID := map[ID]struct{}{}
	currentIndexes := e.local.GetIndexes().Indexes
	updateIndexes := indexMap.Indexes
	for id := range updateIndexes {
		membersID[id] = struct{}{}
	}
	for id := range currentIndexes {
		membersID[id] = struct{}{}
	}
	delete(membersID, e.local.ID())
	for id := range membersID {
		currentIndex, ok := currentIndexes[id]
		if !ok {
			currentIndex = Index{
				StampedKeys: []StampedKey{},
			}
		}
		updateIndex, ok2 := updateIndexes[id]
		if !ok2 {
			continue // because that update can't help for that ID. Absence of proof is not a proof of absence
		}

		previous, _ := e.getIndexTime(id)
		//Check if we already have the latest version
		if !previous.Before(updateIndex.BuildTime) {
			continue // we have a better version
		}
		e.updateIndexTime(id, updateIndex.BuildTime)

		//index all keys and pair them
		allKeys := map[Key]keyPair{}
		for i, k := range currentIndex.StampedKeys {
			allKeys[k.Key] = keyPair{current: &currentIndex.StampedKeys[i]}
		}
		for i, k := range updateIndex.StampedKeys {
			kp, ok := allKeys[k.Key]
			if !ok {
				kp = keyPair{update: &updateIndex.StampedKeys[i]}
			} else {
				kp.update = &updateIndex.StampedKeys[i]
			}
			allKeys[k.Key] = kp
		}
		toFetch := KeyIDPairs{}
		toDelete := KeyIDPairs{}

		//compare key version
		for k, kp := range allKeys {
			if kp.current == nil {
				toFetch = append(toFetch, KeyIDPair{ID: id, Key: k})
				continue
			}
			if kp.update == nil {
				toDelete = append(toDelete, KeyIDPair{ID: id, Key: k})
				continue
			}
			if kp.update.Timestamp.After(kp.current.Timestamp) {
				toFetch = append(toFetch, KeyIDPair{ID: id, Key: k})
				continue
			}
		}
		if len(toFetch) > 0 {
			e.connector.RequestKeysChan() <- DataRequest{KeyIDPairs: toFetch, RequestDestination: indexMap.Source, RequestSource: e.local.ID()}
		}
		if len(toDelete) > 0 {
			e.local.Delete(toDelete)
		}
	}
}
