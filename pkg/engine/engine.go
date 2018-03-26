package engine

import (
	"sort"
	"sync"
	"time"

	"github.com/dbenque/datafan/pkg/api"
)

type Engine struct {
	local                api.LocalMember
	connector            api.Connector
	indexTimeCacheMutext sync.RWMutex
	indexTimeCache       map[api.ID]time.Time
	lastLocalKeys        []api.StampedKey
	syncPeriod           time.Duration
}

func (e *Engine) updateIndexTime(id api.ID, time time.Time) {
	e.indexTimeCacheMutext.Lock()
	defer e.indexTimeCacheMutext.Unlock()
	e.indexTimeCache[id] = time
}
func (e *Engine) getIndexTime(id api.ID) (time.Time, bool) {
	e.indexTimeCacheMutext.RLock()
	defer e.indexTimeCacheMutext.RUnlock()
	t, ok := e.indexTimeCache[id]
	return t, ok
}

func NewEngine(local api.LocalMember, syncPeriod time.Duration) *Engine {
	return &Engine{
		local:          local,
		indexTimeCache: map[api.ID]time.Time{},
		connector:      local.GetConnector(),
		syncPeriod:     syncPeriod,
	}
}

func (e *Engine) AddMember(p api.Member) {
	e.connector.Connect(p)
}

func (e *Engine) Run(stop <-chan struct{}) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		e.connector.Run(stop)
	}()

	wg.Add(1)
	//Synch out Indexes
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(e.syncPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				updatedIndexes := e.local.GetIndexes()
				for id, index := range updatedIndexes.Indexes {
					//Set index build time
					if id != e.local.ID() {
						index.BuildTime, _ = e.getIndexTime(id)
					} else {
						sort.Sort(updatedIndexes.Indexes[id].StampedKeys)
						if updatedIndexes.Indexes[id].StampedKeys.Equal(e.lastLocalKeys) { // To investigate why /*reflect.DeepEqual(e.lastLocalKeys, updatedIndexes.Indexes[id].StampedKeys)*/ does not work here
							index.BuildTime, _ = e.getIndexTime(id)
						} else {
							index.BuildTime = time.Now()
							e.updateIndexTime(id, index.BuildTime)
							e.lastLocalKeys = updatedIndexes.Indexes[id].StampedKeys
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
		defer ticker.Stop()
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
			case dataresponse := <-e.connector.ReceiveDataChan():
				e.local.Put(dataresponse.Items)
				for id, t := range dataresponse.AssociatedBuildTime {
					e.updateIndexTime(id, t)
				}
			case <-stop:
				return
			}
		}
	}()
	wg.Wait()
}

type keyPair struct {
	current *api.StampedKey
	update  *api.StampedKey
}

func (e *Engine) GetLocalMember() api.Member {
	return e.local
}

func (e *Engine) CheckAndGetUpdates(indexMap api.IndexMap) {
	membersID := map[api.ID]struct{}{}
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
			currentIndex = api.Index{
				StampedKeys: []api.StampedKey{},
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

		//index all keys and pair them
		allKeys := map[api.Key]keyPair{}
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
		toFetch := api.KeyIDPairs{}
		toDelete := api.KeyIDPairs{}

		//compare key version
		for k, kp := range allKeys {
			if kp.current == nil {
				toFetch = append(toFetch, api.KeyIDPair{ID: id, Key: k})
				continue
			}
			if kp.update == nil {
				toDelete = append(toDelete, api.KeyIDPair{ID: id, Key: k})
				continue
			}
			if kp.update.Timestamp.After(kp.current.Timestamp) {
				toFetch = append(toFetch, api.KeyIDPair{ID: id, Key: k})
				continue
			}
		}
		if len(toFetch) > 0 {
			e.connector.RequestKeysChan() <- api.DataRequest{KeyIDPairs: toFetch, RequestDestination: indexMap.Source, RequestSource: e.local.ID(), AssociatedBuildTime: map[api.ID]time.Time{id: updateIndex.BuildTime}}
		}
		if len(toDelete) > 0 {
			e.local.Delete(toDelete)
			e.updateIndexTime(id, updateIndex.BuildTime)
		}
	}
}
