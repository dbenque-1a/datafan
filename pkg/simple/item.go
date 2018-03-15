package simple

import (
	"time"

	"github.com/dbenque/datafan/pkg/engine"
)

type Item struct {
	Key   engine.Key
	Time  time.Time
	Value string
	Owner engine.ID
}

func NewItem(key engine.Key, value string) *Item {
	return &Item{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}
}

func (i *Item) GetKey() engine.Key {
	return i.Key
}
func (i *Item) StampedKey() engine.StampedKey {
	return engine.StampedKey{
		Key:       i.Key,
		Timestamp: i.Time,
	}
}
func (i *Item) OwnedBy() engine.ID {
	return i.Owner
}
func (i *Item) DeepCopy() engine.Item {
	j := *i
	return &j
}
