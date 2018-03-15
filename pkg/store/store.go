package store

import "github.com/dbenque/datafan/pkg/engine"

type Store interface {
	GetMembers() []engine.ID
	GetIndex(id engine.ID) engine.Index
	Delete(engine.KeyIDPair)
	Set(engine.Item)
	Get(engine.KeyIDPair) engine.Item
}
