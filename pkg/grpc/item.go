package grpc

import (
	"time"

	"github.com/dbenque/datafan/pkg/api"
	"github.com/dbenque/datafan/pkg/grpc/model"
	google_protobuf "github.com/golang/protobuf/ptypes/timestamp"
)

type Item struct {
	model.Item
}

func (i *Item) GetKey() api.Key {
	return api.Key(i.Item.Key)
}
func (i *Item) StampedKey() api.StampedKey {
	return api.StampedKey{
		Key:       i.GetKey(),
		Timestamp: time.Unix(i.Item.Timestamp.Seconds, int64(i.Item.Timestamp.Nanos)),
	}
}
func (i *Item) OwnedBy() api.ID {
	return api.ID(i.Item.Owner)
}

func (i *Item) DeepCopy() api.Item {
	j := *i
	j.Item.Timestamp = &google_protobuf.Timestamp{Seconds: i.Item.Timestamp.Seconds, Nanos: i.Item.Timestamp.Nanos}
	j.Item.Data = make([]byte, len(i.Item.Data))
	copy(j.Item.Data, i.Item.Data)
	return &j
}
