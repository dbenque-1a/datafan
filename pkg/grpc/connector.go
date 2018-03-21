package grpc

import (
	"context"

	"github.com/dbenque/datafan/pkg/grpc/model"
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
)

type connector struct {
}

//CollectIndexMap receive the
func (c *connector) CollectIndexMap(context.Context, *model.IndexMap) (*google_protobuf.Empty, error) {
	return nil, nil
}

func (c *connector) GetData(context.Context, *model.KeyIDPairs) (*model.Items, error) {
	return nil, nil
}
