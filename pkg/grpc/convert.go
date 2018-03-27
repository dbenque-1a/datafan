package grpc

import (
	"time"

	"github.com/dbenque/datafan/pkg/api"
	"github.com/dbenque/datafan/pkg/grpc/model"
	google_protobuf "github.com/golang/protobuf/ptypes/timestamp"
)

func ConvertIndexMapApiToGRPCModel(index *api.IndexMap) *model.IndexMap {

	in := &model.IndexMap{
		Source:  string(index.Source),
		Indexes: map[string]*model.Index{},
	}

	//convert from API to GRPC type.
	for k, v := range index.Indexes {
		m := &model.Index{
			BuildTime:   &google_protobuf.Timestamp{Seconds: v.BuildTime.Unix(), Nanos: int32(v.BuildTime.Nanosecond())},
			StampedKeys: make([]*model.StampedKey, len(v.StampedKeys)),
		}
		for i, j := range v.StampedKeys {
			m.StampedKeys[i] = &model.StampedKey{
				Key:       string(j.Key),
				Timestamp: &google_protobuf.Timestamp{Seconds: j.Timestamp.Unix(), Nanos: int32(j.Timestamp.Nanosecond())},
			}
		}
		in.Indexes[string(k)] = m
	}
	return in
}

func ConvertIndexMapGRPCModelToAPI(index *model.IndexMap) *api.IndexMap {

	in := &api.IndexMap{
		Source:  api.ID(index.Source),
		Indexes: map[api.ID]api.Index{},
	}

	//convert from API to GRPC type.
	for k, v := range index.Indexes {
		m := api.Index{
			BuildTime:   time.Unix(v.BuildTime.Seconds, int64(v.BuildTime.Nanos)),
			StampedKeys: make([]api.StampedKey, len(v.StampedKeys)),
		}
		for i, j := range v.StampedKeys {
			m.StampedKeys[i] = api.StampedKey{
				Key:       api.Key(j.Key),
				Timestamp: time.Unix(j.Timestamp.Seconds, int64(j.Timestamp.Nanos)),
			}
		}
		in.Indexes[api.ID(k)] = m
	}
	return in
}

func ConvertKeyIDPairAPItoModel(kps *api.KeyIDPair) *model.KeyIDPair {
	return &model.KeyIDPair{
		Key: string(kps.Key),
		Id:  string(kps.ID),
	}
}
func ConvertKeyIDPairsAPItoModel(kps *api.KeyIDPairs) *model.KeyIDPairs {

	kpsModel := &model.KeyIDPairs{KeyIDPairs: make([]*model.KeyIDPair, len(*kps))}
	for i, v := range *kps {
		kpsModel.KeyIDPairs[i] = ConvertKeyIDPairAPItoModel(&v)
	}
	return kpsModel
}

func ConvertKeyIDPairsModelToAPI(kps *model.KeyIDPairs) api.KeyIDPairs {
	kpsAPI := make([]api.KeyIDPair, len(kps.KeyIDPairs))
	for i, v := range kps.KeyIDPairs {
		kpsAPI[i] = api.KeyIDPair{
			Key: api.Key(v.Key),
			ID:  api.ID(v.Id),
		}
	}
	return kpsAPI
}

func ConvertItemsAPItoModel(i api.Items) *model.Items {
	ms := model.Items{Items: make([]*model.Item, len(i))}
	for i, v := range i {
		m := v.(*Item)
		ms.Items[i] = &m.Item
	}
	return &ms
}

func ConvertItemsModelToAPI(in *model.Items) api.Items {
	ms := make([]api.Item, len(in.Items))
	for i, v := range in.Items {
		ms[i] = &Item{*v}
	}
	return ms
}

func ConvertDataResponseModelToAPI(in *model.DataResponse) *api.DataResponse {

	dr := &api.DataResponse{
		Items:               ConvertItemsModelToAPI(in.Items),
		AssociatedBuildTime: ConvertAssociatedBuildTimeModelToAPI(in.AssociatedBuildTime),
	}
	return dr
}

func ConvertAssociatedBuildTimeModelToAPI(in map[string]*google_protobuf.Timestamp) map[api.ID]time.Time {
	bt := map[api.ID]time.Time{}
	for k, v := range in {
		bt[api.ID(k)] = time.Unix(v.Seconds, int64(v.Nanos))
	}
	return bt
}

func ConvertAssociatedBuildTimeAPIToModel(in map[api.ID]time.Time) map[string]*google_protobuf.Timestamp {
	bt := map[string]*google_protobuf.Timestamp{}
	for k, v := range in {
		bt[string(k)] = &google_protobuf.Timestamp{Seconds: v.Unix(), Nanos: int32(v.Nanosecond())}
		return bt
	}
	return bt
}
